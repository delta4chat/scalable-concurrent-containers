//! MPMC channel backend by [`TreeIndex`] and [`Queue`].

use core::sync::atomic::{
    AtomicBool,
    {AtomicUsize, AtomicU8},
    Ordering::{Relaxed, Acquire, SeqCst},
};
use core::future::Future;
use core::task::{Poll, Context, Waker};
use core::fmt::{self, Formatter};
use core::pin::{pin, Pin};

extern crate alloc;
use alloc::sync::Arc;

mod orig_std {
    extern crate std;
    pub use std::task::Wake;
    pub use std::thread::{self, Thread};
    pub use std::time::{Instant, Duration};
}
use orig_std::*;

use crate::atom::Atom;
use crate::tree_index::TreeIndex;
use crate::Queue;
use crate::ebr::Guard;
use crate::linked_list::Entry;

/// another method for create Chan instance.
#[inline]
pub fn new<T>(size: Option<usize>) -> Chan<T> {
    Chan::new(size)
}

/// create Chan instance, then split it to get tuple for (Sender, Receiver)
#[inline]
pub fn split<T>(size: Option<usize>) -> (Chan<T>, Chan<T>) {
    let chan = new(size);
    chan.split().unwrap()
}

/// provide compatibility for `smol::channel`, `async_channel` and `async_std::channel`.
///
/// create (Sender, Receiver) tuple with bounded size (limited number of queued messages).
#[inline]
pub fn bounded<T>(size: usize) -> (Chan<T>, Chan<T>) {
    split(Some(size))
}

/// provide compatibility for `smol::channel`, `async_channel`, and `async_std::channel`.
///
/// create (Sender, Receiver) tuple with unbounded size. (unlimited number of queued messages).
#[inline]
pub fn unbounded<T>() -> (Chan<T>, Chan<T>) {
    split(None)
}

/// this module provides compatibility for `std::sync::{mpsc, mpmc}`.
pub mod std {
    use super::*;

    /// provide compatibility for `std::sync::mpsc::unbounded` and `std::sync::mpmc::unbounded`.
    ///
    /// internally calls [`unbounded()`]
    #[inline]
    pub fn channel<T>() -> (Chan<T>, Chan<T>) {
        unbounded()
    }

    /// provide compatibility for `std::sync::mpsc::bounded` and `std::sync::mpmc::bounded`.
    ///
    /// internally calls [`unbounded()`]
    #[inline]
    pub fn sync_channel<T>(size: usize) -> (Chan<T>, Chan<T>) {
        bounded(size)
    }
}

/// this module provides compatibility for `tokio::sync::mpsc`.
pub mod tokio {
    use super::*;

    /// provide compatibility for `tokio::sync::mpsc::channel`.
    ///
    /// internally calls [`bounded()`]
    #[inline]
    pub fn channel<T>(size: usize) -> (Chan<T>, Chan<T>) {
        bounded(size)
    }

    /// provide compatibility for `tokio::sync::mpsc::unbounded_channel`.
    ///
    /// internally calls [`unbounded()`]
    #[inline]
    pub fn unbounded_channel<T>() -> (Chan<T>, Chan<T>) {
        unbounded()
    }
}

/// Error type for Chan
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ChanError {
    /// Channel full (reached it's max length of queued messages)
    Full,

    /// Channel is empty currently.
    Empty,

    /// Channel receive timed out.
    RecvTimeout,

    /// Channel is closed.
    Closed,

    /// No permission to send
    RecvOnly,

    /// No permission to recv
    SendOnly,

    /// Unexpected Null sdd::Ptr return by Queue::pop()
    NullPtr,

    /// Other Unknown Error
    Other(String),
}

impl fmt::Display for ChanError {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        use ChanError::*;
        f.write_str(
            match self {
                Full => "Chan is Full",
                Empty => "Chan is Empty",
                RecvTimeout => "Chan is Empty and timed out",
                Closed => "Chan is Closed",
                RecvOnly => "Chan is Receiver but try to sending",
                SendOnly => "Chan is Sender but try to receiving",
                NullPtr => "Chan::try_recv() got Null Ptr",
                Other(s) => {
                    f.write_str("Chan Unknown Error: ")?;
                    return f.write_str(&s);
                },
            }
        )
    }
}

impl core::error::Error for ChanError {}

/// the Ordering of Chan
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum ChanOrdering {
    /// un-ordered mode (or "relaxed" mode) is unable to guarantee the ordering of messages.
    Unordered,

    /// FIFO mode is guarantee the ordering of messages is "first-in, first-out".
    FIFO,

    /// LIFO modme is guarantee the ordering of messages is "last-in, first-out".
    LIFO,
}
impl Default for ChanOrdering {
    fn default() -> Self {
        Self::Unordered
    }
}

type Sender<T> = (Atom<Instant>, Queue<T>, Queue<Waker>);
type ArcSender<T> = Arc<Sender<T>>;
type Wakers = Queue<Waker>;

#[derive(Debug)]
struct ChanInner<T: 'static> {
    ordering: Atom<ChanOrdering>,
                      // sender_id -> (last_pop_time, queued_messages, rx_wakers)
    senders: TreeIndex<usize, ArcSender<T>>,
    wakers: Wakers,
    len: AtomicUsize,
    size: Option<usize>,
    global_closed: AtomicBool,
    chan_id_counter: AtomicUsize,
}
unsafe impl<T> Send for ChanInner<T> {}
unsafe impl<T> Sync for ChanInner<T> {}

impl<T> ChanInner<T> {
    #[inline]
    fn new(size: Option<usize>) -> Self {
        Self {
            ordering: Atom::new(Default::default()),
            senders: Default::default(),
            wakers: Default::default(),
            len: AtomicUsize::new(0),
            size,
            global_closed: AtomicBool::new(false),
            chan_id_counter: AtomicUsize::new(100),
        }
    }

    #[inline]
    fn ordering(&self) -> ChanOrdering {
        *(self.ordering.get().unwrap())
    }

    #[inline]
    fn max_id(&self) -> Option<usize> {
        let guard = Guard::new();
        self.senders.iter(&guard).map(|(id, _sender)| { *id }).max()
    }

    #[inline]
    fn gen_id(&self) -> usize {
        match self.ordering() {
            ChanOrdering::FIFO => {
                return ID_FIFO;
            },
            ChanOrdering::LIFO => {
                return ID_LIFO;
            },
            _ => {}
        }

        let id = self.chan_id_counter.fetch_update(
            SeqCst,
            SeqCst,
            |c| {
                if c < MAX_ID {
                    Some(c + 1)
                } else {
                    #[cfg(test)]
                    eprintln!("Chan ID has been exhausted! trying reset...");

                    let max_id = self.max_id().unwrap_or(MIN_ID);
                    if max_id < usize::MAX {
                        #[cfg(test)]
                        eprintln!("reseting Chan ID Counter to {}", max_id + 1);

                        Some(max_id + 1)
                    } else {
                        panic!("Chan ID has been exhausted! most IDs used by active channels, unable to reset ID back to lower value...");
                    }
                }
            }
        ).unwrap();

        assert!(! self.senders.contains(&id));
        id
    }

    #[inline]
    fn add_waker(&self, waker: Waker) {
        self.wakers.push(waker);
    }

    #[inline]
    fn wake_one(&self) -> bool {
        if let Some(waker) = self.wakers.pop() {
            waker.wake_by_ref();
            true
        } else {
            false
        }
    }

    #[inline]
    fn wake(&self, max: usize) -> usize {
        let mut wakes = 0;
        for _ in 0..max {
            if self.wake_one() {
                if wakes < usize::MAX {
                    wakes += 1;
                }
            } else {
                break;
            }
        }
        wakes
    }

    #[inline]
    fn wake_all(&self) -> usize {
        let mut wakes: usize = 0;
        loop {
            let w = self.wake(usize::MAX);
            if w > 0 {
                wakes = wakes.saturating_add(w);
            } else {
                break;
            }
        }
        wakes
    }
}

/// Invalid Chan ID
pub const ID_INVALID: usize = 0;

/// Special Chan ID for [`ChanOrdering::FIFO`]
pub const ID_FIFO: usize = 1;

/// Special Chan ID for [`ChanOrdering::LIFO`]
pub const ID_LIFO: usize = 2;

/// Minimum Chan ID for [`ChanOrdering::Unordered`]
pub const MIN_ID: usize = 100;

/// Maximum Chan ID for [`ChanOrdering::Unordered`]
pub const MAX_ID: usize = usize::MAX;

/// flag value for Receive message only
pub const RECV_ONLY: u8 = b'r';

/// flag value for Send only
pub const SEND_ONLY: u8 = b'w';

/// flag value for Receive and Send message (default flag)
pub const RECV_SEND: u8 = b'+';

/// flag value for Closed
pub const CLOSED:    u8 = b'c';

/// (Experimental) multi-producer, multi-consumer (MPMC) channel backend utilizes a [`TreeIndex`] and [`Queue`], where each message can be received by only one of all existing consumers.
///
/// Typically, this is a first-in, first-out (FIFO) channel, provided that only a single sender is active concurrently.
///
/// However, in the event that multiple senders are active concurrently, there is unable to guarantee the ordering of messages.
///
/// # Fairness
/// the sender queue with oldest "last-pop-time" would be prioritized over the others, which could not starve the other Sender.
#[derive(Debug)]
pub struct Chan<T: 'static> {
    id: usize,
    flag: Arc<AtomicU8>,
    maybe_sender: Option<ArcSender<T>>,
    inner: Arc<ChanInner<T>>,
    dropped: bool,
    avoid_drop: bool,
}
unsafe impl<T> Send for Chan<T> {}
unsafe impl<T> Sync for Chan<T> {}

impl<T> Drop for Chan<T> {
    #[inline]
    fn drop(&mut self) {
        if self.avoid_drop {
            #[cfg(test)]
            eprintln!("avoid drop {}", self.id);

            return;
        }

        #[cfg(test)]
        eprintln!("called drop: id={}", self.id);

        if self.dropped {
            return;
        }
        self.dropped = true;

        self.close();
    }
}

impl<T> Clone for Chan<T> {
    #[inline]
    fn clone(&self) -> Self {
        let flag = self.flag();
        let inner = self.inner.clone();
        let id = inner.gen_id();

        let mut this = Self::init(id, flag, inner);
        if id < MIN_ID {
            this.avoid_drop = true;
        }
        this
    }
}

impl<T> Default for Chan<T> {
    #[inline]
    fn default() -> Self {
        Self::unbounded()
    }
}

impl<T> Chan<T> {
    #[inline]
    fn init(id: usize, flag: u8, inner: Arc<ChanInner<T>>) -> Self {
        let mut this = Self {
            id,
            flag: Arc::new(AtomicU8::new(flag)),
            maybe_sender: None,
            inner,
            dropped: false,
            avoid_drop: false,
        };

        if this.is_sender() {
            let sender: Arc<(Atom<Instant>, Queue<T>, Queue<Waker>)> = Arc::new(Default::default());
            this.maybe_sender = Some(sender.clone());
            let _ = this.inner.senders.insert(id, sender);
        }

        this
    }

    /// create a Channel with optional size (max length of queued messages)
    #[inline]
    pub fn new(size: Option<usize>) -> Self {
        let inner = Arc::new(ChanInner::new(size));
        let id = inner.gen_id();
        Self::init(id, RECV_SEND, inner)
    }

    /// create bounded Channel (limited number of queued messages).
    #[inline]
    pub fn bounded(size: usize) -> Self {
        Self::new(Some(size))
    }

    /// create unbounded Channel (unlimited number of queued messages).
    #[inline]
    pub fn unbounded() -> Self {
        Self::new(None)
    }

    /// get the ordering of messages.
    #[inline]
    pub fn ordering(&self) -> ChanOrdering {
        self.inner.ordering()
    }

    /// set the ordering of messages.
    #[inline]
    pub fn set_ordering(&self, ordering: ChanOrdering) {
        if ordering == ChanOrdering::LIFO {
            todo!("LIFO is not implemented yet");
        }
        self.inner.ordering.set(ordering);
    }

    /// create two side from this channel: the left side is sender, and the right side is receiver.
    ///
    /// this is does not work if this is not a bidirectional channel.
    #[inline]
    pub fn split(&self) -> Result<(Chan<T>, Chan<T>), ChanError> {
        let flag = self.flag();

        if flag == RECV_ONLY {
            return Err(ChanError::RecvOnly);
        }
        if flag == SEND_ONLY {
            return Err(ChanError::SendOnly);
        }

        let sender = self.clone();
        let receiver = self.clone();

        sender.send_only();
        receiver.recv_only();

        Ok((sender, receiver))
    }

    /// get the ID of this channel.
    #[inline]
    pub fn id(&self) -> &usize {
        &self.id
    }

    /// get current flag of this channel.
    #[inline]
    pub fn flag(&self) -> u8 {
        self.flag.load(Relaxed)
    }

    #[inline]
    fn set_flag(&self, flag: u8) -> bool {
        if self.flag() == CLOSED {
            false
        } else {
            self.flag.store(flag, Relaxed);
            true
        }
    }

    /// restrict this Chan instance for receive message only.
    #[inline]
    pub fn recv_only(&self) -> bool {
        if self.flag() == SEND_ONLY {
            false
        } else {
            let b = self.set_flag(RECV_ONLY);
            if b && self.ordering() == ChanOrdering::Unordered {
                let _ = self.inner.senders.remove(&self.id);
            }
            b
        }
    }

    /// restrict this Chan instance for send message only.
    #[inline]
    pub fn send_only(&self) -> bool {
        if self.flag() == RECV_ONLY {
            false
        } else {
            self.set_flag(SEND_ONLY)
        }
    }

    /// checks whether this channel is bidirectional (can send and receive messages)
    #[inline]
    pub fn is_bidirectional(&self) -> bool {
        self.flag() == RECV_SEND
    }

    /// checks whether this channel able to receive messages.
    #[inline]
    pub fn is_receiver(&self) -> bool {
        self.is_bidirectional() || self.flag() == RECV_ONLY
    }

    /// checks whether this channel able to send messages.
    #[inline]
    pub fn is_sender(&self) -> bool {
        self.is_bidirectional() || self.flag() == SEND_ONLY
    }

    /// get current number of senders. this internally calls [`TreeIndex::len()`] so the time complexity is `O(N)`.
    #[inline]
    pub fn senders(&self) -> usize {
        self.inner.senders.len()
    }

    /// get the current length of queued messages in this channel.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len.load(Acquire)
    }

    /// checks whether this channel closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        #[cfg(test)]
        let id = self.id;

        if self.dropped {
            #[cfg(test)]
            eprintln!("{id} is closed by Drop");

            return true;
        }

        if self.inner.global_closed.load(Relaxed) {
            #[cfg(test)]
            eprintln!("{id} is closed by global_closed");

            return true;
        }

        if self.flag() == CLOSED {
            #[cfg(test)]
            eprintln!("{id} is closed by flag");

            return true;
        }

        if self.is_sender() {
            if self.inner.senders.contains(&self.id) {
                #[cfg(test)]
                eprintln!("{id} is closed? false (senders contains)");

                false
            } else {
                #[cfg(test)]
                eprintln!("{id} is closed? true (senders contains)");

                true
            }
        } else {
            #[cfg(test)]
            eprintln!("{id} is_closed? false (final)");

            false
        }
    }

    /// closing this sender and remove it from global register.
    #[inline]
    pub fn close(&self) {
        let _ = self.inner.senders.remove(&self.id);
        self.set_flag(CLOSED);
    }

    /// globally closing this channel so all senders will no longer able to send messages.
    #[inline]
    pub fn close_all(&self) {
        self.close();
        self.inner.global_closed.store(true, Relaxed);
        self.inner.senders.clear();
    }

    /// send message.
    #[inline]
    pub fn send(&self, val: T) -> Result<(), ChanError> {
        if self.is_closed() {
            return Err(ChanError::Closed);
        }

        if self.flag() == RECV_ONLY {
            return Err(ChanError::RecvOnly);
        }

        if let Some(size) = self.inner.size {
            if self.len() >= size {
                return Err(ChanError::Full);
            }
        }

        let (_last_pop_time, queue, _wakers)=
            match self.sender() {
                Some(v) => v,
                _ => {
                    return Err(ChanError::Closed);
                }
            };

        queue.push(val);
        let _ = self.inner.len.fetch_update(
            Acquire,
            Acquire,
            |l| {
                if l < usize::MAX {
                    Some(l + 1)
                } else {
                    None
                }
            }
        );

        let len = self.len();

        let (minor_backlog, major_backlog) =
            if let Some(size) = self.inner.size {
                ((size / 10).min(10), (size / 2).min(100))
            } else {
                (10, 100)
            };

        if len >= major_backlog {
            // wake all receiver due to too many backlog messages
            self.inner.wake_all();
        } else if len >= minor_backlog {
            // wake only len receiver if current len >= minor_backlog
            self.inner.wake(len);
        } else {
            // wake only one receiver futures
            self.inner.wake_one();
        }

        Ok(())
    }

    #[inline]
    fn sender(&self) -> Option<&(Atom<Instant>, Queue<T>, Queue<Waker>)> {
        if let Some(ref sender) = self.maybe_sender {
            Some(&*sender)
        } else {
            None
        }
    }

    #[inline]
    fn try_pop_from(&self, _id: &usize, sender: &Arc<(Atom<Instant>, Queue<T>, Queue<Waker>)>) -> Option<T> {
        let (last_pop_time, queue, wakers) = &**sender;
        if let Some(mut shared) = queue.pop() {
            #[cfg(test)]
            eprintln!("pop message from {_id}");

            // got message from this sender, update last_pop_time.
            last_pop_time.set(Instant::now());

            let _ = self.inner.len.fetch_update(
                Acquire,
                Acquire,
                |l| {
                    if l > 0 {
                        Some(l - 1)
                    } else {
                        None
                    }
                }
            );

            while let Some(waker) = wakers.pop() {
                waker.wake_by_ref();
            }

            if let Some(entry) = unsafe { shared.get_mut() } {
                #[cfg(test)]
                eprintln!("(preferred) got message {entry:p} by shared.get_mut()");

                return Some(unsafe { entry.take_inner() });
            } else {
                let guard = Guard::new();
                let ptr = shared.get_guarded_ptr(&guard).as_ptr();
                let entry = unsafe { &mut *(ptr as *mut Entry<T>) };

                #[cfg(test)]
                eprintln!("(less safe) got message {entry:p} by pointer deref cast");

                return Some(unsafe { entry.take_inner() });
            }
        }

        None
    }

    /// try receive message from this channel.
    /// this is never blocking, if there is no message, it will returns ChanError::Empty.
    #[inline]
    pub fn try_recv(&self) -> Result<T, ChanError> {
        self.try_recv_from().map(|x| { x.1 })
    }

    /// try receive message (with Sender ID) from this channel.
    /// this is never blocking, if there is no message, it will returns ChanError::Empty.
    #[inline]
    pub fn try_recv_from(&self) -> Result<(usize, T), ChanError> {
        if self.is_closed() {
            return Err(ChanError::Closed);
        }

        if self.flag() == SEND_ONLY {
            return Err(ChanError::SendOnly);
        }

        let guard = Guard::new();
        let mut senders: Vec<(&usize, &Arc<(Atom<Instant>, Queue<T>, Queue<Waker>)>)> = self.inner.senders.iter(&guard).collect();

        // sender with smallest last-use-time will be first of order.
        senders.sort_unstable_by_key(|(_, v)| { v.0.get() }); // impl<T: Ord + ?Sized> Ord for Arc<T>

        #[cfg(test)]
        eprintln!("try_recv() ordering: {:?}", {
            let mut order = Vec::new();
            for (k, v) in senders.iter() {
                order.push((k, v.0.get()));
            }
            order
        });

        let ordering = self.ordering();
        for (id, sender) in senders.into_iter() {
            if ordering == ChanOrdering::Unordered {
                if id == &self.id {
                    // skip myself.
                    // so a thread both sending and receiving will only receive messages from other senders.
                    continue;
                }
            }

            if let Some(msg) = self.try_pop_from(id, sender) {
                return Ok((*id, msg));
            }
        }

        Err(ChanError::Empty)
    }

    #[inline]
    fn clone_without_change_id(&self) -> Self {
        Self {
            id: self.id,
            flag: self.flag.clone(),
            inner: self.inner.clone(),
            maybe_sender: self.maybe_sender.clone(),
            dropped: self.dropped,
            avoid_drop: true, // do not close if destructor called
        }
    }

    /// Asynchronous receive message from this channel
    #[inline]
    pub fn recv(&self) -> ChanRecv<T> {
        ChanRecv {
            chan: self.clone_without_change_id(),
            timer: None,
        }
    }

    /// Synchronous receive message from this channel:
    /// internally it calls `block_on(self.recv())`
    #[inline]
    pub fn recv_blocking(&self) -> Result<T, ChanError> {
        block_on(self.recv(), None)
    }

    /// Asynchronous receive message from this channel
    #[inline]
    pub fn recv_timeout(&self, timeout: Duration) -> ChanRecv<T> {
        let mut recv = self.recv();
        recv.timer = Some((Instant::now(), timeout));
        recv
    }

    /// Synchronous receive message from this channel:
    /// internally it calls `block_on(self.recv())`
    #[inline]
    pub fn recv_timeout_blocking(&self, timeout: Duration) -> Result<T, ChanError> {
        block_on(self.recv_timeout(timeout), Some(timeout))
    }

    /// Asynchronous waiting until all queued messages (sent by this ID) is handled properly.
    ///
    /// # Warning: call this with same thread that receives message from this channel will causes dead lock!
    #[inline]
    pub fn wait(&self) -> ChanWait<T> {
        ChanWait(self.clone_without_change_id())
    }

    /// Synchronous waiting until all queued messages (sent by this ID) is handled properly.
    /// internally it calls `block_on(self.recv())`
    ///
    /// # Warning: call this with same thread that receives message from this channel will causes dead lock!
    #[inline]
    pub fn wait_blocking(&self) {
        block_on(self.wait(), None)
    }
}

/// NOTE: not tested WIP
pub(self) fn block_on<T>(fut: impl core::future::Future<Output=T>, interval: Option<Duration>) -> T {
    const DEFAULT_INTERVAL: Duration = Duration::new(3, 0);
    let interval = interval.unwrap_or(DEFAULT_INTERVAL);

    struct ThreadWaker(Thread);
    impl Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = Arc::new(ThreadWaker(thread::current())).into();
    let mut ctx = Context::from_waker(&waker);

    let mut fut = pin!(fut);
    loop {
        match fut.as_mut().poll(&mut ctx) {
            Poll::Ready(ret) => {
                return ret;
            },
            _ => {
                thread::park_timeout(interval);
            }
        }
    }
}

/// [`Chan::recv()`] returns this type that implements [`core::future::Future`] for awaiting
#[derive(Debug)]
pub struct ChanRecv<T: 'static> {
    chan: Chan<T>,
    timer: Option<(Instant, Duration)>,
}
impl<T> core::future::Future for ChanRecv<T> {
    type Output = Result<T, ChanError>;

    #[inline]
    fn poll(
        self: Pin<&mut Self>,
        ctx: &mut Context<'_>
    ) -> Poll<Self::Output> {
        match self.chan.try_recv() {
            Ok(msg) => Poll::Ready(Ok(msg)),
            Err(err) => {
                if err == ChanError::Empty {
                    if let Some(ref timer) = self.timer {
                        let (started, timeout) = timer;
                        if &started.elapsed() > timeout {
                            return Poll::Ready(Err(ChanError::RecvTimeout));
                        }
                    }

                    let waker = ctx.waker();
                    self.chan.inner.add_waker(waker.clone());
                    Poll::Pending
                } else {
                    Poll::Ready(Err(err))
                }
            }
        }
    }
}

/// [`Chan::wait()`] returns this type that implements [`core::future::Future`] for awaiting
#[derive(Debug)]
pub struct ChanWait<T: 'static>(
    Chan<T>,
);
impl<T> Future for ChanWait<T> {
    type Output = ();

    #[inline]
    fn poll(
        self: Pin<&mut Self>,
        ctx: &mut Context<'_>
    ) -> Poll<Self::Output> {
        if self.0.is_closed() {
            return Poll::Ready(());
        }

        let (_last_pop_time, queue, wakers) =
            match self.0.sender() {
                Some(v) => v,
                _ => {
                    return Poll::Ready(());
                }
            };
        if queue.is_empty() {
            Poll::Ready(())
        } else {
            let waker = ctx.waker();
            wakers.push(waker.clone());
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn must_not_recv_from_self() {
        let ch: Chan<u16> = Default::default();
        ch.send(1).unwrap();
        assert_eq!(ch.try_recv(), Err(ChanError::Empty))
    }

    #[test]
    fn must_recv_from_other() {
        let ch: Chan<u16> = Default::default();
        let other_ch = ch.clone();
        other_ch.send(1).unwrap();
        assert_eq!(ch.try_recv(), Ok(1))
    }

    #[test]
    fn concurrent_unordered() {
        concurrent(ChanOrdering::Unordered)
    }

    #[test]
    fn concurrent_fifo() {
        concurrent(ChanOrdering::FIFO)
    }

    fn concurrent(ordering: ChanOrdering) {
        extern crate std;
        use std::thread;
        use std::time::{Instant, SystemTime};

        let ch: Chan<(u16, &'static str, SystemTime)> = Default::default();
        if ordering != ChanOrdering::default() {
            ch.set_ordering(ordering);
        }
        let mut thrs = vec![];
        for i in 0..3 {
            let ch = ch.clone();
            thrs.push(thread::spawn(move || {
                let started = Instant::now();

                while started.elapsed() < Duration::from_secs(3) {
                    ch.send((i, "msg 1 hello", SystemTime::now())).unwrap();
                    if ordering == ChanOrdering::Unordered && i == 0 {
                        let _ = dbg!(ch.try_recv());
                    }
                    ch.send((i, "msg 2 world", SystemTime::now())).unwrap();
                    ch.send((i, "msg 3 good bye", SystemTime::now())).unwrap();

                    thread::sleep(Duration::from_millis(900));
                }

                ch.wait_blocking();
                eprintln!("{ordering:?} thread {i} exiting");
            }));
        }

        ch.recv_only();
        let mut fails = 0;
        let mut empty = 0;
        for _ in 0..1000 {
            if dbg!((ordering, ch.senders())).1 == 0 {
                break;
            }
            dbg!((ordering, &ch.is_closed()));
            let i = Duration::from_secs_f64(1.5);
            let r = ch.recv_timeout(i);
            //dbg!(&r);
            let r = dbg!((ordering, block_on(r, Some(i)))).1;
            if r == Err(ChanError::RecvTimeout) {
                break;
            }
            if r == Err(ChanError::Empty) {
                empty += 1;
                if empty > 100 {
                    break;
                }
                continue;
            }
            if r == Err(ChanError::Closed) {
                break;
            }
            if r.is_err() {
                fails += 1;
                if fails >= 100 {
                    r.unwrap();
                    unreachable!();
                } else {
                    continue;
                }
            }
            r.unwrap();
        }

        ch.close();

        for thr in thrs.into_iter() {
            let _ = dbg!((ordering, thr.join()));
        }
    }
}
