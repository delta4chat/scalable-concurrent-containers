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
    pub use std::time::Duration;
}
use orig_std::*;

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

#[derive(Debug)]
struct ChanInner<T: 'static> {
    skip_id: AtomicUsize,
    senders: TreeIndex<usize, Arc<(Queue<T>, Queue<Waker>)>>, // sender_id -> Queue<T>
    wakers: Queue<Waker>,
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
            skip_id: AtomicUsize::new(0), // zero is invalid value due to chan_id start from 100.
            senders: Default::default(),
            wakers: Default::default(),
            len: AtomicUsize::new(0),
            size,
            global_closed: AtomicBool::new(false),
            chan_id_counter: AtomicUsize::new(100),
        }
    }

    #[inline]
    fn gen_id(&self) -> usize {
        self.chan_id_counter.fetch_update(
            SeqCst,
            SeqCst,
            |c| {
                if c < usize::MAX {
                    Some(c + 1)
                } else {
                    panic!("Chan ID has been exhausted!")
                }
            }
        ).unwrap()
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

/// (Experimental) multi-producer, multi-consumer (MPMC) channel backend utilizes a [`TreeIndex`] and [`Queue`], where each message can be received by only one of all existing consumers.
///
/// Typically, this is a first-in, first-out (FIFO) channel, provided that only a single sender is active concurrently.
///
/// However, in the event that multiple senders are active concurrently, there is unable to guarantee the ordering of messages.
///
/// # in the case of multi-senders
/// If the `rand` feature was not enabled (by default), then the Sender with the smallest ID would be prioritized over the others (the default behavior of [`TreeIndex::iter()`]), which could starve the other Sender with the larger ID. for now implements "idle algorithm" that temporary skipping the latest sender from receiving, but that might not be enough.
///
/// if enables the `rand` feature, then [`rand::seq::SliceRandom::shuffle`](https://docs.rs/rand/0.9.0/rand/seq/trait.SliceRandom.html#tymethod.shuffle) is used for shuffled disorderly iterating, which should ensure some degree of fairness. 
#[derive(Debug)]
pub struct Chan<T: 'static> {
    id: usize,
    flag: Arc<AtomicU8>,
    maybe_sender: Option<Arc<(Queue<T>, Queue<Waker>)>>,
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
            return;
        }

        if Arc::get_mut(&mut self.inner).is_none() || Arc::get_mut(&mut self.flag).is_none() {
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

        Self::init(id, flag, inner)
    }
}

impl<T> Default for Chan<T> {
    #[inline]
    fn default() -> Self {
        Self::unbounded()
    }
}

impl<T> Chan<T> {
    /// flag value for Receive message only
    pub const RECV_ONLY: u8 = b'r';

    /// flag value for Send only
    pub const SEND_ONLY: u8 = b'w';

    /// flag value for Receive and Send message (default flag)
    pub const RECV_SEND: u8 = b'+';

    /// flag value for Closed
    pub const CLOSED:    u8 = b'c';

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
            let sender: Arc<(Queue<T>, Queue<Waker>)> = Arc::new(Default::default());
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
        Self::init(id, Self::RECV_SEND, inner)
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

    /// create two side from this channel: the left side is sender, and the right side is receiver.
    ///
    /// this is does not work if this is not a bidirectional channel.
    #[inline]
    pub fn split(&self) -> Result<(Chan<T>, Chan<T>), ChanError> {
        let flag = self.flag();

        if flag == Self::RECV_ONLY {
            return Err(ChanError::RecvOnly);
        }
        if flag == Self::SEND_ONLY {
            return Err(ChanError::SendOnly);
        }

        let sender = self.clone();
        let receiver = self.clone();

        sender.send_only();
        receiver.recv_only();

        Ok((sender, receiver))
    }

    /// get current flag of this channel.
    #[inline]
    pub fn flag(&self) -> u8 {
        self.flag.load(Relaxed)
    }

    #[inline]
    fn set_flag(&self, flag: u8) -> bool {
        if self.is_closed() {
            false
        } else {
            self.flag.store(flag, Relaxed);
            true
        }
    }

    /// restrict this Chan instance for receive message only.
    #[inline]
    pub fn recv_only(&self) -> bool {
        if self.flag() == Self::SEND_ONLY {
            false
        } else {
            let _ = self.inner.senders.remove(&self.id);
            self.set_flag(Self::RECV_ONLY)
        }
    }

    /// restrict this Chan instance for send message only.
    #[inline]
    pub fn send_only(&self) -> bool {
        if self.flag() == Self::RECV_ONLY {
            false
        } else {
            self.set_flag(Self::SEND_ONLY)
        }
    }

    /// checks whether this channel is bidirectional (can send and receive messages)
    #[inline]
    pub fn is_bidirectional(&self) -> bool {
        self.flag() == Self::RECV_SEND
    }

    /// checks whether this channel able to receive messages.
    #[inline]
    pub fn is_receiver(&self) -> bool {
        self.is_bidirectional() || self.flag() == Self::RECV_ONLY
    }

    /// checks whether this channel able to send messages.
    #[inline]
    pub fn is_sender(&self) -> bool {
        self.is_bidirectional() || self.flag() == Self::SEND_ONLY
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

        if self.flag() == Self::CLOSED {
            #[cfg(test)]
            eprintln!("{id} is closed by flag");

            return true;
        }

        if self.is_sender() {
            if self.inner.senders.contains(&self.id) {
                #[cfg(test)]
                eprintln!("{id} isclosed false (senders contains)");

                false
            } else {
                #[cfg(test)]
                eprintln!("{id} isclosed true (senders contains)");

                true
            }
        } else {
            #[cfg(test)]
            eprintln!("{id} isclosed false (final)");

            false
        }
    }

    /// closing this sender and remove it from global register.
    #[inline]
    pub fn close(&self) {
        let _ = self.inner.senders.remove(&self.id);
        self.set_flag(Self::CLOSED);
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

        if self.flag() == Self::RECV_ONLY {
            return Err(ChanError::RecvOnly);
        }

        if let Some(size) = self.inner.size {
            if self.len() >= size {
                return Err(ChanError::Full);
            }
        }

        let queue =
            match self.queue() {
                Some(q) => q,
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

    fn sender(&self) -> Option<&(Queue<T>, Queue<Waker>)> {
        if let Some(ref sender) = self.maybe_sender {
            Some(&*sender)
        } else {
            None
        }
    }

    fn queue(&self) -> Option<&Queue<T>> {
        self.sender().map(|x| { &x.0 })
    }
    fn wakers(&self) -> Option<&Queue<Waker>> {
        self.sender().map(|x| { &x.1 })
    }

    #[inline]
    fn try_pop_from(&self, id: &usize, sender: &Arc<(Queue<T>, Queue<Waker>)>) -> Option<T> {
        let (queue, wakers) = &**sender;
        if let Some(mut shared) = queue.pop() {
            // received message from this queue, skipping it after (if multi senders exists)
            self.inner.skip_id.store(*id, Relaxed);

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

            if false {
                self.wakers();
            }

            while let Some(waker) = wakers.pop() {
                waker.wake_by_ref();
            }

            if let Some(entry) = unsafe { shared.get_mut() } {
                return Some(unsafe { entry.take_inner() });
            } else {
                let guard = Guard::new();
                let ptr = shared.get_guarded_ptr(&guard).as_ptr();
                let entry = unsafe { &mut *(ptr as *mut Entry<T>) };
                return Some(unsafe { entry.take_inner() });
            }
        }

        None
    }

    /// try receive message from this channel.
    /// this is never blocking, if there is no message, it will returns ChanError::Empty.
    #[inline]
    pub fn try_recv(&self) -> Result<T, ChanError> {
        if self.is_closed() {
            return Err(ChanError::Closed);
        }

        if self.flag() == Self::SEND_ONLY {
            return Err(ChanError::SendOnly);
        }

        let guard = Guard::new();
        let senders: Vec<(&usize, &Arc<(Queue<T>, Queue<Waker>)>)> = self.inner.senders.iter(&guard).collect();

        #[cfg(feature="rand")]
        let mut senders = senders;

        #[cfg(feature="rand")]
        {
            use rand::seq::SliceRandom;
            let mut rng = rand::thread_rng();
            senders.shuffle(&mut rng);
        }

        let senders_len = senders.len();
        let mut maybe_skipped = None;
        for (id, sender) in senders.into_iter() {
            if id == &self.id {
                // skip myself.
                // so a thread both sending and receiving will only receive messages from other senders.
                continue;
            }

            if senders_len > 1 {
                if id == &self.inner.skip_id.load(Relaxed) {
                    // skip the queue that last receive message from.
                    // but we need to clear this state for avoid starve the busying small-ID sender if other big-ID idle sender exists.
                    self.inner.skip_id.store(0, Relaxed);

                    maybe_skipped = Some((id, sender));
                    continue;
                }
            }

            if let Some(msg) = self.try_pop_from(id, sender) {
                return Ok(msg);
            }
        }

        // now other queue is empty, so try the queue that skipped in previous iterating.
        if let Some((id, sender)) = maybe_skipped {
            if let Some(msg) = self.try_pop_from(id, sender) {
                return Ok(msg);
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
            avoid_drop: true, // do not call destructor
        }
    }

    /// Asynchronous receive message from this channel
    #[inline]
    pub fn recv(&self) -> ChanRecv<T> {
        ChanRecv(self.clone_without_change_id())
    }

    /// Synchronous receive message from this channel:
    /// internally it calls `block_on(self.recv())`
    pub fn recv_blocking(&self) -> Result<T, ChanError> {
        block_on(self.recv())
    }

    /// Asynchronous waiting until all queued messages (sent by this ID) is handled properly.
    pub fn wait(&self) -> ChanWait<T> {
        ChanWait(self.clone_without_change_id())
    }

    /// Synchronous waiting until all queued messages (sent by this ID) is handled properly.
    /// internally it calls `block_on(self.recv())`
    pub fn wait_blocking(&self) {
        block_on(self.wait())
    }
}

/// NOTE: not tested WIP
pub(self) fn block_on<T>(fut: impl core::future::Future<Output=T>) -> T {
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
                thread::park_timeout(Duration::from_secs(3));
            }
        }
    }
}

/// [`Chan::recv()`] returns this type that implements [`core::future::Future`] for awaiting
#[derive(Debug)]
pub struct ChanRecv<T: 'static>(
    Chan<T>
);
impl<T> core::future::Future for ChanRecv<T> {
    type Output = Result<T, ChanError>;

    #[inline]
    fn poll(
        self: Pin<&mut Self>,
        ctx: &mut Context<'_>
    ) -> Poll<Self::Output> {
        match self.0.try_recv() {
            Ok(msg) => Poll::Ready(Ok(msg)),
            Err(err) => {
                if err == ChanError::Empty {
                    let waker = ctx.waker();
                    self.0.inner.add_waker(waker.clone());
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

        let (queue, wakers) =
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
    fn concurrent() {
        extern crate std;
        use std::thread;

        let ch: Chan<(u16, &'static str)> = Default::default();
        let mut thrs = vec![];
        for i in 0..3 {
            let ch = ch.clone();
            thrs.push(thread::spawn(move || {
                ch.send((i, "msg 1 hello")).unwrap();
                if i == 0 {
                    let _ = dbg!(ch.try_recv());
                }
                ch.send((i, "msg 2 world")).unwrap();
                ch.send((i, "msg 3 good bye")).unwrap();

                ch.wait_blocking();
                eprintln!("thread {i} exiting");
            }));
        }

        ch.recv_only();
        let mut fails = 0;
        loop {
            dbg!(&ch.is_closed());
            let r = ch.recv();
            dbg!(&r);
            let r = block_on(r);
            if r == Err(ChanError::Empty) || r == Err(ChanError::Closed) {
                break;
            }
            if r.is_err() {
                fails += 1;
                if fails >= 2 {
                    r.unwrap();
                    unreachable!();
                } else {
                    continue;
                }
            }
            r.unwrap();
        }

        ch.close_all();

        for thr in thrs.into_iter() {
            let _ = dbg!(thr.join());
        }
    }
}
