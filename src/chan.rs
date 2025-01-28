//! MPMC channel backend by [`Queue`].

use core::sync::atomic::{
    AtomicBool,
    {AtomicUsize, AtomicU8},
    Ordering::{Relaxed, Acquire, SeqCst},
};

extern crate alloc;
use alloc::sync::Arc;

use crate::tree_index::TreeIndex;
use crate::Queue;
use crate::ebr::Guard;
use crate::linked_list::Entry;

/// another method for create Chan instance.
pub fn new<T>(size: Option<usize>) -> Chan<T> {
    Chan::new(size)
}

/// create Chan instance, then split it to get tuple for (Sender, Receiver)
pub fn split<T>(size: Option<usize>) -> (Chan<T>, Chan<T>) {
    let chan = new(size);
    chan.split().unwrap()
}

/// provide compatibility for `smol::channel`, `async_channel` and `async_std::channel`.
///
/// create (Sender, Receiver) tuple with bounded size (limited number of queued messages).
pub fn bounded<T>(size: usize) -> (Chan<T>, Chan<T>) {
    split(Some(size))
}

/// provide compatibility for `smol::channel`, `async_channel`, and `async_std::channel`.
///
/// create (Sender, Receiver) tuple with unbounded size. (unlimited number of queued messages).
pub fn unbounded<T>() -> (Chan<T>, Chan<T>) {
    split(None)
}

/// this module provides compatibility for `std::sync::{mpsc, mpmc}`.
pub mod std {
    use super::*;

    /// provide compatibility for `std::sync::mpsc::unbounded` and `std::sync::mpmc::unbounded`.
    ///
    /// internally calls [`unbounded()`]
    pub fn channel<T>() -> (Chan<T>, Chan<T>) {
        unbounded()
    }

    /// provide compatibility for `std::sync::mpsc::bounded` and `std::sync::mpmc::bounded`.
    ///
    /// internally calls [`unbounded()`]
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
    pub fn channel<T>(size: usize) -> (Chan<T>, Chan<T>) {
        bounded(size)
    }

    /// provide compatibility for `tokio::sync::mpsc::unbounded_channel`.
    ///
    /// internally calls [`unbounded()`]
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

impl core::fmt::Display for ChanError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> Result<(), core::fmt::Error> {
        use ChanError::*;
        f.write_str(
            match self {
                Full => "Chan is Full",
                Empty => "Chan is Empty",
                Closed => "Chan is Closed",
                RecvOnly => "Chan is Receiver but try to sending",
                SendOnly => "Chan is Sender but try to receiving",
                NullPtr => "Chan::try_recv got Null Ptr",
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
    queues: TreeIndex<usize, Arc<Queue<T>>>, // sender_id -> Queue<T>
    len: AtomicUsize,
    size: Option<usize>,
    global_closed: AtomicBool,
    chan_id_counter: AtomicUsize,
}
unsafe impl<T> Send for ChanInner<T> {}
unsafe impl<T> Sync for ChanInner<T> {}

impl<T> ChanInner<T> {
    fn new(size: Option<usize>) -> Self {
        Self {
            queues: Default::default(),
            len: AtomicUsize::new(0),
            size,
            global_closed: AtomicBool::new(false),
            chan_id_counter: AtomicUsize::new(100),
        }
    }

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
}

/// (Experimental) multi-producer, multi-consumer (MPMC) channel backend utilizes a [`TreeIndex`] and [`Queue`], where each message can be received by only one of all existing consumers.
///
/// Typically, this is a first-in, first-out (FIFO) channel, provided that only a single sender is active concurrently.
///
/// However, in the event that multiple senders are active concurrently, there is unable to guarantee the ordering of messages.
///
/// # in the case of multi-senders
/// If the `rand` feature was not enabled (by default), then the Sender with the smallest ID would be prioritized over the others (the default behavior of [`TreeIndex::iter()`]), which could starve the other Sender with the larger ID.
///
/// if enables the `rand` feature, then [`rand::seq::SliceRandom::shuffle`](https://docs.rs/rand/0.9.0/rand/seq/trait.SliceRandom.html#tymethod.shuffle) is used for shuffled disorderly iterating, which should ensure some degree of fairness. 
#[derive(Debug)]
pub struct Chan<T: 'static> {
    id: usize,
    flag: Arc<AtomicU8>,
    inner: Arc<ChanInner<T>>,
    dropped: bool,
}
unsafe impl<T> Send for Chan<T> {}
unsafe impl<T> Sync for Chan<T> {}

impl<T> Drop for Chan<T> {
    fn drop(&mut self) {
        if self.dropped {
            return;
        }
        self.dropped = true;

        self.close();
    }
}

impl<T> Clone for Chan<T> {
    fn clone(&self) -> Self {
        let flag = self.flag();
        let inner = self.inner.clone();
        let id = inner.gen_id();

        Self::init(id, flag, inner)
    }
}

impl<T> Default for Chan<T> {
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

    fn init(id: usize, flag: u8, inner: Arc<ChanInner<T>>) -> Self {
        let _ = inner.queues.insert(id, Default::default());
        Self {
            id,
            flag: Arc::new(AtomicU8::new(flag)),
            inner,
            dropped: false,
        }
    }

    /// create a Channel with optional size (max length of queued messages)
    pub fn new(size: Option<usize>) -> Self {
        let inner = Arc::new(ChanInner::new(size));
        let id = inner.gen_id();
        Self::init(id, Self::RECV_SEND, inner)
    }

    /// create bounded Channel (limited number of queued messages).
    pub fn bounded(size: usize) -> Self {
        Self::new(Some(size))
    }

    /// create unbounded Channel (unlimited number of queued messages).
    pub fn unbounded() -> Self {
        Self::new(None)
    }

    /// create two side from this channel: the left side is sender, and the right side is receiver.
    ///
    /// this is does not work if this is not a bidirectional channel.
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
    pub fn flag(&self) -> u8 {
        self.flag.load(Relaxed)
    }

    fn set_flag(&self, flag: u8) {
        if self.is_closed() {
            return;
        }
        self.flag.store(flag, Relaxed);
    }

    /// restrict this Chan instance for receive message only.
    pub fn recv_only(&self) {
        self.set_flag(Self::RECV_ONLY);
    }
    /// restrict this Chan instance for send message only.
    pub fn send_only(&self) {
        self.set_flag(Self::SEND_ONLY);
    }

    /// checks whether this channel is bidirectional (can send and receive messages)
    pub fn is_bidirectional(&self) -> bool {
        self.flag() == Self::RECV_SEND
    }
    /// checks whether this channel able to receive messages.
    pub fn is_receiver(&self) -> bool {
        self.is_bidirectional() || self.flag() == Self::RECV_ONLY
    }
    /// checks whether this channel able to send messages.
    pub fn is_sender(&self) -> bool {
        self.is_bidirectional() || self.flag() == Self::SEND_ONLY
    }

    /// get current number of senders. this internally calls [`TreeIndex::len()`] so the time complexity is `O(N)`.
    pub fn senders(&self) -> usize {
        self.inner.queues.len()
    }

    /// get the current length of queued messages in this channel.
    pub fn len(&self) -> usize {
        self.inner.len.load(Acquire)
    }

    /// checks whether this channel closed.
    pub fn is_closed(&self) -> bool {
        if self.dropped {
            return true;
        }

        if self.inner.global_closed.load(Relaxed) {
            return true;
        }

        if self.flag() == Self::CLOSED {
            return true;
        }

        ! self.inner.queues.contains(&self.id)
    }

    /// closing this sender and remove it from global register.
    pub fn close(&self) {
        let _ = self.inner.queues.remove(&self.id);
        self.set_flag(Self::CLOSED);
    }

    /// globally closing this channel so all senders will no longer able to send messages.
    pub fn close_all(&self) {
        self.close();
        self.inner.global_closed.store(true, Relaxed);
        self.inner.queues.clear();
    }

    /// send message.
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
            match
                self.inner.queues.peek_with(&self.id, |k, v| {
                    assert_eq!(self.id, *k);
                    v.clone()
                })
            {
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
        Ok(())
    }

    /// try receive message from this channel.
    /// this is never blocking, if there is no message, it will returns ChanError::Empty.
    pub fn try_recv(&self) -> Result<T, ChanError> {
        if self.is_closed() {
            return Err(ChanError::Closed);
        }

        if self.flag() == Self::SEND_ONLY {
            return Err(ChanError::SendOnly);
        }

        let guard = Guard::new();
        let iter = self.inner.queues.iter(&guard);

        #[cfg(feature="rand")]
        let iter = {
            use rand::seq::SliceRandom;
            let mut vec: Vec<(&usize, &Arc<Queue<T>>)> = iter.collect();
            let mut rng = rand::thread_rng();
            vec.shuffle(&mut rng);
            vec.into_iter()
        };

        for (id, queue) in iter {
            if *id == self.id {
                // skip myself.
                // so a thread both sending and receiving will only receive messages from other senders.
                continue;
            }

            if let Some(mut shared) = queue.pop() {
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

                if let Some(entry) = unsafe { shared.get_mut() } {
                    return Ok(unsafe { entry.take_inner() });
                } else {
                    let guard = Guard::new();
                    let ptr = shared.get_guarded_ptr(&guard).as_ptr();
                    let entry = unsafe { &mut *(ptr as *mut Entry<T>) };
                    return Ok(unsafe { entry.take_inner() });
                }
            }
        }

        Err(ChanError::Empty)
    }

    fn clone_without_change_id(&self) -> Self {
        let flag = self.flag();
        let inner = self.inner.clone();
        let id = self.id;

        Self::init(id, flag, inner)
    }

    /// Asynchronous receive message from this channel
    pub fn recv(&self) -> ChanRecv<T> {
        ChanRecv(self.clone_without_change_id())
    }
}

/// Chan::recv() returns this type that implements [`core::future::Future`] for awaiting
pub struct ChanRecv<T: 'static>(
    Chan<T>
);
impl<T> core::future::Future for ChanRecv<T> {
    type Output = Result<T, ChanError>;

    fn poll(
        self: core::pin::Pin<&mut Self>,
        _ctx: &mut core::task::Context<'_>
    ) -> core::task::Poll<Self::Output> {
        use core::task::Poll;
        match self.0.try_recv() {
            Ok(msg) => Poll::Ready(Ok(msg)),
            Err(err) => {
                if err == ChanError::Empty {
                    Poll::Pending
                } else {
                    Poll::Ready(Err(err))
                }
            }
        }
    }
}

