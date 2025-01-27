//! MPMC channel backend by [`Queue`].

use core::sync::atomic::{
    AtomicBool,
    AtomicUsize,
    Ordering::{Relaxed, Acquire},
};

extern crate alloc;
use alloc::sync::Arc;

use crate::Queue;
use crate::ebr::Guard;
use crate::linked_list::Entry;

#[derive(Debug)]
struct ChanInner<T> {
    msg: Queue<T>,
    len: AtomicUsize,
    size: Option<usize>,
    closed: AtomicBool,
}
unsafe impl<T> Send for ChanInner<T> {}
unsafe impl<T> Sync for ChanInner<T> {}

/// (Experimental) Multi-producer, multi-consumer FIFO channel.
#[derive(Clone, Debug)]
pub struct Chan<T>(
    Arc<ChanInner<T>>
);
unsafe impl<T> Send for Chan<T> {}
unsafe impl<T> Sync for Chan<T> {}

/// Error type for Chan
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ChanError {
    /// Channel full (reached it's max length of queued messages)
    Full,

    /// Channel is empty currently.
    Empty,

    /// Channel is closed.
    Closed,

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
                NullPtr => "Chan::try_recv got Null Ptr",
                Other(s) => {
                    f.write_str("Chan Unknown Error: ")?;
                    return f.write_str(&s);
                },
            }
        )
    }
}

impl<T> Default for Chan<T> {
    fn default() -> Self {
        Self::new(None)
    }
}

impl<T> Chan<T> {
    /// create a Channel with optional size (max length of queued messages)
    pub fn new(size: Option<usize>) -> Self {
        Self(Arc::new(ChanInner {
            msg: Default::default(),
            len: AtomicUsize::new(0),
            size,
            closed: AtomicBool::new(false),
        }))
    }

    /// get the current length of channel.
    pub fn len(&self) -> usize {
        self.0.len.load(Relaxed)
    }

    /// checks whether this channel closed.
    pub fn is_closed(&self) -> bool {
        self.0.closed.load(Relaxed)
    }

    /// closing this channel.
    pub fn close(&self) {
        self.0.closed.store(true, Relaxed);
    }

    /// try receive message from this channel.
    /// this is never blocking, if there is no message, it will returns ChanError::Empty.
    pub fn try_recv(&self) -> Result<T, ChanError> {
        if self.is_closed() {
            return Err(ChanError::Closed);
        }
        if let Some(mut shared) = self.0.msg.pop() {
            if let Some(entry) = unsafe { shared.get_mut() } {
                Ok(unsafe { entry.take_inner() })
            } else {
                let guard = Guard::new();
                let ptr = shared.get_guarded_ptr(&guard).as_ptr();
                let entry = unsafe { &mut *(ptr as *mut Entry<T>) };
                Ok(unsafe { entry.take_inner() })
            }
        } else {
            Err(ChanError::Empty)
        }
    }

    /// send message but does not checking the lifetime of T.
    pub unsafe fn send_unchecked(&self, val: T) -> Result<(), ChanError> {
        if self.is_closed() {
            return Err(ChanError::Closed);
        }
        if let Some(size) = self.0.size {
            if self.len() >= size {
                return Err(ChanError::Full);
            }
        }
        unsafe { self.0.msg.push_unchecked(val); }
        self.0.len.fetch_add(1, Acquire);
        Ok(())
    }
}

impl<T: 'static> Chan<T> {
    /// send message.
    pub fn send(&self, val: T) -> Result<(), ChanError> {
        unsafe { self.send_unchecked(val) }
    }
}
