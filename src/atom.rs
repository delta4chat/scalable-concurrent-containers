//! Atom elements

use super::ebr::{AtomicShared, Guard, Shared, Tag};

use core::sync::atomic::Ordering::Acquire; //{AcqRel, Acquire, Relaxed};
use core::fmt::{Debug, Formatter};

extern crate alloc;
use alloc::sync::Arc;

/// Atom is a container that only store single element.
pub struct Atom<T> {
    value: AtomicShared<Arc<T>>,
}

impl<T: Debug> Debug for Atom<T> {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), core::fmt::Error> {
        let val = self.get();
        f.debug_tuple("Atom").field(&val).finish()
    }
}

impl<T> Default for Atom<T> {
    #[inline]
    fn default() -> Self {
        Self::init()
    }
}

impl<T> Atom<T> {
    /// initialize Atom with no value.
    #[inline]
    pub fn init() -> Self {
        Self {
            value: AtomicShared::default(),
        }
    }
}

impl<T: 'static> Atom<T> {
    /// creates new instance of Atom for provided value
    #[inline]
    pub fn new(val: T) -> Self {
        unsafe { Self::new_unchecked(val) }
    }

    /// creates new instance of Atom for provided Arc-wrapped value
    #[inline]
    pub fn new_arc(arc_val: Arc<T>) -> Self {
        unsafe { Self::new_arc_unchecked(arc_val) }
    }
}

impl<T> Atom<T> {
    /// creates new instance of Atom for provided value
    ///
    /// ## <big>however, this is not checking the lifetime of T.</big>
    ///
    /// # <big>Safety</big>
    /// `T::drop` can be run after the [`Atom`] is dropped,
    /// therefore it is safe only if `T::drop` does not access short-lived data,
    /// or [`core::mem::needs_drop`] is `false` for `T`.
    #[inline]
    pub unsafe fn new_unchecked(val: T) -> Self {
        let this = Self::init();
        unsafe { this.set_unchecked(val); }
        this
    }

    /// creates new instance of Atom for provided Arc-wrapped value
    ///
    /// ## <big>however, this is not checking the lifetime of T.</big>
    ///
    /// # <big>Safety</big>
    /// `T::drop` can be run after the [`Atom`] is dropped,
    /// therefore it is safe only if `T::drop` does not access short-lived data,
    /// or [`core::mem::needs_drop`] is `false` for `T`.
    #[inline]
    pub unsafe fn new_arc_unchecked(arc_val: Arc<T>) -> Self {
        let this = Self::init();
        unsafe { this.set_arc_unchecked(arc_val); }
        this
    }
}

impl<T> Clone for Atom<T> {
    #[inline]
    fn clone(&self) -> Self {
        let self_clone = Self::init();
        if let Some(arc_val) = self.get() {
            unsafe { self_clone.set_arc_unchecked(arc_val); }
        }
        self_clone
    }
}

impl<T: 'static> Atom<T> {
    /// set Atom to provided value. returns the Arc-wrapped-value of you provided value.
    #[inline]
    pub fn set(&self, val: T) -> Arc<T> {
        unsafe { self.set_unchecked(val) }
    }

    /// set Atom to provided Arc-wrapped value.
    #[inline]
    pub fn set_arc(&self, arc_val: Arc<T>) {
        unsafe { self.set_arc_unchecked(arc_val) }
    }

    /// set Atom to provided value and returns the old value.
    #[inline]
    pub fn swap(&self, val: T) -> Option<Arc<T>> {
        unsafe { self.swap_unchecked(val) }
    }

    /// set Atom to provided Arc-wrapped value and returns the old value.
    #[inline]
    pub fn swap_arc(&self, arc_val: Arc<T>) -> Option<Arc<T>> {
        unsafe { self.swap_arc_unchecked(arc_val) }
    }
}

impl<T> Atom<T> {
    /// set Atom to provided value. returns the Arc-wrapped-value of you provided value.
    ///
    /// ## <big>however, this is not checking the lifetime of T.</big>
    ///
    /// # <big>Safety</big>
    /// `T::drop` can be run after the [`Atom`] is dropped,
    /// therefore it is safe only if `T::drop` does not access short-lived data,
    /// or [`core::mem::needs_drop`] is `false` for `T`.
    #[inline]
    pub unsafe fn set_unchecked(&self, val: T) -> Arc<T> {
        let arc_val = Arc::new(val);
        unsafe { self.set_arc_unchecked(arc_val.clone()); }
        arc_val
    }

    /// set Atom to provided Arc-wrapped value.
    ///
    /// ## <big>however, this is not checking the lifetime of T.</big>
    ///
    /// # <big>Safety</big>
    /// `T::drop` can be run after the [`Atom`] is dropped,
    /// therefore it is safe only if `T::drop` does not access short-lived data,
    /// or [`core::mem::needs_drop`] is `false` for `T`.
    #[inline]
    pub unsafe fn set_arc_unchecked(&self, arc_val: Arc<T>) {
        let shared = Some(unsafe { Shared::new_unchecked(arc_val) });
        self.value.swap((shared, Tag::None), Acquire);
    }

    /// set Atom to provided value and returns the old value.
    ///
    /// ## <big>however, this is not checking the lifetime of T.</big>
    ///
    /// # <big>Safety</big>
    /// `T::drop` can be run after the [`Atom`] is dropped,
    /// therefore it is safe only if `T::drop` does not access short-lived data,
    /// or [`core::mem::needs_drop`] is `false` for `T`.
    #[inline]
    pub unsafe fn swap_unchecked(&self, val: T) -> Option<Arc<T>> {
        let arc_val = Arc::new(val);
        unsafe { self.swap_arc_unchecked(arc_val) }
    }

    /// set Atom to provided Arc-wrapped value and returns the old value.
    ///
    /// ## <big>however, this is not checking the lifetime of T.</big>
    ///
    /// # <big>Safety</big>
    /// `T::drop` can be run after the [`Atom`] is dropped,
    /// therefore it is safe only if `T::drop` does not access short-lived data,
    /// or [`core::mem::needs_drop`] is `false` for `T`.
    #[inline]
    pub unsafe fn swap_arc_unchecked(&self, arc_val: Arc<T>) -> Option<Arc<T>> {
        let shared = Some(unsafe { Shared::new_unchecked(arc_val) });
        if let Some(old_shared) = self.value.swap((shared, Tag::None), Acquire).0 {
            let guard = Guard::new();
            Some(old_shared.get_guarded_ref(&guard).clone())
        } else {
            None
        }
    }

    /// get inner value.
    #[inline]
    pub fn get(&self) -> Option<Arc<T>> {
        let guard = Guard::new();
        self.value.load(Acquire, &guard).as_ref().cloned()
    }

    /// take the inner value and set it to None.
    #[inline]
    pub fn take(&self) -> Option<Arc<T>> {
        if let Some(shared) = self.value.swap((None, Tag::None), Acquire).0 {
            let guard = Guard::new();
            Some(shared.get_guarded_ref(&guard).clone())
        } else {
            None
        }
    }
}

impl<T: 'static> Atom<T> {
    /// update the provided value if any.
    ///
    /// ## <big>however, this is not checking the lifetime of T.</big>
    ///
    /// # <big>Safety</big>
    /// `T::drop` can be run after the [`Atom`] is dropped,
    /// therefore it is safe only if `T::drop` does not access short-lived data,
    /// or [`core::mem::needs_drop`] is `false` for `T`.
    #[inline]
    pub fn update(
        &self,
        f: impl FnMut(Option<Arc<T>>)->Option<T>
    ) -> (Option<Arc<T>>, Option<Arc<T>>) {
        unsafe { self.update_unchecked(f) }
    }

    /// update the provided Arc-wrapped value if any.
    ///
    /// ## <big>however, this is not checking the lifetime of T.</big>
    ///
    /// # <big>Safety</big>
    /// `T::drop` can be run after the [`Atom`] is dropped,
    /// therefore it is safe only if `T::drop` does not access short-lived data,
    /// or [`core::mem::needs_drop`] is `false` for `T`.
    #[inline]
    pub fn update_arc(
        &self,
        f: impl FnMut(Option<Arc<T>>)->Option<Arc<T>>
    ) -> (Option<Arc<T>>, Option<Arc<T>>) {
        unsafe { self.update_arc_unchecked(f) }
    }
}

impl<T> Atom<T> {
    /// update the provided value if any.
    #[inline]
    pub unsafe fn update_unchecked(
        &self,
        mut f: impl FnMut(Option<Arc<T>>)->Option<T>
    ) -> (Option<Arc<T>>, Option<Arc<T>>) {
        let old = self.get();
        if let Some(new) = f(old.clone()) {
            unsafe { self.set_unchecked(new); }
        }
        (old, self.get())
    }

    /// update the provided Arc-wrapped value if any.
    #[inline]
    pub unsafe fn update_arc_unchecked(
        &self,
        mut f: impl FnMut(Option<Arc<T>>)->Option<Arc<T>>
    ) -> (Option<Arc<T>>, Option<Arc<T>>) {
        let old = self.get();
        if let Some(new) = f(old.clone()) {
            unsafe { self.set_arc_unchecked(new); }
        }
        (old, self.get())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn debug() {
        dbg!(Atom::<u8>::init());
        eprintln!("{:?}", Atom::new(2*8*16));
    }

    #[test]
    fn concurrect() {
        let a = Arc::new(Atom::new(19440u128));

        let mut thrs = Vec::new();
        for i in 0..10 {
            let a = a.clone();
            let thr = std::thread::spawn(move || {
                let mut n: u128 = 1000;
                if (i % 2) == 0 {
                    n = 15;
                }
                for _ in 0..n {
                    if (i % 2) == 0 {
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    }
                    a.update(|n| {
                        eprintln!("n={n:?}");
                        n.map(|n| { (*n) + i + 1 })
                    });
                }
            });
            thrs.push(thr);
        }

        for thr in thrs.into_iter() {
            thr.join().unwrap();
        }
    }
}
