//! Atom elements

use super::ebr::{AtomicShared, Guard, Shared, Tag};

use core::sync::atomic::Ordering::Acquire; //{AcqRel, Acquire, Relaxed};

extern crate alloc;
use alloc::sync::Arc;

/// Atom is a container that only store single element.
pub struct Atom<T: ?Sized> {
    value: AtomicShared<Arc<T>>,
}

unsafe impl<T: ?Sized> Send for Atom<T> {}
unsafe impl<T: ?Sized> Sync for Atom<T> {}

impl<T: ?Sized + core::fmt::Debug> core::fmt::Debug for Atom<T> {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> Result<(), core::fmt::Error> {
        let val = self.get();
        f.debug_tuple("Atom").field(&val).finish()
    }
}
impl<T: ?Sized + core::fmt::Display> core::fmt::Display for Atom<T> {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> Result<(), core::fmt::Error> {
        if let Some(val) = self.get() {
            f.write_str(val.to_string().as_ref())
        } else {
            Err(core::fmt::Error)
        }
    }
}

impl<T: ?Sized> From<&Atom<T>> for Option<Arc<T>> {
    fn from(val: &Atom<T>) -> Option<Arc<T>> {
        val.get()
    }
}
impl<T: ?Sized> From<Atom<T>> for Option<Arc<T>> {
    fn from(val: Atom<T>) -> Option<Arc<T>> {
        (&val).into()
    }
}

impl<T: ?Sized + PartialEq> PartialEq for Atom<T> {
    fn eq(&self, other: &Self) -> bool {
        self.get() == other.get()
    }
}
impl<T: ?Sized + Eq> Eq for Atom<T> {}

impl<T: ?Sized + PartialOrd> PartialOrd for Atom<T> {
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        let other = other.get();
        self.get().partial_cmp(&other)
    }
}
impl<T: ?Sized + Ord> Ord for Atom<T> {
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        let other = other.get();
        self.get().cmp(&other)
    }
}

impl<T: ?Sized + core::hash::Hash> core::hash::Hash for Atom<T> {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.get().hash(state)
    }
}

impl<T: 'static + Sized> From<T> for Atom<T> {
    fn from(val: T) -> Self {
        Self::new(val)
    }
}
impl<T: 'static + ?Sized + Clone> From<&T> for Atom<T> {
    fn from(val: &T) -> Self {
        val.clone().into()
    }
}

impl<T: 'static + ?Sized> From<Arc<T>> for Atom<T> {
    fn from(arc_val: Arc<T>) -> Self {
        Self::new_arc(arc_val)
    }
}
impl<T: 'static + ?Sized> From<&Arc<T>> for Atom<T> {
    fn from(arc_val: &Arc<T>) -> Self {
        arc_val.clone().into()
    }
}
impl<T: 'static + ?Sized + Clone> From<Arc<&T>> for Atom<T> {
    fn from(arc_val: Arc<&T>) -> Self {
        (*arc_val).into()
    }
}

impl<T: 'static + ?Sized> From<Box<T>> for Atom<T> {
    fn from(box_val: Box<T>) -> Self {
        let arc_val: Arc<T> = box_val.into();
        Self::new_arc(arc_val)
    }
}
impl<T: 'static + ?Sized + Clone> From<Box<&T>> for Atom<T> {
    fn from(box_val: Box<&T>) -> Self {
        (*box_val).into()
    }
}
impl<T: 'static + ?Sized + Clone> From<&Box<T>> for Atom<T> {
    fn from(box_val: &Box<T>) -> Self {
        box_val.clone().into()
    }
}

impl<T: ?Sized> Default for Atom<T> {
    #[inline]
    fn default() -> Self {
        Self::init()
    }
}

impl<T: ?Sized> Atom<T> {
    /// initialize Atom with no value.
    #[inline]
    pub fn init() -> Self {
        Self {
            value: AtomicShared::default(),
        }
    }
}

impl<T: 'static + Sized> Atom<T> {
    /// creates new instance of Atom for provided value
    #[inline]
    pub fn new(val: T) -> Self {
        unsafe { Self::new_unchecked(val) }
    }
}

impl<T: 'static + ?Sized> Atom<T> {
    /// creates new instance of Atom for provided Arc-wrapped value
    #[inline]
    pub fn new_arc(arc_val: Arc<T>) -> Self {
        unsafe { Self::new_arc_unchecked(arc_val) }
    }
}

impl<T: Sized> Atom<T> {
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
}

impl<T: ?Sized> Atom<T> {
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

impl<T: ?Sized> Clone for Atom<T> {
    #[inline]
    fn clone(&self) -> Self {
        let self_clone = Self::init();
        if let Some(arc_val) = self.get() {
            unsafe { self_clone.set_arc_unchecked(arc_val); }
        }
        self_clone
    }
}

impl<T: 'static + Sized> Atom<T> {
    /// set Atom to provided value. returns the Arc-wrapped-value of you provided value.
    #[inline]
    pub fn set(&self, val: T) -> Arc<T> {
        unsafe { self.set_unchecked(val) }
    }

    /// set Atom to provided value and returns the old value.
    #[inline]
    pub fn swap(&self, val: T) -> Option<Arc<T>> {
        unsafe { self.swap_unchecked(val) }
    }
}

impl<T: 'static + ?Sized> Atom<T> {
    /// set Atom to provided Arc-wrapped value.
    #[inline]
    pub fn set_arc(&self, arc_val: Arc<T>) {
        unsafe { self.set_arc_unchecked(arc_val) }
    }

    /// set Atom to provided Arc-wrapped value and returns the old value.
    #[inline]
    pub fn swap_arc(&self, arc_val: Arc<T>) -> Option<Arc<T>> {
        unsafe { self.swap_arc_unchecked(arc_val) }
    }
}

impl<T: Sized> Atom<T> {
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
}

impl<T: ?Sized> Atom<T> {
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

impl<T: 'static + Sized> Atom<T> {
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
}

impl<T: 'static + ?Sized> Atom<T> {
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

impl<T: Sized> Atom<T> {
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
}

impl<T: ?Sized> Atom<T> {
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
