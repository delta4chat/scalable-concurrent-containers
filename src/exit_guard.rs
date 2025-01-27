//! This module implements a simplified, yet safe version of
//! [`scopeguard`](https://crates.io/crates/scopeguard).

use std::ops::{Deref, DerefMut};

/// "defer function" called at the Drop trait called of a instance.
pub struct Defer<F: FnOnce()>(
    Option<F>
);

impl<F: FnOnce()> Defer<F> {
    /// create Defer instance with provided function.
    #[inline]
    pub fn new(f: F) -> Self {
        Self(
            Some(f)
        )
    }

    /// cancel this defer function, so that will be never called.
    #[inline]
    pub fn cancel(&mut self) {
        self.0.take();
    }
}
impl<F: FnOnce()> Drop for Defer<F> {
    #[inline]
    fn drop(&mut self) {
        if let Some(f) = self.0.take() {
            f();
        }
    }
}
unsafe impl<F: FnOnce() + Send> Send for Defer<F> {}
unsafe impl<F: FnOnce() + Sync> Sync for Defer<F> {}

/// [`ExitGuard`] captures the environment and invokes the defined closure at the end of the scope.
pub struct ExitGuard<T, F: FnOnce(T)> {
    captured: Option<T>,
    drop_callback: Option<F>,
}

impl<T, F: FnOnce(T)> ExitGuard<T, F> {
    /// Creates a new [`ExitGuard`] with the specified variables captured.
    #[inline]
    pub fn new(captured: T, drop_callback: F) -> Self {
        Self {
            captured: Some(captured),
            drop_callback: Some(drop_callback),
        }
    }

    /// cancel this ExitGuard, so will never calling provided drop_callback.
    #[inline]
    pub fn cancel(&mut self) {
        self.drop_callback.take();
    }

    /// cancel this ExitGuard by calling `self.cancel()`, then delete the inner value.
    ///
    /// this is unsafe because it will causes panic for every Deref or DerefMut.
    ///
    /// # Safety
    /// make sure never de-reference after calling this.
    #[inline]
    pub unsafe fn delete(&mut self) {
        self.cancel();
        self.captured.take();
    }
}

impl<T, F: FnOnce(T)> Drop for ExitGuard<T, F> {
    #[inline]
    fn drop(&mut self) {
        if let Some(f) = self.drop_callback.take() {
            if let Some(c) = self.captured.take() {
                f(c);
            }
        }
    }
}

impl<T, F: FnOnce(T)> Deref for ExitGuard<T, F> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.captured.as_ref().unwrap()
    }
}

impl<T, F: FnOnce(T)> DerefMut for ExitGuard<T, F> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.captured.as_mut().unwrap()
    }
}
