//! One shutdown deadline and completion barrier for connections and their tasks.
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::{Instant, Sleep, sleep_until, timeout_at};

/// Lazily allocated stall/idle timer shared by streaming HTTP bodies and
/// WebSocket endpoints so immediate operations never allocate a [`Sleep`].
pub(crate) struct LazyDeadline {
    sleep: Option<Pin<Box<Sleep>>>,
    armed: bool,
}

impl LazyDeadline {
    pub(crate) const fn new() -> Self {
        Self {
            sleep: None,
            armed: false,
        }
    }

    pub(crate) const fn is_armed(&self) -> bool {
        self.armed
    }

    pub(crate) const fn disarm(&mut self) {
        self.armed = false;
    }

    /// Arm (if currently disarmed) for `now + duration` and poll whether the
    /// deadline has elapsed. Automatically disarms on expiration.
    pub(crate) fn poll_expired(&mut self, cx: &mut Context<'_>, duration: Duration) -> bool {
        self.poll_expired_with(cx, || Instant::now() + duration)
    }

    /// Arm (if currently disarmed) for the instant returned by `deadline` and poll
    /// whether it has elapsed. Automatically disarms on expiration.
    pub(crate) fn poll_expired_with(
        &mut self,
        cx: &mut Context<'_>,
        deadline: impl FnOnce() -> Instant,
    ) -> bool {
        let sleep = match self.sleep {
            Some(ref mut sleep) => {
                if !self.armed {
                    sleep.as_mut().reset(deadline());
                    self.armed = true;
                }
                sleep
            }
            None => {
                self.armed = true;
                self.sleep.insert(Box::pin(sleep_until(deadline())))
            }
        };
        let expired = sleep.as_mut().poll(cx).is_ready();
        if expired {
            self.armed = false;
        }
        expired
    }

    #[cfg(test)]
    pub(crate) const fn is_allocated(&self) -> bool {
        self.sleep.is_some()
    }
}

struct ShutdownInner {
    deadline: OnceLock<Instant>,
    notify: Notify,
    active: AtomicUsize,
    drained: Notify,
}

impl ShutdownInner {
    /// Wait until shutdown is triggered and return the drain deadline.
    ///
    /// Registers the task's [`Waker`] in [`Notify`] at most once per waker so
    /// steady-state polls from `biased` `select!` loops (`Shutdown::bound`,
    /// `drive_graceful`, `bridge_ws`) cost a single atomic load and pointer check
    /// instead of locking `notify.waiters` on every socket wake.
    async fn deadline(&self) -> Instant {
        if let Some(&at) = self.deadline.get() {
            return at;
        }
        let notified = self.notify.notified();
        tokio::pin!(notified);
        let mut registered_waker: Option<Waker> = None;
        std::future::poll_fn(|cx| {
            if let Some(&at) = self.deadline.get() {
                return Poll::Ready(at);
            }
            if registered_waker
                .as_ref()
                .is_some_and(|w| w.will_wake(cx.waker()))
            {
                return Poll::Pending;
            }
            // `ShutdownTx` stores `deadline` before `notify_waiters()`, so any wake
            // landing between the check above and registration is observed right below.
            let _ = notified.as_mut().poll(cx);
            if let Some(&at) = self.deadline.get() {
                return Poll::Ready(at);
            }
            registered_waker = Some(cx.waker().clone());
            Poll::Pending
        })
        .await
    }

    async fn bound<F: Future>(&self, task: F) -> Option<F::Output> {
        tokio::pin!(task);
        tokio::select! {
            biased;
            end = self.deadline() => timeout_at(end, task).await.ok(),
            value = &mut task => Some(value),
        }
    }
}

pub(crate) struct ShutdownTx(Arc<ShutdownInner>);

impl ShutdownTx {
    pub(crate) fn send(self, at: Instant) {
        if self.0.deadline.set(at).is_ok() {
            self.0.notify.notify_waiters();
        }
    }
}

impl Drop for ShutdownTx {
    fn drop(&mut self) {
        if self.0.deadline.get().is_none() && self.0.deadline.set(Instant::now()).is_ok() {
            self.0.notify.notify_waiters();
        }
    }
}

#[derive(Clone)]
pub(crate) struct Shutdown(Arc<ShutdownInner>);

impl Shutdown {
    pub(crate) fn deadline(&self) -> impl Future<Output = Instant> {
        self.0.deadline()
    }

    /// Run `task` until it completes or the drain deadline passes, whichever is
    /// first. `None` means the deadline cancelled it.
    pub(crate) fn bound<F: Future>(&self, task: F) -> impl Future<Output = Option<F::Output>> {
        self.0.bound(task)
    }
}

/// Single-consumer completion barrier resolved once every [`Lifecycle`] token has dropped.
pub(crate) struct Drain(Arc<ShutdownInner>);

impl Drain {
    /// Wait until every accepted connection (via `ServerBuilders`), HTTP/2 request
    /// task, and upgraded WebSocket relay holding a [`Lifecycle`] token has dropped.
    pub(crate) async fn wait(self) {
        loop {
            if self.0.active.load(Ordering::Acquire) == 0 {
                return;
            }
            self.0.drained.notified().await;
        }
    }
}

pub(crate) fn channel() -> (ShutdownTx, Lifecycle, Drain) {
    let inner = Arc::new(ShutdownInner {
        deadline: OnceLock::new(),
        notify: Notify::new(),
        active: AtomicUsize::new(1),
        drained: Notify::new(),
    });
    (
        ShutdownTx(Arc::clone(&inner)),
        Lifecycle(Arc::clone(&inner)),
        Drain(inner),
    )
}

/// Drain-tracked handle sharing the server's [`ShutdownInner`] allocation (`8` bytes).
///
/// Only cloning an existing [`Lifecycle`] increments `active`, so the `1 -> 0` transition
/// happens at most once across a server's lifetime. [`Drain::wait`] reads `active` before
/// registering a waiter, so it relies on `notify_one` storing a permit when nobody is
/// waiting yet; `notify_waiters` would lose a wakeup landing between the load and wait.
pub(crate) struct Lifecycle(Arc<ShutdownInner>);

impl Clone for Lifecycle {
    fn clone(&self) -> Self {
        self.0.active.fetch_add(1, Ordering::Relaxed);
        Self(Arc::clone(&self.0))
    }
}

impl Drop for Lifecycle {
    fn drop(&mut self) {
        if self.0.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            // Must remain `notify_one` (stored permit), not `notify_waiters`, so a
            // `1 -> 0` drop before `Drain::wait` registers `notified()` cannot be lost.
            self.0.drained.notify_one();
        }
    }
}

impl Lifecycle {
    pub(crate) fn deadline(&self) -> impl Future<Output = Instant> {
        self.0.deadline()
    }

    /// Return a non-drain-tracked [`Shutdown`] handle sharing this server's deadline.
    pub(crate) fn shutdown(&self) -> Shutdown {
        Shutdown(Arc::clone(&self.0))
    }

    /// Run a closure with direct access to [`Shutdown`] while guaranteeing its
    /// future (and any captured sockets, permits, or metric guards) drops before
    /// releasing the completion barrier.
    pub(crate) async fn run_with<F, Fut, T>(self, f: F) -> T
    where
        F: FnOnce(Shutdown) -> Fut,
        Fut: Future<Output = T>,
    {
        let res = f(self.shutdown()).await;
        drop(self);
        res
    }

    async fn run<F: Future>(self, task: F) -> Option<F::Output> {
        let res = self.0.bound(task).await;
        drop(self);
        res
    }
}

// Hyper's HTTP/2 request futures can outlive the connection future. Bound and
// track them too, including upstream handshakes that have not created a relay.
impl<F> hyper::rt::Executor<F> for Lifecycle
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, task: F) {
        tokio::spawn(self.clone().run(task));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::Wake;

    #[tokio::test]
    async fn completion_barrier_waits_for_cancelled_future_cleanup() {
        struct Dropped(Arc<AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        for closing in [Some(Instant::now()), None] {
            let (shutdown, lifecycle, drain) = channel();
            if let Some(at) = closing {
                shutdown.send(at);
            } else {
                drop(shutdown);
            }
            let dropped = Arc::new(AtomicBool::new(false));
            let guard = Dropped(dropped.clone());
            let task = lifecycle.run(async move {
                let _guard = guard;
                std::future::pending::<()>().await;
            });
            let handle = tokio::spawn(task);
            tokio::time::timeout(Duration::from_secs(1), drain.wait())
                .await
                .expect("late or closed shutdown must cancel the task");
            assert!(
                dropped.load(Ordering::Acquire),
                "completion must follow resource cleanup"
            );
            assert!(handle.await.unwrap().is_none());
        }

        // Dropping the last `Lifecycle` before `drain.wait()` is polled must still resolve immediately.
        let (_shutdown, lifecycle, drain) = channel();
        drop(lifecycle);
        tokio::time::timeout(Duration::from_secs(1), drain.wait())
            .await
            .expect("already-drained barrier must resolve immediately");
    }

    #[tokio::test]
    async fn deadline_caches_waker_re_registers_on_migration_and_wakes_on_send() {
        struct CountWake(AtomicUsize);
        impl Wake for CountWake {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let (tx, lifecycle, _drain) = channel();
        let rx = lifecycle.shutdown();
        let fut = rx.deadline();
        tokio::pin!(fut);

        let w1_state = Arc::new(CountWake(AtomicUsize::new(0)));
        let w1 = Waker::from(Arc::clone(&w1_state));
        let mut cx1 = Context::from_waker(&w1);
        assert!(fut.as_mut().poll(&mut cx1).is_pending());
        // Steady-state repeat poll with the same waker stays Pending via fast path.
        assert!(fut.as_mut().poll(&mut cx1).is_pending());

        // Task migration to a distinct waker updates registration so `send` wakes `w2`.
        let w2_state = Arc::new(CountWake(AtomicUsize::new(0)));
        let w2 = Waker::from(Arc::clone(&w2_state));
        let mut cx2 = Context::from_waker(&w2);
        assert!(fut.as_mut().poll(&mut cx2).is_pending());

        let target = Instant::now() + Duration::from_secs(5);
        tx.send(target);
        assert_eq!(w2_state.0.load(Ordering::SeqCst), 1);
        assert_eq!(fut.as_mut().poll(&mut cx2), Poll::Ready(target));
    }
}
