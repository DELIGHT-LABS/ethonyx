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

    /// Extend an already-armed deadline by `delta` while preserving elapsed stall
    /// time (no-op when disarmed or `delta` is zero).
    pub(crate) fn extend(&mut self, delta: Duration) {
        if !delta.is_zero()
            && self.armed
            && let Some(sleep) = &mut self.sleep
        {
            let next = sleep.deadline() + delta;
            sleep.as_mut().reset(next);
        }
    }

    /// Arm (if currently disarmed) for `now + duration` and poll whether the
    /// deadline has elapsed. Automatically disarms on expiration.
    pub(crate) fn poll_expired(&mut self, cx: &mut Context<'_>, duration: Duration) -> bool {
        let sleep = match &mut self.sleep {
            Some(sleep) => {
                if !self.armed {
                    sleep.as_mut().reset(Instant::now() + duration);
                }
                sleep
            }
            slot @ None => slot.insert(Box::pin(sleep_until(Instant::now() + duration))),
        };
        let expired = sleep.as_mut().poll(cx).is_ready();
        self.armed = !expired;
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
    fn new(active: usize) -> Arc<Self> {
        Arc::new(Self {
            deadline: OnceLock::new(),
            notify: Notify::new(),
            active: AtomicUsize::new(active),
            drained: Notify::new(),
        })
    }

    /// Wait until shutdown is triggered and return the drain deadline.
    ///
    /// Registers the task's [`Waker`] in [`Notify`] at most once per waker so
    /// steady-state polls from `poll_fn` / `select!` loops (`Shutdown::bound`,
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
        let end = if let Some(&end) = self.deadline.get() {
            end
        } else {
            // Poll `task` before registering a `Notify` waiter so 1-poll HTTP/2
            // requests (probes, local mocks, immediate errors) skip waiter locking;
            // scoping `self.deadline()` inside `select!` drops `Notified + Option<Waker>`
            // (~80 B) before the `timeout_at` drain await.
            tokio::select! {
                biased;
                value = &mut task => return Some(value),
                end = self.deadline() => end,
            }
        };
        if Instant::now() >= end {
            return None;
        }
        timeout_at(end, task).await.ok()
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
    #[cfg(feature = "otel")]
    pub(crate) fn current_deadline(&self) -> Option<Instant> {
        self.0.deadline.get().copied()
    }

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
        // `active` starts at 1, drops `1 -> 0` at most once (calling `notify_one()`,
        // which stores a permit if no waiter exists yet), and `Drain::wait(self)` is
        // single-consumer — so one `Acquire` fast path + `notified().await` is exact.
        if self.0.active.load(Ordering::Acquire) != 0 {
            self.0.drained.notified().await;
        }
    }
}

pub(crate) fn channel() -> (ShutdownTx, Lifecycle, Drain) {
    let inner = ShutdownInner::new(1);
    (
        ShutdownTx(Arc::clone(&inner)),
        Lifecycle(Arc::clone(&inner)),
        Drain(inner),
    )
}

/// Single-shot shutdown deadline channel without a drain completion barrier.
#[cfg(feature = "otel")]
pub(crate) fn shutdown_channel() -> (ShutdownTx, Shutdown) {
    let inner = ShutdownInner::new(0);
    (ShutdownTx(Arc::clone(&inner)), Shutdown(inner))
}

/// Drain-tracked handle sharing the server's [`ShutdownInner`] allocation (`8` bytes).
///
/// Only cloning an existing [`Lifecycle`] increments `active`, so the `1 -> 0` transition
/// happens at most once across a server's lifetime.
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
            self.0.drained.notify_one();
        }
    }
}

impl Lifecycle {
    pub(crate) fn current_deadline(&self) -> Option<Instant> {
        self.0.deadline.get().copied()
    }

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

    struct CountWake(AtomicUsize);
    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

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

    #[tokio::test(start_paused = true)]
    async fn lazy_deadline_reuses_allocation_across_disarm_and_expiry() {
        let mut deadline = LazyDeadline::new();
        assert!(!deadline.is_allocated());
        assert!(!deadline.is_armed());

        // Disarming an unallocated deadline must stay unallocated.
        deadline.disarm();
        assert!(!deadline.is_allocated());

        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        assert!(!deadline.poll_expired(&mut cx, Duration::from_secs(5)));
        assert!(deadline.is_allocated());
        assert!(deadline.is_armed());

        // Disarming caches `Box<Sleep>` without dropping it, and re-arming resets the target.
        tokio::time::advance(Duration::from_secs(4)).await;
        deadline.disarm();
        assert!(deadline.is_allocated());
        assert!(!deadline.is_armed());

        assert!(!deadline.poll_expired(&mut cx, Duration::from_secs(5)));
        assert!(deadline.is_armed());
        let armed_at = deadline.sleep.as_ref().unwrap().deadline();
        deadline.extend(Duration::ZERO);
        assert_eq!(deadline.sleep.as_ref().unwrap().deadline(), armed_at);
        tokio::time::advance(Duration::from_secs(4)).await;
        // Extending an armed deadline adds delta without discarding the 4s already elapsed.
        deadline.extend(Duration::from_secs(3));
        assert_eq!(
            deadline.sleep.as_ref().unwrap().deadline(),
            armed_at + Duration::from_secs(3)
        );
        assert!(!deadline.poll_expired(&mut cx, Duration::from_secs(5)));
        tokio::time::advance(Duration::from_secs(3)).await;
        assert!(!deadline.poll_expired(&mut cx, Duration::from_secs(5)));
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(deadline.poll_expired(&mut cx, Duration::from_secs(5)));
        assert!(deadline.is_allocated());
        assert!(!deadline.is_armed());
        // Extending while disarmed is a no-op.
        deadline.extend(Duration::from_secs(10));
        assert!(!deadline.is_armed());
    }

    #[tokio::test]
    async fn bound_completes_one_poll_tasks_without_registering_notify_waiter() {
        let (tx, lifecycle, _drain) = channel();
        let shutdown = lifecycle.shutdown();

        // 1-poll Ready future resolves immediately without cloning the waker into `Notify`.
        let w_state = Arc::new(CountWake(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&w_state));
        let mut cx = Context::from_waker(&waker);
        let ready_bound = shutdown.bound(std::future::ready(42usize));
        tokio::pin!(ready_bound);
        assert_eq!(ready_bound.poll(&mut cx), Poll::Ready(Some(42)));
        assert_eq!(Arc::strong_count(&w_state), 2);
        assert_eq!(w_state.0.load(Ordering::SeqCst), 0);

        // A multi-poll future is polled once (not twice) on its first `Pending` turn,
        // and registers the shutdown waker so `tx.send` wakes it.
        let polls = Arc::new(AtomicUsize::new(0));
        let polls_clone = Arc::clone(&polls);
        let pending_bound = shutdown.bound(std::future::poll_fn(move |_cx| {
            let n = polls_clone.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                Poll::Pending
            } else {
                Poll::Ready(99usize)
            }
        }));
        tokio::pin!(pending_bound);
        assert!(pending_bound.as_mut().poll(&mut cx).is_pending());
        assert_eq!(polls.load(Ordering::SeqCst), 1);

        tx.send(Instant::now() + Duration::from_secs(5));
        assert_eq!(w_state.0.load(Ordering::SeqCst), 1);
        assert_eq!(pending_bound.as_mut().poll(&mut cx), Poll::Ready(Some(99)));
        assert_eq!(polls.load(Ordering::SeqCst), 2);

        // Once the shutdown deadline has already elapsed, `bound` returns `None`
        // immediately without polling `task` even once.
        let (expired_tx, expired_lifecycle, _expired_drain) = channel();
        expired_tx.send(Instant::now());
        let expired_polls = Arc::new(AtomicUsize::new(0));
        let expired_polls_clone = Arc::clone(&expired_polls);
        let expired_res = expired_lifecycle
            .shutdown()
            .bound(std::future::poll_fn(move |_cx| {
                expired_polls_clone.fetch_add(1, Ordering::SeqCst);
                Poll::Ready(1usize)
            }))
            .await;
        assert_eq!(expired_res, None);
        assert_eq!(expired_polls.load(Ordering::SeqCst), 0);

        // A pending task woken mid-flight by an already-elapsed (0-grace) deadline
        // must cancel immediately without a redundant extra poll inside `timeout_at`.
        let (zero_tx, zero_lifecycle, _zero_drain) = channel();
        let zero_shutdown = zero_lifecycle.shutdown();
        let zero_polls = Arc::new(AtomicUsize::new(0));
        let zero_polls_clone = Arc::clone(&zero_polls);
        let zero_bound = zero_shutdown.bound(std::future::poll_fn(move |_cx| {
            zero_polls_clone.fetch_add(1, Ordering::SeqCst);
            Poll::<usize>::Pending
        }));
        tokio::pin!(zero_bound);
        assert!(zero_bound.as_mut().poll(&mut cx).is_pending());
        assert_eq!(zero_polls.load(Ordering::SeqCst), 1);

        zero_tx.send(Instant::now());
        assert_eq!(zero_bound.as_mut().poll(&mut cx), Poll::Ready(None));
        assert_eq!(zero_polls.load(Ordering::SeqCst), 2);
    }
}
