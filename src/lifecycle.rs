//! One shutdown deadline and completion barrier for connections and their tasks.
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use tokio::sync::{Notify, mpsc};
use tokio::time::{Instant, Sleep, sleep_until, timeout_at};

/// Lazily allocated stall/idle timer shared by streaming HTTP bodies and
/// WebSocket endpoints so immediate operations never allocate a [`Sleep`].
#[derive(Default)]
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

    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }

    /// Arm (if currently disarmed) for `now + duration` and poll whether the
    /// deadline has elapsed. Automatically disarms on expiration.
    pub(crate) fn poll_expired(&mut self, cx: &mut Context<'_>, duration: Duration) -> bool {
        if !self.armed {
            let at = Instant::now() + duration;
            match self.sleep.as_mut() {
                Some(sleep) => sleep.as_mut().reset(at),
                None => self.sleep = Some(Box::pin(sleep_until(at))),
            }
            self.armed = true;
        }
        let expired = self
            .sleep
            .as_mut()
            .expect("sleep initialized when armed")
            .as_mut()
            .poll(cx)
            .is_ready();
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
    closed: AtomicBool,
    deadline: OnceLock<Instant>,
    notify: Notify,
}

pub(crate) struct ShutdownTx(Arc<ShutdownInner>);

impl ShutdownTx {
    pub(crate) fn send(self, at: Instant) {
        let _ = self.0.deadline.set(at);
        self.0.closed.store(true, Ordering::SeqCst);
        self.0.notify.notify_waiters();
    }
}

impl Drop for ShutdownTx {
    fn drop(&mut self) {
        if !self.0.closed.swap(true, Ordering::SeqCst) {
            self.0.notify.notify_waiters();
        }
    }
}

#[derive(Clone)]
pub(crate) struct Shutdown(Arc<ShutdownInner>);

impl Shutdown {
    fn resolved_deadline(&self) -> Instant {
        self.0.deadline.get().copied().unwrap_or_else(Instant::now)
    }
}

pub(crate) fn channel() -> (ShutdownTx, Shutdown) {
    let inner = Arc::new(ShutdownInner {
        closed: AtomicBool::new(false),
        deadline: OnceLock::new(),
        notify: Notify::new(),
    });
    (ShutdownTx(Arc::clone(&inner)), Shutdown(inner))
}

/// Wait until shutdown is triggered and return the drain deadline.
///
/// Registers the task's [`Waker`] in [`Notify`] at most once per waker so
/// steady-state polls from `biased` `select!` loops (`Lifecycle::run`,
/// `bridge_ws`) cost a single atomic load and pointer check instead of
/// locking `notify.waiters` on every socket wake.
pub(crate) async fn deadline(shutdown: &Shutdown) -> Instant {
    if shutdown.0.closed.load(Ordering::SeqCst) {
        return shutdown.resolved_deadline();
    }
    let notified = shutdown.0.notify.notified();
    tokio::pin!(notified);
    let mut registered_waker: Option<Waker> = None;
    std::future::poll_fn(|cx| {
        if shutdown.0.closed.load(Ordering::SeqCst) {
            return Poll::Ready(shutdown.resolved_deadline());
        }
        if registered_waker
            .as_ref()
            .is_some_and(|w| w.will_wake(cx.waker()))
        {
            return Poll::Pending;
        }
        if notified.as_mut().poll(cx).is_ready() || shutdown.0.closed.load(Ordering::SeqCst) {
            return Poll::Ready(shutdown.resolved_deadline());
        }
        registered_waker = Some(cx.waker().clone());
        Poll::Pending
    })
    .await
}

#[derive(Clone)]
pub(crate) struct Lifecycle {
    shutdown: Shutdown,
    completed: mpsc::Sender<()>,
}

impl Lifecycle {
    pub(crate) fn new(shutdown: Shutdown, completed: mpsc::Sender<()>) -> Self {
        Self {
            shutdown,
            completed,
        }
    }

    pub(crate) fn shutdown(&self) -> Shutdown {
        self.shutdown.clone()
    }

    pub(crate) async fn deadline(&self) -> Instant {
        deadline(&self.shutdown).await
    }

    pub(crate) async fn run<F: Future>(self, task: F) -> Option<F::Output> {
        let Self {
            shutdown,
            completed: _completed,
        } = self;
        // Drop the task (and its sockets) before releasing the completion guard.
        tokio::pin!(task);
        tokio::select! {
            biased;
            end = deadline(&shutdown) => timeout_at(end, task).await.ok(),
            value = &mut task => Some(value),
        }
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
    use std::sync::atomic::AtomicUsize;
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
            let (shutdown, receiver) = channel();
            if let Some(at) = closing {
                shutdown.send(at);
            } else {
                drop(shutdown);
            }
            let (completed, mut drained) = mpsc::channel(1);
            let dropped = Arc::new(AtomicBool::new(false));
            let guard = Dropped(dropped.clone());
            let task = Lifecycle::new(receiver, completed).run(async move {
                let _guard = guard;
                std::future::pending::<()>().await;
            });
            let handle = tokio::spawn(task);
            assert!(
                tokio::time::timeout(Duration::from_secs(1), drained.recv())
                    .await
                    .expect("late or closed shutdown must cancel the task")
                    .is_none()
            );
            assert!(
                dropped.load(Ordering::Acquire),
                "completion must follow resource cleanup"
            );
            assert!(handle.await.unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn deadline_caches_waker_re_registers_on_migration_and_wakes_on_send() {
        struct CountWake(AtomicUsize);
        impl Wake for CountWake {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let (tx, rx) = channel();
        let fut = deadline(&rx);
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
