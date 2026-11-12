//! One shutdown deadline and completion barrier for connections and their tasks.
use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
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

pub(crate) type Shutdown = watch::Receiver<Option<Instant>>;

pub(crate) async fn deadline(shutdown: &mut Shutdown) -> Instant {
    shutdown
        .wait_for(Option::is_some)
        .await
        .ok()
        .and_then(|value| *value)
        .unwrap_or_else(Instant::now)
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

    pub(crate) async fn deadline(&mut self) -> Instant {
        deadline(&mut self.shutdown).await
    }

    pub(crate) async fn run<F: Future>(self, task: F) -> Option<F::Output> {
        let Self {
            mut shutdown,
            completed: _completed,
        } = self;
        // Drop the task (and its sockets) before releasing the completion guard.
        tokio::pin!(task);
        tokio::select! {
            biased;
            end = deadline(&mut shutdown) => timeout_at(end, task).await.ok(),
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
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    #[tokio::test]
    async fn completion_barrier_waits_for_cancelled_future_cleanup() {
        struct Dropped(Arc<AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        for closing in [Some(Instant::now()), None] {
            let (shutdown, receiver) = watch::channel(closing);
            if closing.is_none() {
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
                tokio::time::timeout(std::time::Duration::from_secs(1), drained.recv())
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
}
