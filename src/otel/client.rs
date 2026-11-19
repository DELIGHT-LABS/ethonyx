//! OTLP HTTP client backed by ethonyx's shared `hyper-util` + `webpki-roots` stack.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use hyper::body::Body as _;
use opentelemetry_http::{HttpClient, HttpError, ResponseBodyTooLarge};
use tokio::sync::watch;
use tracing::instrument::WithSubscriber as _;

const OTLP_DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
/// Upper bound (1 hour) so extreme `OTEL_EXPORTER_OTLP_*_TIMEOUT` values cannot
/// overflow `Instant::now() + timeout` on any platform.
const OTLP_MAX_TIMEOUT: Duration = Duration::from_secs(3600);
const OTLP_MAX_RESPONSE_BODY_BYTES: usize = 4 * 1024 * 1024;

fn parse_timeout_ms(raw: Option<&str>) -> Option<Duration> {
    raw.map(str::trim)
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&ms| ms > 0)
        .map(|ms| Duration::from_millis(ms).min(OTLP_MAX_TIMEOUT))
}

fn resolve_timeout_from(signal: Option<&str>, global: Option<&str>) -> Duration {
    parse_timeout_ms(signal)
        .or_else(|| parse_timeout_ms(global))
        .unwrap_or(OTLP_DEFAULT_TIMEOUT)
}

fn resolve_timeout(signal_env: &str) -> Duration {
    resolve_timeout_from(
        std::env::var(signal_env).ok().as_deref(),
        std::env::var("OTEL_EXPORTER_OTLP_TIMEOUT").ok().as_deref(),
    )
}

struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// OTLP HTTP client backed by ethonyx's shared `hyper-util` + `webpki-roots` stack,
/// avoiding `reqwest`, `rustls-platform-verifier`, and duplicate hidden Tokio runtimes.
pub(super) struct OtlpHyperClient {
    client: crate::transport::ProxyClient,
    rt: tokio::runtime::Handle,
    timeout: Duration,
    shutdown: watch::Receiver<Option<tokio::time::Instant>>,
}

impl OtlpHyperClient {
    pub(super) const fn timeout(&self) -> Duration {
        self.timeout
    }
}

impl fmt::Debug for OtlpHyperClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OtlpHyperClient")
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

/// Build per-signal trace and metric OTLP clients sharing one pooled HTTPS client
/// and one shutdown deadline broadcast channel.
pub(super) fn build_signal_clients(
    rt: tokio::runtime::Handle,
    shutdown: watch::Receiver<Option<tokio::time::Instant>>,
) -> (OtlpHyperClient, OtlpHyperClient) {
    let client = crate::transport::build_otlp_client();
    let trace_client = OtlpHyperClient {
        client: client.clone(),
        rt: rt.clone(),
        timeout: resolve_timeout("OTEL_EXPORTER_OTLP_TRACES_TIMEOUT"),
        shutdown: shutdown.clone(),
    };
    let metric_client = OtlpHyperClient {
        client,
        rt,
        timeout: resolve_timeout("OTEL_EXPORTER_OTLP_METRICS_TIMEOUT"),
        shutdown,
    };
    (trace_client, metric_client)
}

impl HttpClient for OtlpHyperClient {
    fn send_bytes<'life0, 'async_trait>(
        &'life0 self,
        request: opentelemetry_http::Request<Bytes>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<opentelemetry_http::Response<Bytes>, HttpError>>
                + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let client = self.client.clone();
            let timeout = self.timeout;
            let mut shutdown = self.shutdown.clone();
            // Detach the spawned export task from any global `tracing` subscriber (pairing
            // with `NoSubscriberExecutor` on the client) so `hyper`/`h2` export spans
            // cannot recursively feed back into the OpenTelemetry trace exporter.
            let mut handle = AbortOnDrop(self.rt.spawn(
                async move {
                    let now = tokio::time::Instant::now();
                    let mut deadline = now + timeout;
                    if let Some(shutdown_deadline) = *shutdown.borrow_and_update() {
                        deadline = deadline.min(shutdown_deadline);
                    }
                    // Refuse new dials once the shutdown deadline has passed so SDK retry
                    // backoff never opens fresh TCP connections during teardown.
                    if now >= deadline {
                        return Err("OTLP HTTP export timed out".into());
                    }

                    let req = request.map(crate::response::full_body);
                    let mut send = std::pin::pin!(async move {
                        let resp = client
                            .request(req)
                            .await
                            .map_err(|e| -> HttpError { Box::new(e) })?;
                        let (parts, mut body) = resp.into_parts();
                        if body.size_hint().lower() > OTLP_MAX_RESPONSE_BODY_BYTES as u64 {
                            return Err(Box::new(ResponseBodyTooLarge::new()) as HttpError);
                        }
                        let mut collected = BytesMut::new();
                        while let Some(frame) =
                            std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await
                        {
                            let frame = frame.map_err(|e| -> HttpError { Box::new(e) })?;
                            if let Ok(data) = frame.into_data() {
                                if collected.len().saturating_add(data.len())
                                    > OTLP_MAX_RESPONSE_BODY_BYTES
                                {
                                    return Err(Box::new(ResponseBodyTooLarge::new()) as HttpError);
                                }
                                collected.extend_from_slice(&data);
                            }
                        }
                        Ok(http::Response::from_parts(parts, collected.freeze()))
                    });

                    let sleep = tokio::time::sleep_until(deadline);
                    tokio::pin!(sleep);
                    let mut shutdown_closed = false;
                    loop {
                        tokio::select! {
                            biased;
                            res = &mut send => {
                                return res.map(|mut resp| {
                                    // Strip `Retry-After` once shutdown has started so
                                    // `opentelemetry-otlp`'s synchronous retry loop cannot
                                    // `std::thread::sleep` up to 30s on a 429/503 throttle
                                    // header during graceful teardown.
                                    if shutdown.borrow_and_update().is_some() {
                                        resp.headers_mut().remove(http::header::RETRY_AFTER);
                                    }
                                    resp
                                });
                            }
                            () = &mut sleep => return Err("OTLP HTTP export timed out".into()),
                            res = shutdown.changed(), if !shutdown_closed => match res {
                                Ok(()) => {
                                    if let Some(shutdown_deadline) = *shutdown.borrow_and_update()
                                        && shutdown_deadline < deadline
                                    {
                                        deadline = shutdown_deadline;
                                        sleep.as_mut().reset(deadline);
                                    }
                                }
                                Err(_) => shutdown_closed = true,
                            },
                        }
                    }
                }
                .with_subscriber(tracing::dispatcher::Dispatch::none()),
            ));
            (&mut handle.0)
                .await
                .map_err(|e| -> HttpError { Box::new(e) })?
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{Method, Request, Response, StatusCode};
    use http_body_util::{BodyExt, Full};
    use hyper::body::Incoming;
    use hyper::service::service_fn;
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::server::conn::auto;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::AsyncReadExt;

    fn test_client(
        timeout: Duration,
        shutdown: watch::Receiver<Option<tokio::time::Instant>>,
    ) -> OtlpHyperClient {
        OtlpHyperClient {
            client: crate::transport::build_otlp_client(),
            rt: tokio::runtime::Handle::current(),
            timeout,
            shutdown,
        }
    }

    fn post_req(uri: &str, body: &'static [u8]) -> Request<Bytes> {
        Request::builder()
            .method(Method::POST)
            .uri(uri)
            .body(Bytes::from_static(body))
            .unwrap()
    }

    #[test]
    fn parse_timeout_ms_trims_clamps_and_rejects_zero_or_invalid() {
        assert_eq!(
            parse_timeout_ms(Some("2500")),
            Some(Duration::from_millis(2500))
        );
        assert_eq!(
            parse_timeout_ms(Some("  1500 \n")),
            Some(Duration::from_millis(1500))
        );
        assert_eq!(
            parse_timeout_ms(Some(&u64::MAX.to_string())),
            Some(OTLP_MAX_TIMEOUT)
        );
        for bad in [
            None,
            Some(""),
            Some("   "),
            Some("0"),
            Some("-5"),
            Some("abc"),
        ] {
            assert_eq!(parse_timeout_ms(bad), None, "{bad:?}");
        }
        assert_eq!(
            resolve_timeout_from(Some("1500"), Some("3000")),
            Duration::from_millis(1500)
        );
        assert_eq!(
            resolve_timeout_from(Some("invalid"), Some("3000")),
            Duration::from_millis(3000)
        );
        assert_eq!(
            resolve_timeout_from(None, Some("3000")),
            Duration::from_millis(3000)
        );
        assert_eq!(
            resolve_timeout_from(Some("0"), Some("bad")),
            OTLP_DEFAULT_TIMEOUT
        );
        assert_eq!(resolve_timeout_from(None, None), OTLP_DEFAULT_TIMEOUT);
    }

    #[tokio::test]
    async fn otlp_hyper_client_exports_from_non_tokio_thread_and_enforces_body_limit() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _server = AbortOnDrop(tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let svc = service_fn(|req: Request<Incoming>| async move {
                        let is_huge = req.uri().path() == "/huge";
                        let body = req.into_body().collect().await.unwrap().to_bytes();
                        let reply = if is_huge {
                            Bytes::from(vec![b'x'; OTLP_MAX_RESPONSE_BODY_BYTES + 1])
                        } else {
                            body
                        };
                        Ok::<_, std::convert::Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .body(Full::new(reply))
                                .unwrap(),
                        )
                    });
                    let _ = auto::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(stream), svc)
                        .await;
                });
            }
        }));

        let (_tx, rx) = watch::channel(None);
        let client = test_client(OTLP_DEFAULT_TIMEOUT, rx);
        let ok_uri = format!("http://{addr}/v1/traces");
        let huge_uri = format!("http://{addr}/huge");

        fn block_on_os_thread<F: Future>(fut: F) -> F::Output {
            struct ThreadWaker(std::thread::Thread);
            impl std::task::Wake for ThreadWaker {
                fn wake(self: Arc<Self>) {
                    self.0.unpark();
                }
            }
            let waker = std::task::Waker::from(Arc::new(ThreadWaker(std::thread::current())));
            let mut cx = std::task::Context::from_waker(&waker);
            let mut fut = std::pin::pin!(fut);
            loop {
                match fut.as_mut().poll(&mut cx) {
                    std::task::Poll::Ready(val) => return val,
                    std::task::Poll::Pending => std::thread::park(),
                }
            }
        }

        // Exercise the exact OS-thread `futures_executor::block_on`-style call path used
        // by OTel's `BatchSpanProcessor` / `PeriodicReader` worker threads.
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let worker = std::thread::spawn(move || {
            let ok_resp =
                block_on_os_thread(client.send_bytes(post_req(&ok_uri, b"otlp-payload"))).unwrap();

            let huge_err = block_on_os_thread(client.send_bytes(post_req(&huge_uri, b"")))
                .expect_err("response > 4 MiB must be rejected");
            let _ = done_tx.send((ok_resp, huge_err));
        });
        let (ok_resp, huge_err) = done_rx.await.unwrap();
        worker.join().unwrap();

        assert_eq!(ok_resp.status(), StatusCode::OK);
        assert_eq!(ok_resp.body(), &Bytes::from_static(b"otlp-payload"));
        assert!(
            huge_err.is::<ResponseBodyTooLarge>(),
            "expected ResponseBodyTooLarge, got: {huge_err}"
        );
    }

    #[tokio::test]
    async fn shutdown_deadline_preempts_hung_export_and_blocks_post_deadline_retries() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepts = Arc::new(AtomicUsize::new(0));
        let accepts_srv = Arc::clone(&accepts);
        let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
        let _server = AbortOnDrop(tokio::spawn(async move {
            let mut held = Vec::new();
            let mut notify = Some(accepted_tx);
            while let Ok((stream, _)) = listener.accept().await {
                accepts_srv.fetch_add(1, Ordering::SeqCst);
                if let Some(tx) = notify.take() {
                    let _ = tx.send(());
                }
                held.push(stream);
            }
        }));

        let (shutdown_tx, shutdown_rx) = watch::channel(None);
        let client = test_client(Duration::from_secs(60), shutdown_rx);
        let uri = format!("http://{addr}/v1/metrics");
        let req = post_req(&uri, b"metrics");

        let export = tokio::spawn(async move { client.send_bytes(req).await });
        tokio::time::timeout(Duration::from_secs(2), accepted_rx)
            .await
            .expect("server must accept OTLP connection")
            .unwrap();

        let started = tokio::time::Instant::now();
        let _ = shutdown_tx.send(Some(started + Duration::from_millis(40)));
        let err = tokio::time::timeout(Duration::from_secs(2), export)
            .await
            .expect("shutdown deadline must preempt 60s OTLP timeout")
            .unwrap()
            .expect_err("preempted export must fail with timeout");
        assert!(err.to_string().contains("timed out"), "unexpected: {err}");

        // A retry attempted after the shutdown deadline must fail immediately without dialing.
        let retry_client = test_client(Duration::from_secs(60), shutdown_tx.subscribe());
        let retry_err = retry_client
            .send_bytes(post_req(&uri, b"retry"))
            .await
            .expect_err("post-deadline retry must be rejected upfront");
        assert!(retry_err.to_string().contains("timed out"));
        assert_eq!(accepts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn dropping_send_bytes_future_aborts_spawned_request_task() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (connected_tx, connected_rx) = tokio::sync::oneshot::channel();
        let (eof_tx, eof_rx) = tokio::sync::oneshot::channel();
        let _server = AbortOnDrop(tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 256];
            let _ = stream.read(&mut buf).await;
            let _ = connected_tx.send(());
            // Wait until the client closes the connection upon aborting the spawned task.
            while let Ok(n) = stream.read(&mut buf).await {
                if n == 0 {
                    let _ = eof_tx.send(());
                    break;
                }
            }
        }));

        let (_tx, rx) = watch::channel(None);
        let client = test_client(Duration::from_secs(60), rx);
        let uri = format!("http://{addr}/v1/traces");
        let mut fut = client.send_bytes(post_req(&uri, b"trace"));
        tokio::select! {
            _ = &mut fut => panic!("blackhole server never replies"),
            _ = connected_rx => {}
        }
        drop(fut);

        tokio::time::timeout(Duration::from_secs(2), eof_rx)
            .await
            .expect("dropping send_bytes future must abort spawned task and close socket")
            .unwrap();
    }

    #[tokio::test]
    async fn otlp_hyper_client_and_executor_emit_no_tracing_spans() {
        use hyper::rt::Executor as _;
        use tracing_subscriber::prelude::*;

        struct SpanCounter(Arc<AtomicUsize>);
        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for SpanCounter {
            fn on_new_span(
                &self,
                _attrs: &tracing::span::Attributes<'_>,
                _id: &tracing::span::Id,
                _ctx: tracing_subscriber::layer::Context<'_, S>,
            ) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _server = AbortOnDrop(tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let svc = service_fn(|_req: Request<Incoming>| async move {
                Ok::<_, std::convert::Infallible>(
                    Response::builder()
                        .status(StatusCode::OK)
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
            });
            let _ = auto::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(stream), svc)
                .await;
        }));

        let spans = Arc::new(AtomicUsize::new(0));
        let subscriber = tracing_subscriber::registry().with(SpanCounter(Arc::clone(&spans)));
        let uri = format!("http://{addr}/v1/traces");

        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let worker = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let status = tracing::subscriber::with_default(subscriber, || {
                tracing::callsite::rebuild_interest_cache();
                rt.block_on(async {
                    let _control = tracing::trace_span!("control").entered();
                    assert_eq!(spans.load(Ordering::SeqCst), 1);

                    let (_tx, rx) = watch::channel(None);
                    let client = test_client(OTLP_DEFAULT_TIMEOUT, rx);
                    let resp = client.send_bytes(post_req(&uri, b"trace")).await.unwrap();

                    // Exercise `NoSubscriberExecutor` directly with an `h2`-style
                    // `parent: None` span to confirm background driver tasks stay silent.
                    let (exec_tx, exec_rx) = tokio::sync::oneshot::channel();
                    crate::transport::NoSubscriberExecutor.execute(async move {
                        let _driver = tracing::debug_span!(parent: None, "Connection").entered();
                        let _ = exec_tx.send(());
                    });
                    exec_rx.await.unwrap();
                    resp.status()
                })
            });
            let _ = done_tx.send((status, spans.load(Ordering::SeqCst)));
        });

        let (status, count) = done_rx.await.unwrap();
        worker.join().unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            count, 1,
            "OTLP export and NoSubscriberExecutor must emit zero spans beyond the control span"
        );
    }

    #[tokio::test]
    async fn shutdown_strips_retry_after_header_from_responses() {
        let arrived = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let arrived_srv = Arc::clone(&arrived);
        let release_srv = Arc::clone(&release);

        let (addr, _server) = crate::test_util::spawn_http_mock(move |req| {
            let arrived = Arc::clone(&arrived_srv);
            let release = Arc::clone(&release_srv);
            async move {
                let slow = req.uri().path() == "/slow";
                let _ = req.into_body().collect().await;
                if slow {
                    arrived.notify_one();
                    release.notified().await;
                }
                Response::builder()
                    .status(StatusCode::TOO_MANY_REQUESTS)
                    .header(http::header::RETRY_AFTER, "30")
                    .body(crate::response::full_body(Bytes::new()))
                    .unwrap()
            }
        })
        .await;

        let (shutdown_tx, shutdown_rx) = watch::channel(None);
        let client = test_client(OTLP_DEFAULT_TIMEOUT, shutdown_rx);
        let fast_uri = format!("http://{addr}/v1/metrics");
        let slow_uri = format!("http://{addr}/slow");

        // 1. Before shutdown, collector `Retry-After` is preserved verbatim.
        let resp = client
            .send_bytes(post_req(&fast_uri, b"metrics"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            resp.headers()
                .get(http::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok()),
            Some("30")
        );

        // 2. When shutdown activates while an export is already in flight,
        // `Retry-After` is stripped before returning to `opentelemetry-otlp`.
        let inflight = tokio::spawn({
            let client = test_client(OTLP_DEFAULT_TIMEOUT, shutdown_tx.subscribe());
            let req = post_req(&slow_uri, b"inflight");
            async move { client.send_bytes(req).await }
        });
        arrived.notified().await;
        let _ = shutdown_tx.send(Some(tokio::time::Instant::now() + Duration::from_secs(5)));
        release.notify_one();
        let inflight_resp = inflight.await.unwrap().unwrap();
        assert_eq!(inflight_resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(
            inflight_resp
                .headers()
                .get(http::header::RETRY_AFTER)
                .is_none(),
            "in-flight response must drop Retry-After once shutdown starts"
        );

        // 3. Subsequent exports started during shutdown also strip `Retry-After`.
        let after_resp = client
            .send_bytes(post_req(&fast_uri, b"after"))
            .await
            .unwrap();
        assert_eq!(after_resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(
            after_resp
                .headers()
                .get(http::header::RETRY_AFTER)
                .is_none(),
            "post-shutdown response must drop Retry-After"
        );
    }
}
