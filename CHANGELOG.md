# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- next-header -->
## [Unreleased] - ReleaseDate

### Added

- Add multi-endpoint Reference EL block-lag readiness gating (`--el-reference-url` / `ETHONYX_EL_REFERENCE_URL`, `--el-max-reference-lag` default `1`, `--el-reference-poll-interval` default `3s`) with shared ConfigMap self-URL stripping (`--el-http-url` matches excluded at startup), per-round `eth_chainId` verification and automatic recovery, causal `eth_blockNumber` pairing, unified two-strike fail-open (`Active` → `Retained` → `Unavailable`), `/healthz` `reference` snapshot, and `ethonyx_reference_{health_status,block_number,block_lag}` metrics while keeping data-plane queries unobstructed.
- Add `--sse-idle-timeout` (`ETHONYX_SSE_IDLE_TIMEOUT`, default `900` seconds, `0` = disabled) to keep long-lived Beacon SSE (`text/event-stream`) subscriptions open across quiet intervals while bounding stalled normal HTTP response bodies by `--proxy-timeout`.
- Add `--max-connections` (`ETHONYX_MAX_CONNECTIONS`, default `4096`, `0` = unlimited) shared semaphore backpressure across all `--listen` ports, HTTP/1.1, HTTP/2, HTTP/1.1 upgraded WebSockets, and concurrent HTTP/2 Extended `CONNECT` WebSocket streams (with `ethonyx_connection_limit_saturated_total`), plus a `15s` initial protocol-preface read timeout and `65s` HTTP/1.1 header/keep-alive idle timeout.
- Add `mode` (`"probed"` | `"trusted"`) to `/healthz` (`HealthSnapshot`), reporting honest unpolled state under `--trust-upstream` without fabricated peer counts or gauges.
- Propagate W3C `traceparent` context on upstream WebSocket handshakes when `otel` is enabled.
- Add optional `--admin-listen` (`ETHONYX_ADMIN_LISTEN`) dedicated listener and `Bound::admin_addr()` to serve `/livez`, `/readyz`, `/healthz`, and `/metrics` over an isolated 64-connection pool immune to `--max-connections` saturation — with tight admin limits (`5s` preface/header timeout, HTTP/1.1 keep-alive disabled, `10s` max connection lifetime with graceful `GOAWAY` at `9s`, and `1s` shutdown drain cap), returning `404 Not Found` for all valid requests (including WebSocket upgrades) to those four probe paths on `--listen` when set (`400` for non-WebSocket `CONNECT` or non-origin targets), `405` with `Allow: GET, HEAD` for non-`GET`/`HEAD` probe requests on `--admin-listen`, validating canonical IPv6, IPv4-mapped IPv6, link-local `%zone`, cross-plane `[::]`, and pre-bind/post-bind resolved socket overlaps at startup, and failing fast on `AddrInUse` / `PermissionDenied` during multi-address hostname binds to prevent dual-stack split-brain.
- Add `ethonyx_response_body_errors_total` (`upstream`, `reason="idle_timeout"|"upstream_error"`) across Prometheus and OpenTelemetry (plus `warn` logging) to surface mid-stream upstream response body stalls and transport errors occurring after headers arrive, including unbounded `--sse-idle-timeout 0` streams while excluding client-side disconnects while reading the response.
- Add `Prepared::bind()` and `Bound` (`local_addrs()`, `serve()`) library lifecycle entry points for race-free ephemeral listener binding (`127.0.0.1:0`), log resolved kernel socket addresses on startup, and isolate lock-free atomic Prometheus and OpenTelemetry metric registries per server instance without process-global `opentelemetry::global` or `metrics` state (removing `metrics` and `metrics-exporter-prometheus` along with the 5-second upkeep loop and rendering `/metrics` in deterministic family/series order without blank separator lines).

### Changed

- **BREAKING**: Remove implicit upstream URL defaults across `--el-http-url` (`ETHONYX_EL_HTTP_URL`, now required), `--el-ws-url` (`ETHONYX_EL_WS_URL`, now optional — omitting it disables WebSocket forwarding and returns `404 Not Found` on WS upgrades), and `--cl-beacon-url` (`ETHONYX_CL_BEACON_URL`, now optional — omitting it runs `ethonyx` in **EL-only mode**, returning `404 Not Found` on CL routes and omitting CL fields/metrics) while retaining the `0.0.0.0:8547` default on `--listen`; reject CL-only flags without `--cl-beacon-url`, reference-only flags without `--el-reference-url`, and `--readyz-strict` / `--el-reference-url` alongside `--trust-upstream` at startup, and include the latest block height in `/readyz` `el_syncing` detail (`"synced (block N)"`).
- Gate `/readyz` and mocked `/eth/v1/node/health` on Consensus Layer `is_optimistic` and `el_offline` Engine API flags (preserving `206 Partial Content` while actively syncing, including before `latest` is populated under `--readyz-strict`), require a readable `latest` EL block when `eth_syncing` reports `false`, and keep unsupported `net_peerCount` (`-32601`) omitted without probe errors or `ethonyx_upstream_peers{layer="el"}` zero-pinning across transient outages.
- **BREAKING**: Rename the project, crate, binary, and observability surface from `ethryx` to **`ethonyx`**:
  - Binary & library crate: `ethryx` → `ethonyx` (repository: `https://github.com/DELIGHT-LABS/ethonyx`)
  - Environment variable prefix: `ETHRYX_*` → `ETHONYX_*` (including `ETHONYX_STRESS_SECONDS` and build-time `ETHONYX_GIT_DESCRIBE`)
  - Tracing log filter target (`RUST_LOG`): `ethryx` (`ethryx::*`) → `ethonyx` (`ethonyx::*`)
  - Prometheus metrics prefix: `ethryx_*` → `ethonyx_*`
  - OpenTelemetry default service name, tracer scope, and meter scope: `"ethryx"` → `"ethonyx"`
  - Release archive naming: `ethonyx-<tag>-<target>.{tar.gz,zip}`
- **BREAKING**: Export `ethonyx_proxy_request_duration_seconds` as an explicit-bucket Histogram (`[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]` seconds; unit `"s"` in OpenTelemetry) across both Prometheus and OpenTelemetry instead of a Prometheus summary (replacing `quantile` series with `_bucket`/`_sum`/`_count` so `histogram_quantile()` aggregates accurately across replicas), align tracked HTTP status buckets (tracking common 1xx–5xx codes individually and consolidating unlisted codes under `"OTHER"`), export `ethonyx_active_connections` as an `i64` OpenTelemetry `UpDownCounter`, enable `opentelemetry-otlp/internal-logs` (capping `opentelemetry-otlp` at `info` and suppressing `*.PartialSuccess` warnings under both `--log-level` and `RUST_LOG`, and discarding non-2xx collector error bodies so raw OTLP URLs and collector error bodies are not logged), and replace `reqwest` / `rustls-platform-verifier` inside the `otel` OTLP HTTP exporter with a `hyper-util` + `webpki-roots` client detached from ambient `tracing` subscribers (eliminating duplicate TLS roots, hidden Tokio runtimes, and self-instrumentation loops, and making HTTPS OTLP export work in `FROM scratch` containers; note that OTLP HTTPS trusts bundled WebPKI roots only — no OS/private CA store — and does not honor `HTTP_PROXY`/`HTTPS_PROXY`).
- Upgrade the Rust toolchain and MSRV to `1.99`, bump `cargo-about` to `0.9.3`, replace legacy `cross` release builds with `taiki-e/setup-cross-toolchain-action`, and update direct and transitive Cargo dependencies (`tokio` `1.54.1`, `aws-lc-rs` `1.19.0`, `rustls-webpki` `0.103.16`, `webpki-roots` `1.0.12`, etc.).
- Unify the CL/EL HTTP proxy pipeline, extract inbound connection acceptance (`src/server.rs`), shared response helpers (`src/response.rs`), and upstream transport/TLS client management (`src/transport.rs`), modularize health (`src/health/`) and WebSocket handshake/relay (`src/websocket/`) subsystems, replace the 2-task split/mutex WebSocket relay with a lock-free single-task full-duplex `poll_fn` state machine, rewrite upstream URIs via zero-copy `Uri::from_parts` and single-allocation `BytesMut` path composition, batch Reference EL `eth_chainId` + `eth_blockNumber` into a single JSON-RPC batch per round (reference endpoints must support JSON-RPC batch requests), and simplify `H2cState` to a 3-state machine (`Unprobed` → `H2c` / `H1`) with single-poll settling (once settled on HTTP/1.1, stays HTTP/1.1 until restart) and stale-socket retry.
- Fail fast at startup when `--otel-endpoint` / `ETHONYX_OTEL_ENDPOINT` is configured on a default (non-`otel`) build, reject duplicate, wildcard-overlapping, non-link-local `%zone`, and resolved-hostname-overlapping `--listen` addresses (failing fast on `AddrInUse` / `PermissionDenied` during multi-address hostname binds instead of falling back across address families), reject signed `+` prefixes in Beacon decimal health fields, and allow `--network custom` without slot-clock flags when slot-age tracking is disabled (`--trust-upstream` or `--cl-genesis-time 0`).
- Bound EL batch decode errors (returning `expected JSON-RPC batch array` for non-array bodies without echoing oversized IDs) and guard build-time `git describe` and source rerun hints on local `.git` presence.
- Initiate an RFC 6455 `Close(1001 Going Away)` closing handshake on active HTTP/1.1 and HTTP/2 Extended `CONNECT` WebSocket relays before `--shutdown-grace` expiry (draining echoed `Close` frames and closing transport write halves cleanly without `TCP RST`), cache per-task shutdown wakers on a single `OnceLock<Instant>` + `Notify` channel without steady-state waiter mutex locking, and cap timeout flags to prevent `Instant` addition overflow.
- Redact credentials, non-root API-key path segments, queries, and fragments from `Config` `Debug` output, startup logs, and validation errors, reject `userinfo@` credentials and malformed/out-of-range ports in upstream and OTel URLs at startup, and return a fixed `"bad gateway"` body on `502` proxy errors (logged at `warn`).

### Removed

- Remove the 256-byte `rpc.rs` JSON-RPC prefilter and its obsolete `rpc::tests::prefilter_seeded_stress_matches_full_parser` differential stress step from CI/README (`serde_json` now validates directly on single-pass trimmed JSON slices).

### Fixed

- Return `503 Service Unavailable` (instead of `206 Partial Content`) from mocked `/eth/v1/node/health` when either upstream layer is unreachable or when `--readyz-strict` detects stale blocks/slots; return `206` (or the Beacon API `?syncing_status=<code>` override in `200..=599`, `400` on invalid) only when both layers are reachable and actively syncing.
- Preserve valid upstream `Content-Length` headers and body `size_hint` on proxied HTTP/1.1 responses instead of unconditionally degrading to `Transfer-Encoding: chunked`, normalize leading-zero `Content-Length` values, restore `Content-Length: 0` on empty `POST`/`PUT`/`PATCH` requests forwarded over HTTP/1.1, strip `Content-Length` and collapse bodies on `204`/`304` responses (preventing HTTP/2 `Content-Length` re-injection and forbidden `DATA` frames), keep `Content-Length: 0` on valid `205` responses while draining empty chunked `205` bodies and rejecting non-empty `205` with `502`, strip response bodies on all HTTP/2 `HEAD` responses while keeping headers intact (and preserving HTTP/1.1 `HEAD` `Content-Length`), reject empty, unsupported, duplicate-`chunked`, or trailing-empty `Transfer-Encoding` codings (`501` on requests, `502` on upstream responses except bodyless `304` and non-`204`/`205` `HEAD` metadata) and unsolicited `1xx` upstream responses (`502`), stream `POST` bodies larger than 64 KiB straight through under `--mock-eth-syncing` via a zero-copy prefix body instead of buffering up to 64 MiB, skip local mocks on non-`100-continue` `Expect` headers, strip companion `tracestate` whenever `Connection` nominates `traceparent`, sanitize response headers before SSE idle-timeout classification, match comma-separated WebSocket `Upgrade`/`Connection` tokens at the byte level consistently with hop-by-hop `Connection` nomination (so non-ASCII `obs-text` in sibling tokens does not mask valid upgrade tokens), return `414 URI Too Long` when scoped URI rewriting overflows `http::Uri` limits, and reject non-origin-form (`OPTIONS *`) or non-WebSocket `CONNECT` targets with `400 Bad Request` before proxy metrics/spans.
- Omit the spurious `content-type: text/plain; charset=utf-8` header from mocked `eth_syncing` JSON-RPC notification `204 No Content` responses, reject oversized `Content-Length` requests (> 64 MiB) on mocked `--mock-cl-subscriptions` endpoints with `413 Payload Too Large` upfront before draining or sending `100 Continue`, strip `Expect: 100-continue` headers before forwarding upstream (handled by Hyper on HTTP/1.1, while preserving unknown `Expect` values per RFC 9110 §10.1.1), reject origin-form `CONNECT` requests carrying HTTP/1.1 `Upgrade: websocket` headers and HTTP/1.1 WebSocket upgrades carrying a request body (`!is_end_stream()`, including empty chunked bodies) with `400 Bad Request`, return `Sec-WebSocket-Version: 13` on WebSocket version-mismatch `400` responses, normalize proxied upstream response versions and extensions (preventing HTTP/1.0 upstream responses from closing downstream HTTP/1.1 keep-alive connections and keeping Hyper's HTTP/1.1 fast path on HTTP/2 upstreams), and emit HTTP `502` upstream proxy error warnings inside the active OpenTelemetry `request` span.
- Drain request bodies frame-by-frame without buffering (bounded by 64 MiB and `--proxy-timeout`) on `--mock-cl-subscriptions` endpoints so HTTP/1.1 keep-alive connections remain synchronized across pipelined/sequential requests.
- Fail closed under `--readyz-strict` (and omit `/healthz` age fields) when the system clock precedes the Unix epoch instead of reporting a zero age.
- Retry transient per-connection and signal-interrupted `accept()` errors (`ConnectionAborted`, `ConnectionReset`, `ConnectionRefused`, `Interrupted`) immediately at `debug` level across `--listen` and `--admin-listen` while preserving the `100ms` backoff and `warn` logging for persistent OS / file-descriptor exhaustion failures.
- Prevent full-duplex WebSocket backpressure deadlocks on pipelined bursts, flush `Pong` replies immediately on hop-by-hop `Ping` frames (pausing inbound reads while a `Pong` control flush is stalled, bounding control flush stalls by `min(proxy_timeout, 5s)`, and forwarding upstream `Ping`s only when the client stream is completely idle while absorbing `Pong`s without duplication), fragment reassembled outbound `Text`/`Binary` messages larger than `1 MiB` zero-copy on UTF-8 boundaries so messages up to `64 MiB` stay within peer `16 MiB` frame caps and a `2 MiB` write buffer, chunk transport writes to `64 KiB` so HTTP/2 Extended `CONNECT` streams track slow-reader progress accurately, drop unsent outbound payloads and shut down transport write halves cleanly on abrupt peer disconnects and protocol errors as well as `Close` handshakes, advertise HTTP/2 RFC 8441 `SETTINGS_ENABLE_CONNECT_PROTOCOL` only when `--el-ws-url` is configured, share WebPKI TLS roots across HTTP and `wss://` upstreams without advertising `h2` ALPN on WebSocket handshakes, and enforce `4 KiB`-threshold write-progress stall deadlines so slow-but-progressing peers succeed while zero-progress or Slowloris sinks abort without trickle-frame extension.
- Canonicalize uppercase `WS://` / `WSS://` / `HTTP://` / `HTTPS://` schemes and query-only root URLs at startup (e.g. `wss://host?key=...` → `wss://host/?key=...`) as well as inbound absolute-form query-only request targets forwarded to root upstreams (e.g. `GET http://host?q=1` → `/?q=1`), reject IPv6 link-local zone IDs in upstream URLs, refuse to expose credentials containing `#`/`/`/`?` before `@` or non-numeric `:port` tokens in redacted URL diagnostics, handle L2 `eth_syncing` objects without `currentBlock`/`highestBlock`, surface top-level JSON-RPC error objects returned for batch health probes, truncate upstream JSON-RPC and decode health errors to 512 bytes on UTF-8 boundaries, keep cleartext EL traffic conservatively on HTTP/1.1 until `h2c` is confirmed while retrying confirmed `h2c` once on stale-socket errors within the same timeout budget, separate post-header health probe response-body timeouts (`response body timeout`) from transport errors so stalled bodies do not trigger false `h2c` fallbacks, and record negotiated TLS HTTP/2 (`h2`) versions on `/healthz`.
- Inject OpenTelemetry `traceparent` headers after `strip_hop_by_hop` so client `Connection: traceparent` headers cannot suppress outgoing trace propagation (ignoring duplicate `traceparent` headers, joining multi-line `tracestate` headers, omitting empty/whitespace-only `tracestate` headers, stripping orphaned `tracestate` on fresh root spans per W3C Trace Context, and recording `http.response.status_code` on request spans across both `otel` and non-`otel` builds plus 5xx error status on OTel spans), export request spans immediately without pinning them to pooled upstream connection driver tasks, honor per-signal `OTEL_EXPORTER_OTLP_{TRACES,METRICS}_TIMEOUT` / `OTEL_EXPORTER_OTLP_TIMEOUT` with trimming and upper-bound clamping, enforce a strict 5s shutdown deadline on OTLP provider teardown (stripping `Retry-After` throttle headers during shutdown, bounding in-flight exports on a dedicated shutdown thread, and aborting spawned export tasks on drop), trigger best-effort background OTel provider cleanup on startup/bind errors or dropped futures without stalling error reporting, race startup against shutdown signals (exiting cleanly before binding any listener when shutdown arrives during startup, keeping both Unix and non-Unix processes alive when `ctrl_c` registration fails, and returning an error if the accept loop or health poller exits unexpectedly), and validate `RUST_LOG`, non-zero timeouts, listener addresses (including IPv6 link-local zone IDs), semaphore bounds, schemes, and authorities at startup before initializing telemetry and metrics.
- Honor explicit `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES` (`service.name=...`) overrides instead of overwriting them with the default service name.
- Track Git worktrees, packed/loose refs, tags, and crate sources in `build.rs` without invalidating Cargo's incremental cache or triggering a post-`describe` index rebuild.

## [0.2.5] - 2026-10-19

### Added

- Include project license texts, dependency license notices, and Rust library notices in every release binary archive, with verification before upload.
- Add default/all-feature CI checks on Linux, macOS and Windows.
- Verify actual default/all-feature release archives for all four targets in CI, with regression tests for missing, empty and altered license files.
- Add seeded million-input JSON differential stress and sustained WebSocket churn checks.

### Changed

- Classify each request once for routing and observability, pre-parse upstream URIs at startup and reuse inbound path/query without re-parsing on root-based upstreams, skip body generation on `HEAD` probes, skip the background health poll loop under `--trust-upstream`, and gate per-request tracing spans, `access_log` capture, and context propagation on active subscriber targets or OpenTelemetry. Pass WebSocket drain guards explicitly without request-extension storage.

- Offer Ethryx under `MIT OR Apache-2.0`, preserving the existing MIT license as an option.
- Update the dependency lockfile and OpenTelemetry stack; disable unused Prometheus, metrics utility, futures and telemetry features, and remove the redundant Hyper–Tungstenite adapter.
- Share WebSocket HTTP/1 and HTTP/2 handshake/relay code and HTTP/WebSocket JSON-RPC mock parsing. Reduce WebSocket buffers and eliminate the extra sink mutex on ordinary relays (and `Arc` wrapping on mocked relays) while retaining frame/message limits and protocol validation.
- Deserialize EL hex quantities and Beacon decimal fields directly into typed `u64` data, and assemble mock batch replies in one output buffer without retaining request vectors. Stop batch inspection at the first request requiring upstream handling.
- Skip full mock parsing for UTF-8 inputs up to 256 bytes when both the target method text and JSON escapes are absent, and abort full parsing immediately at non-`eth_syncing` method fields; retain full validation for every possible local reply.

### Fixed

- Separate release build caches by target to prevent native host build scripts from failing against the cross image's older glibc.
- Generate license reports through an explicit UTF-8 file so Windows PowerShell releases do not fail on cargo-about's redirected-output guard.
- Preserve WebSocket subprotocol selection across split header lines, strip hop-by-hop and `Connection`-nominated headers before restoring canonical upstream handshake headers, and apply bounded close-handshake draining to both standard and mocked relays. Decline unsupported extensions and reject unsolicited extension or ambiguous subprotocol responses.
- Separate base path joining from query merging when constructing upstream URIs so query-authenticated base URLs preserve both paths and queries.
- Share one shutdown deadline across HTTP connections, HTTP/2 request tasks and WebSocket relays, including late handshakes. Wait for cancellation cleanup before returning from the library and handle an already completed shutdown future.
- Keep HTTP/2 WebSocket upgrades working on probe-named paths by classifying health probes by both method and path.
- Handle escaped JSON-RPC method names and notifications correctly; mock `eth_syncing` only when `params` is omitted or an empty array (`[]`), preserve explicit null/large IDs, and forward non-empty `params` or mixed batches intact. Require request objects and validate complete local batches before replying.
- Accept split and token-list WebSocket upgrade headers while retaining key/version validation. Carry the classified handshake mode through dispatch so HTTP/1 CONNECT cannot bypass validation; require version 13 for HTTP/2 Extended CONNECT.
- Require object-shaped Beacon envelopes/data, EL response/block objects, and canonical `0x`-prefixed hex quantities so positional arrays or malformed hex strings cannot pass health validation. Parse the fixed three-response EL batch in a single pass without heap-allocating a vector while preserving ID validation and partial results.
- Bound health checks through response-body completion and mock POST collection by size and time.
- Use HTTP/1.1 for trusted cleartext upstreams when health-based h2c detection is disabled, and avoid redundant cleartext h2c probes on `https://` EL upstreams.
- Initialize the actual OTel tracer before the subscriber, use per-signal OTLP paths, and shut down trace and metric providers on Ctrl-C or Unix SIGTERM without blocking Tokio workers.

## [0.2.4] - 2026-07-23

### Added

- Added single consolidated `--mock-cl-subscriptions` (`ETHRYX_MOCK_CL_SUBSCRIPTIONS`) flag to intercept `prepare_beacon_proposer`, `beacon_committee_subscriptions`, and `sync_committee_subscriptions` requests locally with `200 OK`. Replaced the previous 3 individual flags.
- Added `--cl-mev-relay-url` (`ETHRYX_CL_MEV_RELAY_URL`) option to rewrite `POST /eth/v1/validator/register_validator` requests to `/eth/v1/builder/validators` and proxy directly to an upstream MEV Relay.
- Added `--mock-eth-syncing` (`ETHRYX_MOCK_ETH_SYNCING`) and `--mock-cl-node-health` (`ETHRYX_MOCK_CL_NODE_HEALTH`) flags to intercept `eth_syncing` JSON-RPC and `GET /eth/v1/node/health` requests locally, decoupled from `--trust-upstream`.

### Changed

- Optimized hop-by-hop header removal using static `HeaderName` constants to eliminate per-request header name string parsing.

### Fixed

- Preserved upstream URI path components without appending unnecessary trailing slashes (`/`) during root request proxying and WebSocket handshakes, preventing HTTP 400 `Unsupported method: /` errors from commercial RPC providers (Alchemy).

## [0.2.2] - 2026-07-03

## [0.2.1] - 2026-06-16

## [0.2.0] - 2026-06-10

## [0.1.4] - 2026-06-04

### Added

- **Route client-specific consensus API paths to CL upstream**: Added path matching to forward client-specific consensus API paths (e.g. `/lighthouse/...`, `/prysm/...`, `/teku/...`, `/lodestar/...`, `/nimbus/...`) to the Consensus Layer (CL) upstream instead of the Execution Layer.
- **Enriched connection access logs**: Added `upstream` (EL/CL) and `proto` (HTTP/WS) fields to connection access logs (`target: "access_log"`).
- **Startup upstream protocol logging**: Logs configured upstream URLs and their resolved/negotiated protocols (e.g. `HTTP/2 (h2c)` vs `HTTP/1.1`) at `info` level once startup health check completes.

## [0.1.3] - 2026-06-03

### Added

- The boot log line now includes the ethryx `version` (e.g. `v0.1.2`) and a `git`
  field from build-time `git describe`, so a tagged release (`git` = `v0.1.2`) is
  distinguishable from an ad-hoc build off a later commit
  (`v0.1.2-5-g20537f9`, `-dirty` when the tree was modified). No new dependency —
  a small `build.rs` captures it; `git` is `unknown` without a git checkout.
- `--access-log` (`ETHRYX_ACCESS_LOG`): an opt-in per-connection access log on a
  dedicated `access_log` target — peer, the negotiated `HTTP/1.1` vs `HTTP/2`, and
  the first request's method and path. The target sits outside the `ethryx`
  hierarchy, so raising the app log doesn't pull it in. Kept separate from the
  application log so `info` stays quiet by default, and health-probe paths
  (`/livez`, `/readyz`, `/healthz`) are excluded so frequent k8s / LB checks don't
  bury real traffic.
- **WebSocket path/query & header forwarding**: The WebSocket proxy now preserves client paths, query strings, and custom headers (such as `Authorization` tokens) during the handshake to the upstream WS server.
- **HTTP/1.1 fallback on h2c connection preface timeout**: Added automatic fallback to HTTP/1.1 if the execution layer upstream hangs on the cleartext h2c preface instead of closing the connection.
- **Graceful connection draining**: Shutdown logic now tracks active client connections and gracefully drains them up to the `--shutdown-grace` period before exiting.
- **Response body size limit**: Capped execution layer HTTP health poll responses at 10 MB to prevent memory exhaustion from unexpected large payloads.

### Changed

- `info` logging stays reserved for lifecycle and state-change events; per-request
  proxy / WS outcomes and connection errors are `debug`, and connection accept /
  close are `trace`. A healthy sidecar is near-silent at `info` between changes.
- **JSON-RPC batching for EL health queries**: Consolidated `eth_syncing`, `net_peerCount`, and `eth_getBlockByNumber` queries into a single batch request to reduce upstream request overhead.
- **Zero-clone JSON extraction**: Used `Value::take()` and `Vec::remove()` in health poll batch parsing to eliminate deep copies of JSON Value structures.
- **Zero-allocation connection logging**: Replaced `Arc<AtomicBool>` connection logging flag with a stack-captured `AtomicBool` directly inside the connection loop to avoid heap allocations per connection.
- **Header copying optimization**: Replaced loop-based header insertion with direct `HeaderMap::clone()` to utilize optimized vectorized block allocations.
- **Type-safe health check errors**: Replaced string-based error matching in health checking flow control with a structured `HealthError` enum.

## [0.1.2] - 2026-05-31

### Added

- HTTP/2 support: the listener serves HTTP/1.1 and HTTP/2 on the same port,
  auto-detected per connection — including cleartext **h2c** via prior-knowledge,
  which covers the common "TLS-terminating LB / mesh forwards h2c to the backend"
  shape (Envoy, Istio, HAProxy `proto h2`). h1 and h1 WebSocket are unchanged. The
  upstream client now auto-negotiates h2 for `https://` upstreams via ALPN
  (cleartext upstreams stay h1). No new flags; plaintext stays the default (TLS
  termination remains the LB/mesh's job).
- HTTP/2 WebSocket via RFC 8441 Extended CONNECT: an h2 client (or h2 mesh) can
  open a WebSocket with `:protocol=websocket`, which ethryx bridges to the upstream
  h1 WebSocket. The HTTP/1.1 `Upgrade` WebSocket path is unchanged.
- EL JSON-RPC upstream h2c auto-detection: the health poller probes the EL hop
  (preferring cleartext **h2c**) and forwards over HTTP/2 when the upstream serves
  it (geth ≥v1.17, erigon, reth), falling back to HTTP/1.1 otherwise. No flag. If a
  running upstream stops serving h2c, the poller falls back within one poll; a
  cleartext upstream that newly adds h2c is picked up on restart. The CL Beacon hop
  stays HTTP/1.1, and `https://` EL upstreams continue to negotiate h2 via ALPN.
- `/healthz` now reports the upstream HTTP `transport` per layer (`h2c` / `h2` /
  `http/1.1`), so the auto-detected EL transport is observable.
- `/readyz` readiness probe and `/healthz` state snapshot, joining `/livez` as a
  three-tier probe model (liveness / readiness / monitoring), following the
  Kubernetes `livez` / `readyz` / `healthz` convention.
  - `/readyz` is the load-balancer traffic gate. It gates on EL + CL **sync
    status** only, so a network-wide stall (or a fleet-wide peer dip) does not
    drain every backend out of rotation at once. `--readyz-strict`
    (`ETHRYX_READYZ_STRICT`) additionally gates on EL block / CL slot freshness.
  - `/healthz` is verdict-free: it always returns `200` and reports each live
    EL/CL value as a machine-readable numeric field (peer counts, block / slot
    age, sync status) under `el` / `cl`, with any upstream failure recorded in a
    per-layer `errors` array — leaving thresholding and alerting to the consumer.
  - Both endpoints serve a snapshot refreshed by a background poller that waits
    `--health-poll-interval` / `ETHRYX_HEALTH_POLL_INTERVAL` (default 5s) between
    polls, so upstream load is constant regardless of probe rate and a probe
    never blocks on upstream. Block / slot ages are recomputed live per request.
    Readiness transitions are logged once by the poller (bounded to the poll
    rate, and visible even if nothing probes `/readyz`), not per probe.
- `--log-level` / `ETHRYX_LOG_LEVEL` (default `info`) to set the log level when
  `RUST_LOG` is unset; `RUST_LOG` still overrides it and allows per-target
  directives.

### Changed

- Logging follows a sidecar discipline: routine upstream / client failures
  (proxy 502s, WebSocket drops) and each health poll are now `debug` (were
  `error`); readiness changes are logged once at `warn` (not-ready) / `info`
  (recovered); listener `accept()` failures stay `warn`; request routing is
  `trace`.

### Removed

- `--el-min-peers` / `--cl-min-peers` (`ETHRYX_EL_MIN_PEERS` /
  `ETHRYX_CL_MIN_PEERS`): peer count no longer gates any endpoint. `/readyz`
  gates on sync status; `/healthz` reports the raw peer count for the monitoring
  layer to threshold. The `--*-max-*-age-secs` flags are retained but now gate
  `/readyz` under `--readyz-strict` rather than a health verdict.

### Fixed

- The accept loop now backs off briefly after a failed `accept()` instead of
  retrying immediately, so a persistent error (e.g. file-descriptor exhaustion)
  no longer busy-spins a core.

## [0.1.1] - 2026-05-28

### Changed

- `/health` top-level `status` value is now `"healthy"` / `"unhealthy"` (was
  `"ok"` / `"unhealthy"`, asymmetric).
- `/health` EL syncing detail parses `currentBlock` / `highestBlock` from
  `eth_syncing` and renders as `"syncing (block X, distance Y)"`, mirroring
  the CL `"syncing (slot S, distance D)"` format.
- `/health` CL check errors now prefix with the Beacon API endpoint path
  (`node/syncing: ...`, `node/peer_count: ...`) to disambiguate against the
  literal `syncing` status word.
- Env vars for the two `*-age-secs` flags gained the missing `_SECS` suffix
  (`ETHRYX_EL_MAX_BLOCK_AGE` → `ETHRYX_EL_MAX_BLOCK_AGE_SECS`,
  `ETHRYX_CL_MAX_SLOT_AGE` → `ETHRYX_CL_MAX_SLOT_AGE_SECS`) so the env name
  matches its CLI flag.

## [0.1.0] - 2026-05-28

### Added

- Initial sidecar implementation: hyper 1 reverse proxy for Ethereum EL JSON-RPC
  (HTTP / WebSocket) and CL Beacon REST API, aggregated `/health` with EL+CL
  sync / peer / freshness checks, multi-port listen, `--network` presets for CL
  genesis (mainnet / hoodi / sepolia / holesky / custom), CLI + env config via
  clap.
