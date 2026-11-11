# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- next-header -->
## [Unreleased] - ReleaseDate

### Changed

- **BREAKING**: Rename the project, crate, binary, and observability surface from `ethryx` to **`ethonyx`**:
  - Binary & library crate: `ethryx` → `ethonyx` (repository: `https://github.com/DELIGHT-LABS/ethonyx`)
  - Environment variable prefix: `ETHRYX_*` → `ETHONYX_*` (including `ETHONYX_STRESS_SECONDS` and build-time `ETHONYX_GIT_DESCRIBE`)
  - Tracing log filter target (`RUST_LOG`): `ethryx` (`ethryx::*`) → `ethonyx` (`ethonyx::*`)
  - Prometheus metrics prefix: `ethryx_*` → `ethonyx_*`
  - OpenTelemetry default service name, tracer scope, and meter scope: `"ethryx"` → `"ethonyx"`
  - Release archive naming: `ethonyx-<tag>-<target>.{tar.gz,zip}`
- Upgrade the Rust toolchain and MSRV to `1.99`, bump `cargo-about` to `0.9.3`, replace legacy `cross` release builds with `taiki-e/setup-cross-toolchain-action`, and update direct and transitive Cargo dependencies (`tokio` `1.54.1`, `aws-lc-rs` `1.19.0`, `rustls-webpki` `0.103.16`, `webpki-roots` `1.0.12`, `webpki-root-certs` `1.0.12`, etc.).

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
