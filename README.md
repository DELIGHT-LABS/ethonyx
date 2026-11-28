# ethonyx

[![CI](https://github.com/DELIGHT-LABS/ethonyx/actions/workflows/ci.yml/badge.svg)](https://github.com/DELIGHT-LABS/ethonyx/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/DELIGHT-LABS/ethonyx?include_prereleases&sort=semver)](https://github.com/DELIGHT-LABS/ethonyx/releases)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/License-MIT%20OR%20Apache--2.0-yellow.svg)](LICENSE)
[![Rust 1.99+](https://img.shields.io/badge/rust-1.99%2B-blue.svg)](rust-toolchain.toml)

Lightweight Rust reverse proxy and health-check sidecar for Ethereum **Execution Layer** (EL)
nodes and optional **Consensus Layer** (CL) Beacon node pairs (supports both `EL + CL` and
`EL-only` deployments such as L2s and Cosmos EVM chains).

## Routing

| Request                              | Forwarded to        |
|--------------------------------------|---------------------|
| `GET/HEAD /livez`                    | 200 OK (process liveness; served exclusively on `--admin-listen` when set, `404` on `--listen`) |
| `GET/HEAD /readyz`                   | Readiness gate (200 / 503; data-plane queries always pass through; moved to `--admin-listen` when set) |
| `GET/HEAD /healthz`                  | State snapshot (always 200; moved to `--admin-listen` when set) |
| `GET/HEAD /metrics`                  | Prometheus metrics scrape endpoint (moved to `--admin-listen` when set) |
| `GET/HEAD /eth/v1/node/health`       | ethonyx node health (`200` / `206` or `?syncing_status` / `400` / `503`) if `--mock-cl-node-health` set, else `--cl-beacon-url` (`404` in EL-only mode; always served on `--listen` as a Beacon API endpoint) |
| `POST /eth/v1/validator/register_validator` | `--cl-mev-relay-url` (rewritten to `/eth/v1/builder/validators`) if set, else `--cl-beacon-url` (`404` in EL-only mode) |
| `POST /eth/v1/validator/{prepare_beacon_proposer, beacon_committee_subscriptions, sync_committee_subscriptions}` | `200 OK` if `--mock-cl-subscriptions` set, else `--cl-beacon-url` (`404` in EL-only mode) |
| `/eth/...`, `/lighthouse/...`, `/prysm/...`, `/teku/...`, `/lodestar/...`, `/nimbus/...` | `--cl-beacon-url` (`404` when `--cl-beacon-url` is omitted) |
| `Upgrade: websocket` / H2 Extended `CONNECT` | `--el-ws-url` (`404` on HTTP/1.1 Upgrade; RFC 8441 Extended `CONNECT` not advertised when omitted) |
| everything else (JSON-RPC `POST /`)  | `--el-http-url` *(required)* |

## Probes

Four endpoints, split by purpose (Kubernetes `z` convention):

| Endpoint            | Returns                       | Wire to                     | Gates on                                              |
|---------------------|-------------------------------|-----------------------------|-------------------------------------------------------|
| `GET/HEAD /livez`   | always `200`, body `ok`       | liveness probe (restart)    | nothing — only that the process is up; no upstream call |
| `GET/HEAD /readyz`  | `200` ready / `503` not ready | readiness probe (LB gate)   | EL sync + readable `latest` block, optional CL sync + Engine API (`!is_optimistic && !el_offline`), optional `--el-reference-url` block lag, plus freshness with `--readyz-strict` |
| `GET/HEAD /healthz` | always `200` + JSON snapshot  | monitoring / alerting / curl | nothing — reports state, never judges                 |
| `GET/HEAD /metrics` | Prometheus metrics text format | Prometheus scraper          | nothing — metrics scraper                             |

`/livez` and `/readyz` differ when the node is up but should not receive load-balanced
traffic (startup, mid-sync, Engine API stall, or lagging behind peers): `/livez` stays
`200` (don't restart me) while `/readyz` returns `503` (route around me). Direct HTTP
JSON-RPC, WebSocket, and REST requests continue to pass through unconditionally even
while `/readyz` is `503`.

### `/readyz` — the traffic gate

Gates on **EL sync + readable `latest` block** (and **CL sync + `!is_optimistic && !el_offline`**
when `--cl-beacon-url` is configured). A caught-up node reports `eth_syncing == false`
even when the chain stalls network-wide, so a chain incident (or a fleet-wide peer dip)
won't drop every backend out of the load balancer at once.

```jsonc
// 200 — synced. `cl_syncing` appears when `--cl-beacon-url` is set;
// `el_reference_lag` appears when `--el-reference-url` is set;
// `el_block_fresh` / `cl_slot_fresh` appear only under `--readyz-strict`.
{ "status": "ready",
  "el_syncing": { "ok": true, "detail": "synced (block 21000000)" },
  "cl_syncing": { "ok": true, "detail": "synced (slot 9412341, distance 0)" } }
```

- **Reference block-lag gate (`--el-reference-url`)**: Compares local EL height against one or more peer Ethonyx/EL URLs or external JSON-RPC endpoints (which must support JSON-RPC batch requests for `eth_chainId` + `eth_blockNumber`) every `--el-reference-poll-interval` (default `3s`), returning `503` when lag exceeds `--el-max-reference-lag` (default `1` block, so `0–1` blocks pass and `≥ 2` blocks deroute). During a network-wide chain halt (even with a 1-block partial propagation split), nodes stay within `max_lag` and remain `200`. Any round that cannot produce a verified `(reference, causal local)` sample (all references fail, all `chain_id`s mismatch, or local `eth_chainId` / `eth_blockNumber` fails) counts as a missed round: the first missed round retains the last verdict, and two consecutive missed rounds fail open (`200`) so a reference outage never drains healthy nodes (monitor `/healthz` `reference.errors` and alert on `ethonyx_reference_health_status == 0`).
- **Wall-clock freshness gate (`--readyz-strict`)**: Additionally gates on EL block age (`--el-max-block-age-secs`) and CL slot age (`--cl-max-slot-age-secs`) — choose it when serving strictly at-head data matters more than fleet availability during a chain halt.

### `/healthz` — state for monitoring

Always `200`. Reports each live EL (and optional CL / Reference) value as a machine-readable
field for your monitoring stack to threshold; it applies no thresholds and renders no verdict.
A signal whose upstream call failed is omitted and the error is recorded under
that layer's `errors` array (`net_peerCount` `-32601 Method not found` is silently omitted
on chains that do not implement the `net` namespace).

| Field                                              | Source                                |
|----------------------------------------------------|---------------------------------------|
| `mode` (`"probed"` / `"trusted"`)                  | whether background polling is active or `--trust-upstream` is set |
| `el.transport` / `cl.transport` (`h2c`/`h2`/`http/1.1`) | upstream HTTP version ethonyx uses |
| `el.syncing` (`false` = synced)                    | EL `eth_syncing`                      |
| `el.sync_distance` (while syncing)                 | EL `eth_syncing` highest − current    |
| `el.peers`                                         | EL `net_peerCount`                    |
| `el.block_number` / `el.block_age_secs`            | EL `eth_getBlockByNumber("latest")`   |
| `cl.syncing` / `cl.is_optimistic` / `cl.el_offline` | Beacon `/eth/v1/node/syncing`         |
| `cl.sync_distance` / `cl.head_slot` / `cl.slot_age_secs` | Beacon `head_slot` vs. wall-clock |
| `cl.peers`                                         | Beacon `/eth/v1/node/peer_count`      |
| `reference.block_number` / `reference.block_lag`   | highest verified `--el-reference-url` `eth_blockNumber` |
| `el.errors` / `cl.errors` / `reference.errors`     | any upstream or reference call that failed |

```json
{ "mode": "probed",
  "el": { "transport": "h2c", "syncing": false, "peers": 23, "block_number": 21000000, "block_age_secs": 5 },
  "cl": { "transport": "http/1.1", "syncing": false, "is_optimistic": false, "el_offline": false, "sync_distance": 0, "peers": 78, "head_slot": 9412341, "slot_age_secs": 3 } }
```

CL slot age is derived from `head_slot * --cl-seconds-per-slot +
--cl-genesis-time`. Use `--network <name>` for a preset (defaults to mainnet)
instead of typing both; `--cl-genesis-time 0` omits `cl.slot_age_secs`.

### Polling

`/readyz` and `/healthz` don't query upstream per request. A background task
polls the EL batch and both CL endpoints concurrently, then waits
`--health-poll-interval` (default 1s; each call
bounded by `--health-timeout`, including the response body — an `h2c` → HTTP/1.1 probe fallback grants the HTTP/1.1 attempt its own timeout) before the next poll, and the endpoints return
the latest snapshot instantly — so upstream load is constant regardless of probe
rate, a slow upstream never blocks a probe, and the poller pauses a full interval
between polls rather than hammering a struggling node. Block / slot ages are
recomputed live per request, so they stay accurate between polls. The cache is warmed by one poll before the listener accepts, so the process serves
a real snapshot from its first probe. When `--el-reference-url` is configured, a
separate background cadence (`--el-reference-poll-interval`, default `3s`) learns
the local `eth_chainId` first, then queries `eth_chainId` and `eth_blockNumber`
in a single 2-element JSON-RPC batch on each distinct reference endpoint, reading the local
`eth_blockNumber` right after any verified reference that raises the round's
highest head (one local read in steady state).

Readiness is logged by the poller on transition (becoming not-ready / recovering)
— once per change, bounded to the poll rate and emitted even if nothing probes
`/readyz`, rather than warning on every probe.

| `--network` | genesis_time   | seconds_per_slot |
|-------------|----------------|------------------|
| `mainnet`   | `1606824023`   | `12`             |
| `hoodi`     | `1742213400`   | `12`             |
| `sepolia`   | `1655733600`   | `12`             |
| `holesky`   | `1695902400`   | `12`             |
| `custom`    | *(required)*   | *(required)*     |

For a **private / custom beacon chain**, pass `--network custom` together with
explicit `--cl-genesis-time <unix>` and `--cl-seconds-per-slot <secs>`. When
CL slot-age tracking is active (polled mode with `--cl-beacon-url` and non-zero
`--cl-genesis-time`), the sidecar refuses to start if either is missing under
`custom`; `--trust-upstream` or `--cl-genesis-time 0` disables slot-age tracking
and does not require them.

## HTTP/2

ethonyx serves HTTP/1.1 and HTTP/2 on the same `--listen` port — the protocol is
auto-detected per connection. Cleartext HTTP/2 (**h2c**, prior-knowledge) is
supported, covering the common "TLS-terminating LB / mesh forwards h2c to the
backend" shape (Envoy, Istio, HAProxy `proto h2`); plain HTTP/1.1 and the
HTTP/1.1 WebSocket upgrade are unchanged.

WebSocket works over both transports when `--el-ws-url` is configured: the HTTP/1.1
`Upgrade` handshake and HTTP/2 Extended CONNECT (RFC 8441, `:protocol=websocket`).
Either is bridged to the upstream's HTTP/1.1 WebSocket (`--el-ws-url`); when
`--el-ws-url` is omitted, HTTP/1.1 WebSocket upgrades return `404 Not Found` and
HTTP/2 does not advertise RFC 8441 `SETTINGS_ENABLE_CONNECT_PROTOCOL` (unnegotiated
HTTP/2 Extended `CONNECT` requests are rejected with `RST_STREAM(PROTOCOL_ERROR)`).

Subprotocol offers from all request header lines are combined, and the selected
protocol is relayed from the upstream. Unsupported extensions, including
compression, are not negotiated; unsolicited extension responses are rejected.
Frames remain limited to 16 MiB and complete messages to 64 MiB, including
fragmented messages. The single-task non-blocking full-duplex `poll_fn` relay reads
in 8 KiB chunks with frame storage that grows for larger frames, uses 16 KiB
write buffers (capped at `2 MiB` total write buffer and `64 KiB` per transport
write) with zero-copy `1 MiB` outbound fragmentation aligned to UTF-8 boundaries,
and maintains independent directional backpressure with no split-sink lock;
client `Ping` frames flush their local `Pong` immediately without forwarding
upstream, upstream `Ping` frames flush their local `Pong` and are additionally
forwarded to completely idle clients (whose `Pong` is absorbed locally without
duplication) so front-side L7 load balancers observe keepalive traffic, and
outbound relay sinks enforce progress-based stall deadlines (`--proxy-timeout`
for data frames, `min(--proxy-timeout, 5s)` when only a control frame flush is pending) that reset once
at least `4 KiB` of new transport bytes drain after backpressure begins (bounded
by kernel TCP / HTTP/2 flow-control send windows) while refusing extensions from
queued trickle frames. Tungstenite continues to validate masking, UTF-8, control
frames, and upstream handshakes.

Inbound connections are bounded by `--max-connections` (`ETHONYX_MAX_CONNECTIONS`,
default `4096`, `0` = unlimited) across all `--listen` addresses (including probe
endpoints unless `--admin-listen` is configured, in which case `--admin-listen`
uses its own isolated 64-connection pool), HTTP/1.1, HTTP/2 (`max_concurrent_streams = 256`,
`keep_alive_interval = 30s`), HTTP/1.1 upgraded WebSockets, and concurrent HTTP/2
Extended `CONNECT` WebSocket streams (`503 Service Unavailable` when saturated).
Newly accepted sockets enforce a 15-second initial protocol-preface read timeout
(evicting silent or partial HTTP/2-preface sockets), followed by a 65-second
HTTP/1.1 header / keep-alive idle timeout (> standard 60-second LB and client pool
idle timeouts). Upstream response bodies are guarded by per-chunk idle
timeouts: `--proxy-timeout` (default `60s`) for standard responses and
`--sse-idle-timeout` (`ETHONYX_SSE_IDLE_TIMEOUT`, default `900s`, `0` = disabled)
for `text/event-stream` Beacon event streams.

Shutdown sets one deadline shared by HTTP connections, HTTP/2 request tasks
and WebSocket relays. Existing work can finish during `--shutdown-grace`; late
handshakes use the time remaining, and active WebSocket relays continue
forwarding frames across the grace window before initiating an RFC 6455
`Close(1001 Going Away)` handshake right ahead of the deadline. At the deadline,
any remaining work is cancelled, and the library waits for its futures and
sockets to be released before returning, including with zero grace or an
already completed shutdown future.

The upstream client auto-negotiates h2 for `https://` upstreams via ALPN. A
cleartext EL JSON-RPC upstream can't be auto-negotiated, so the health poller
probes it: it attempts cleartext **h2c** on the first probe and forwards over
HTTP/2 once confirmed (geth ≥v1.17, erigon, reth), falling back to HTTP/1.1
otherwise — no flag. Until `h2c` is confirmed by the initial warm-up poll, the
data-plane stays conservatively on HTTP/1.1 (avoiding false warnings against
h1-only nodes). A confirmed `h2c` upstream retries once on a non-timeout
transport error within the same probe timeout so stale pooled sockets or rapid
pod restarts do not trigger spurious fallbacks; if `h2c` still fails while
HTTP/1.1 succeeds, it downgrades to HTTP/1.1 within one poll (and once settled on
HTTP/1.1, stays on HTTP/1.1 until restart). The data-plane
follows the verdict and never retries across protocols (to avoid double-sending
a non-idempotent call like `eth_sendRawTransaction`), so while a running
upstream is switching away from h2c the data-plane can briefly return `502`s —
up to one poll interval — until the verdict updates. `https://` CL and EL
upstreams also negotiate h2 via ALPN and report the negotiated transport on
`/healthz`.

ethonyx does **not** terminate TLS — it serves plaintext and leaves TLS to the
LB / service mesh in front.

## Usage

`--el-http-url` and at least one of `--listen` or `--admin-listen` are required;
no listener or upstream URL carries an implicit default, and every optional endpoint
(`--listen`, `--admin-listen`, `--el-ws-url`, `--cl-beacon-url`, `--cl-mev-relay-url`,
`--el-reference-url`, `--otel-endpoint`) is enabled only when explicitly provided:

```sh
ethonyx \
  --listen 0.0.0.0:8547 \
  --admin-listen 0.0.0.0:9090 \
  --el-http-url   http://127.0.0.1:8545 \
  --el-ws-url     ws://127.0.0.1:8546 \
  --cl-beacon-url http://127.0.0.1:5052
```

Every flag also accepts an `ETHONYX_*` env var (see `ethonyx --help`). To disable an
optional endpoint (`ETHONYX_LISTEN`, `ETHONYX_ADMIN_LISTEN`, `ETHONYX_EL_WS_URL`, etc.) via environment
variables, leave the variable unset rather than setting an empty string (`""`), which
is rejected as invalid; if a reference
URL's query string itself contains a comma, pass repeated `--el-reference-url` CLI
flags rather than comma-delimiting.

### Dedicated admin/probe listener (`--admin-listen`) & probe-only mode

Pass `--admin-listen` (`ETHONYX_ADMIN_LISTEN`, e.g. `0.0.0.0:9090`) to serve `/livez`,
`/readyz`, `/healthz`, and `/metrics` on a dedicated TCP listener with its own 64-connection
pool and tight connection limits:
- **Saturation resilience**: Kubelet probes and Prometheus scrapes remain responsive even when client traffic saturates `--max-connections` on `--listen`. To prevent idle clients from wedging the 64-slot admin pool, `--admin-listen` enforces a `5s` initial-preface and HTTP/1.1 header timeout, disables HTTP/1.1 keep-alive (`Connection: close`), caps HTTP/2 concurrency at `32` streams, bounds total connection lifetime to `10s` (sending a graceful HTTP/2 `GOAWAY` at `9s`), and caps admin shutdown drain at `1s` (or immediately under `--shutdown-grace 0`). Because pool saturation queues subsequent accepts until in-flight slots drain (up to `5s` for silent TCP clients or `10s` for slow/stalled HTTP/1.1 and HTTP/2 connections), restrict `--admin-listen` to cluster-internal or network-policy-scoped traffic rather than exposing it publicly.
- **Surface isolation**: When paired with `--listen`, all valid requests (including `GET`, `HEAD`, `POST`, HTTP/1.1 WebSocket upgrades, and HTTP/2 Extended `CONNECT` WebSockets) to the four probe paths return `404 Not Found` on `--listen` (never forwarded to `--el-http-url` or `--el-ws-url`), while non-WebSocket `CONNECT` and non-origin request targets still return `400 Bad Request`. `--admin-listen` serves only those four paths (`405` with `Allow: GET, HEAD` on unsupported methods; `400` on `CONNECT` or non-origin request targets; `404` on all other paths, including `/eth/v1/node/health`, which remains a Beacon API route on `--listen`).
- **Metric hygiene**: Connections on `--admin-listen` do not consume `--max-connections`, increment `ethonyx_active_connections{protocol="tcp"}`, or trigger `ethonyx_connection_limit_saturated_total`.
- **Probe-only mode (`--admin-listen` without `--listen`)**: Omit `--listen` to run `ethonyx` purely as a health-check and observability sidecar with zero data-plane sockets, zero proxy state (retaining only the core upstream probing client), an effective `1s` shutdown drain cap (`--shutdown-grace 0` drains immediately), and (when `--otel-endpoint` is set) metrics-only OTLP export. Data-plane-only flags (`--el-ws-url`, `--cl-mev-relay-url`, `--mock-eth-syncing`, `--mock-cl-subscriptions`, `--mock-cl-node-health`, `--proxy-timeout`, `--sse-idle-timeout`, `--max-connections`, `--access-log`) and `--trust-upstream` (rejected because an unpolled probe-only sidecar would statically return `200`) fail closed at startup when `--listen` is omitted. **Upgrading from `v0.3.0`**: deployments that previously passed `--admin-listen` while relying on the implicit `0.0.0.0:8547` default must add `--listen 0.0.0.0:8547` explicitly to retain proxy mode (otherwise `ethonyx` starts cleanly in probe-only mode without opening `:8547`):

```sh
ethonyx \
  --admin-listen 0.0.0.0:9099 \
  --network mainnet \
  --el-http-url    http://127.0.0.1:8545 \
  --cl-beacon-url  http://127.0.0.1:5052 \
  --el-reference-url http://10.0.1.11:8545
```

- When `--admin-listen` is omitted (with `--listen` set), `--listen` serves probes and data-plane routes together.

### Multi-port listen

`--listen` is repeatable (or comma-separated). All `--listen` ports serve
identical routes — useful when some traffic hits the box via LB on one port
while operators or direct clients (and scrapers when `--admin-listen` is unset)
reach it on another:

```sh
ethonyx \
  --listen 0.0.0.0:8547 \
  --listen 127.0.0.1:9547 \
  ...
# or
ETHONYX_LISTEN=0.0.0.0:8547,127.0.0.1:9547 ethonyx ...
```

All `--listen` listeners share a single fair round-robin accept loop and
`--max-connections` permit pool, so an idle listener never withholds capacity
from an active port (when `--admin-listen` is configured, it runs its own
separate accept loop and 64-connection pool).

## Logging

Structured JSON to stdout. Levels follow a sidecar-appropriate discipline:

| Level   | What                                                                |
|---------|---------------------------------------------------------------------|
| `error` | genuine internal faults                                             |
| `warn`  | readiness became **not-ready** (LB will deroute); upstream `502` proxy / WS dial errors; mid-stream upstream response body errors / idle timeouts; upstream `h2c → http/1.1` fallback; connection-limit saturation; persistent `accept()` failures |
| `info`  | lifecycle (start w/ version / listen / shutdown), readiness **recovered** |
| `debug` | routine activity: each health poll, upstream `h2c` confirmation, client connection/body read and transient `accept()` errors, WS disconnects |
| `trace` | fine-grained internal flow (request routing, connection accept / close) |

`info` is reserved for notable, low-frequency events, so a healthy sidecar is
nearly silent between state changes — request rates and latencies belong in
metrics, not one log line per request. Upstream `502` proxy / WebSocket dial
failures, mid-stream upstream response body errors or idle timeouts, and
connection-limit saturation transitions emit `warn` so degraded upstream
connectivity is visible under the default `info` level, while routine client
disconnects stay `debug`.
Readiness *changes* are logged once by the poller (not per probe).

The startup line carries two version fields: `version` (the crate version, e.g.
`v0.1.2`) and `git` (the build's `git describe`). For a tagged release the two
match (`git` is `v0.1.2`); an ad-hoc build off a later commit shows
`v0.1.2-5-g20537f9`, with a `-dirty` suffix when the tree had uncommitted
changes — so a binary's exact provenance is always visible in its logs. (`git`
is `unknown` when built without a git checkout, e.g. from a source tarball.)
After the initial upstream warm-up poll, the connection-summary `info` line also
records `mode` (`"proxy"`, `"proxy+admin"`, or `"probe-only"`).

Set the level with `--log-level <trace|debug|info|warn|error>` (default `info`). `RUST_LOG` overrides `--log-level` and allows per-target directives. Under both `--log-level` and `RUST_LOG`, `opentelemetry-otlp` is always capped at `info` and `*.PartialSuccess` warnings are suppressed so raw OTLP export URLs and collector error bodies are never logged:

```sh
ethonyx --log-level debug ...
RUST_LOG=ethonyx=debug,hyper=warn ethonyx ...
```

### Access log

For a per-request trail — peer, HTTP version, method, path, upstream destination (`EL`/`CL`), upstream protocol (`HTTP`/`WS`), HTTP status code, and latency in milliseconds — enable `--access-log` (`ETHONYX_ACCESS_LOG`). It
emits one line per request on a dedicated `access_log` target, kept separate
from the application log (the nginx / Envoy / Caddy split) so the `info` stream
stays quiet by default. Health-probe paths (`/livez`, `/readyz`, `/healthz`, `/metrics`) are
**excluded** even when it's on, so frequent k8s / LB checks don't bury real
traffic.

The `access_log` target is deliberately *not* under `ethonyx`, so raising the app
log (`--log-level debug`, or `RUST_LOG=ethonyx=debug`) does **not** turn it on —
the access log is controlled only by `--access-log` or by naming its target
directly. When `RUST_LOG` is set it takes over the whole filter (the
`--access-log` flag is then ignored), so name the target there if you want it:

```sh
ethonyx --access-log ...
RUST_LOG=ethonyx=debug,access_log=info ethonyx ...
```

## Metrics

ethonyx exports core application and proxy metrics on `GET /metrics` (on `--admin-listen` when configured, otherwise on `--listen`).

The following metrics are exposed:
- `ethonyx_active_connections`: Number of currently active data-plane client connections on `--listen` (labeled by `protocol`: `tcp` tracks open inbound HTTP/1.1 and HTTP/2 TCP sessions until upgrade handoff or socket close, while `ws` tracks active upgraded HTTP/1.1 and HTTP/2 Extended `CONNECT` WebSocket relays; `--admin-listen` probe connections are excluded).
- `ethonyx_connection_limit_saturated_total`: Total number of inbound TCP accepts queued or HTTP/2 Extended `CONNECT` WebSocket streams rejected due to `--max-connections` saturation on `--listen`.
- `ethonyx_proxy_requests_total`: Total number of proxied HTTP/WS requests (labeled by `upstream`, `method`, `status`; common HTTP status codes have individual series while unlisted codes are grouped under `status="OTHER"`).
- `ethonyx_proxy_request_duration_seconds`: Explicit-bucket latency histogram of upstream proxied requests (`0.005`–`10`s + `+Inf`, labeled by `upstream`) exported consistently to Prometheus (`_bucket`/`_sum`/`_count` for `histogram_quantile()`) and OpenTelemetry.
- `ethonyx_response_body_errors_total`: Total number of mid-stream upstream response body failures occurring after response headers were received (labeled by `upstream` as `EL` or `CL`, and `reason` as `idle_timeout` or `upstream_error`, including quiet SSE subscriptions that hit `--sse-idle-timeout`; client-side disconnects while reading the response are excluded; like request counters, each label series is emitted once its first error occurs, so alerts should check `> 0` or account for `absent()` rather than relying solely on `increase()` to catch the first sample).
- `ethonyx_upstream_peers`: Number of peers reported by upstream nodes (labeled by `layer`: `layer="el"` is omitted when `net_peerCount` is unsupported, and `layer="cl"` is omitted when CL is disabled).
- `ethonyx_upstream_sync_distance`: Remaining sync distance in blocks or slots (labeled by `layer` as `el` or `cl`).
- `ethonyx_upstream_block_number`: Latest execution layer block number.
- `ethonyx_upstream_slot_number`: Latest consensus layer head slot number (registered only when `--cl-beacon-url` is set).
- `ethonyx_upstream_health_status`: Upstream health status (1 = healthy/synced, 0 = degraded/down, labeled by `layer`).
- `ethonyx_reference_health_status`: Reference lag-sample status (`1` = verified `(block_number, block_lag)` sample obtained this round, `0` = degraded/unavailable; registered only when `--el-reference-url` is set).
- `ethonyx_reference_block_number`: Highest verified block number from the latest `--el-reference-url` sample (retains its last value across fail-open outages; filter alerts with `ethonyx_reference_health_status == 1`).
- `ethonyx_reference_block_lag`: Local EL block lag from the latest `--el-reference-url` sample (retains its last value across fail-open outages; filter alerts with `ethonyx_reference_health_status == 1`).

## OpenTelemetry

If built with the `otel` feature, ethonyx supports exporting traces and metrics to an OTLP endpoint (HTTP protocol). Passing `--otel-endpoint` / `ETHONYX_OTEL_ENDPOINT` (including an empty string) to a default (non-`otel`) build fails fast at startup rather than silently ignoring telemetry configuration.

Pass an HTTP(S) **base URL**, optionally with a path prefix and without a query or fragment.
Ethonyx appends `/v1/traces` and `/v1/metrics` for the respective signals (in probe-only mode
without `--listen`, only the `/v1/metrics` provider is started and `Prepared::tracer()` returns
`None`). The exporters use `hyper-util` + Rustls with bundled `webpki-roots` (working out of the box
in `FROM scratch` containers; OS/private CA stores and `HTTP_PROXY`/`HTTPS_PROXY`
environment variables are not loaded) and run their blocking work outside Tokio
workers. Active providers flush on orderly shutdown (Ctrl-C, or SIGTERM on Unix).

```sh
# Build with OpenTelemetry support
cargo build --features otel

# Run with OTLP endpoint
ethonyx --otel-endpoint http://localhost:4318 ...
```

When enabled:
- **Distributed Tracing**: Context propagation is hooked into HTTP and WebSocket upstream requests (W3C standard `traceparent` and `tracestate` headers are injected after hop-by-hop header sanitization).
- **Span Attributes**: Upstream requests are instrumented with tracing spans containing method, path, upstream layer, `http.response.status_code`, and `Error` span status on `5xx` responses. Request spans remain active for OTLP export regardless of `--log-level` (e.g., `--log-level warn`).
- **Exporter Timeouts**: OTLP HTTP exports honor `OTEL_EXPORTER_OTLP_TRACES_TIMEOUT` and `OTEL_EXPORTER_OTLP_METRICS_TIMEOUT` (falling back to `OTEL_EXPORTER_OTLP_TIMEOUT`, default `10000` ms, clamped to 1 hour) and are bounded by a 5-second flush deadline on shutdown.
- **Resource Identity**: `service.name` defaults to `ethonyx` and honors standard `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES` overrides.

## systemd

```ini
[Unit]
Description=Ethonyx EL/CL sidecar
After=network-online.target geth.service lighthouse.service
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/ethonyx \
  --network mainnet \
  --listen 0.0.0.0:8547 \
  --el-http-url   http://127.0.0.1:8545 \
  --el-ws-url     ws://127.0.0.1:8546 \
  --cl-beacon-url http://127.0.0.1:5052
Restart=on-failure
RestartSec=2
User=ethonyx
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

For testnets, set `--network hoodi` (or `sepolia` / `holesky`). For a private
chain: `--network custom --cl-genesis-time <unix> --cl-seconds-per-slot 12`.

### Readiness & reference block-lag tuning

`/readyz` gates on sync status (and CL Engine API health when `--cl-beacon-url` is set).
To deroute slow or silently stalled nodes without draining the fleet during a chain halt,
point `--el-reference-url` at your peer nodes (or external reference RPCs):

| Flag                           | Default | Gates `/readyz` on                                    |
|--------------------------------|---------|-------------------------------------------------------|
| `--el-reference-url`           | `none`  | Comma-separated / repeatable peer or reference EL URLs |
| `--el-max-reference-lag`       | `1`     | Max allowed EL block lag behind highest valid reference |
| `--el-reference-poll-interval` | `3`     | Reference poll interval in seconds                    |
| `--el-max-block-age-secs`      | `60`    | EL latest-block wall-clock age (with `--readyz-strict`) |
| `--cl-max-slot-age-secs`       | `60`    | CL head-slot wall-clock age (with `--readyz-strict`)  |

- **Peer-only vs. external reference URLs**: Listing peer Ethonyx/EL URLs (never the cluster's own frontend LB VIP) in a shared ConfigMap is safe even when a node's own URL is included (entries matching `--el-http-url` are stripped at startup, and pod-DNS self-entries evaluate to `lag == 0`), ensuring the highest peer stays `200 Ready` in Kubernetes `Service` pools (which do not fail open when 0 pods are ready). Mixing in an external commercial RPC can drain all local pods if the entire local cluster falls behind that external provider — pair external references with load-balancer panic-mode fail-open where appropriate.
- **Sub-second block chains**: On chains with `< 1s` block times (e.g. Arbitrum `250ms`, Sei `400ms`), raise `--el-max-reference-lag` (e.g. `3–8`) so normal P2P propagation and asynchronous poll-window spread do not flap readiness.
- **EL-only mode**: Omit `--cl-beacon-url` on non-Beacon chains; configure `--el-reference-url` or `--readyz-strict` if you want silent EL stalls to fail `/readyz`.
- `--el-max-block-age-secs` and `--cl-max-slot-age-secs` remain inert without `--readyz-strict`; `/healthz` always reports the raw age regardless (omitted only if the system clock precedes the Unix epoch). Peer counts never gate readiness.

### Trusted / Commercial Endpoint & Data-Plane Mocking Tuning

When running `ethonyx` against a fallback/secondary node or a commercial paid RPC provider (e.g., Alchemy, Infura, QuickNode):

| Flag                                     | Default | Description                                                                 |
|------------------------------------------|---------|-----------------------------------------------------------------------------|
| `--trust-upstream`                       | `false` | Disable background health polling (0 RPC cost) and always report ready/200 (incompatible with `--readyz-strict` / `--el-reference-url`). |
| `--mock-cl-subscriptions`                | `false` | Intercept `prepare_beacon_proposer`, `beacon_committee_subscriptions`, and `sync_committee_subscriptions` with `200 OK` (requires `--cl-beacon-url`). |
| `--mock-eth-syncing`                     | `false` | Statically mock `eth_syncing` JSON-RPC (HTTP & WebSocket) with `result: false`. |
| `--mock-cl-node-health`                  | `false` | Intercept `GET/HEAD /eth/v1/node/health` locally (`200`/`206` or `?syncing_status`/`400`/`503`, or `200 OK` with `--trust-upstream`; requires `--cl-beacon-url`). |
| `--cl-mev-relay-url`                     | `none`  | Optional upstream MEV Relay URL (`ETHONYX_CL_MEV_RELAY_URL`; requires `--cl-beacon-url`). |

- **`--trust-upstream`**: When enabled, `ethonyx` trusts the upstream node and skips background EL/CL health polling (consuming zero Compute Units / API credits on commercial providers). `/healthz` reports `"mode": "trusted"` with only the active `transport` per layer (no fabricated peer or block numbers), and `/metrics` omits unpolled upstream gauges.
- **`--mock-cl-subscriptions`**: Intercepts `POST /eth/v1/validator/prepare_beacon_proposer`, `POST /eth/v1/validator/beacon_committee_subscriptions`, and `POST /eth/v1/validator/sync_committee_subscriptions` requests (sent periodically by SSV Node / Validator Clients) and returns `HTTP 200 OK` (`{"code":200,"message":"success"}`) locally, shielding against 400 errors from commercial providers that block write-only validator APIs.
- **`--mock-eth-syncing`**: Intercepts `eth_syncing` JSON-RPC calls over HTTP POST and WebSocket frames locally (`result: false`), shielding commercial providers from downstream client sync polls.
- **`--mock-cl-node-health`**: Intercepts `GET/HEAD /eth/v1/node/health` requests locally using ethonyx's cached readiness state (`200` ready, `206` or the Beacon API `?syncing_status=<code>` override in `200..=599` while syncing and both layers are reachable, `400` on invalid `?syncing_status`, `503` unhealthy/stale; always `200 OK` under `--trust-upstream`), shielding commercial providers from beacon node health probes.
- **`--cl-mev-relay-url`**: When configured (e.g., `https://boost-relay-hoodi.flashbots.net`), `ethonyx` intercepts `POST /eth/v1/validator/register_validator` requests from validator clients (such as SSV Node), rewrites the request path to `/eth/v1/builder/validators`, and forwards them directly to the specified MEV Relay.

With `--trust-upstream`, cleartext HTTP uses HTTP/1.1 because health-based h2c
detection is disabled; HTTPS still negotiates via ALPN. Data-plane requests are
never retried across protocols.

The `eth_syncing` mock handles JSON-RPC 2.0 calls and entirely local batches.
Notifications (no `id`) produce HTTP `204` or no WebSocket message; explicit
`id: null` still receives a reply. Mixed batches and invalid requests are
forwarded intact. HTTP POST bodies under `--mock-eth-syncing` inspect up to a
64 KiB prefix and forward upstream under one shared `--proxy-timeout` budget
(returning `408` if the prefix itself stalls), streaming larger bodies straight
through to upstream without buffering or `413` (a client stall after switching
to streaming surfaces as an upstream timeout `502`); `--mock-cl-subscriptions`
drains request bodies up to 64 MiB (`413` on overflow) and `--proxy-timeout`
(`408` on a stalled body). Ordinary HTTP forwarding continues to stream bodies.

## Development

Git hooks live in `.githooks/`. One-time setup per clone:

```sh
git config core.hooksPath .githooks
cargo install --locked \
    cargo-audit \
    cargo-deny \
    cargo-release \
    cargo-llvm-cov
# optional: `just` (https://github.com/casey/just) for the shortcuts in justfile
```

- `pre-commit` runs `cargo fmt --all -- --check` and `cargo clippy
  --all-targets --locked -- -D warnings` (skipped when the commit touches no
  Rust / `Cargo.*` / `rust-toolchain*` files).
- `pre-push` runs `cargo test --locked` plus `cargo audit -D warnings`
  (RustSec advisory DB). The audit step is soft-skipped if `cargo-audit` is
  not installed.
- Bypass with `--no-verify` only for emergencies.

Common tasks (via `just`):

| Recipe          | Action                                        |
|-----------------|-----------------------------------------------|
| `just check`    | fmt + clippy + doc + test (default & `--all-features`) + deny + audit |
| `just fmt`      | `cargo fmt --all`                             |
| `just deny`     | `cargo deny --all-features check` (supply-chain audit) |
| `just coverage` | HTML coverage report under `target/llvm-cov/` (`--all-features`) |
| `just release`  | `cargo release patch --execute`               |

Open in a devcontainer (VSCode / Codespaces) and `.devcontainer/devcontainer.json`
installs all of the above plus the cross-compile targets automatically.

### Extended validation

Run the longer correctness checks explicitly:

```sh
ETHONYX_STRESS_SECONDS=60 cargo test --release --locked --test integration \
  websocket::websocket_sustained_churn_preserves_replies_and_releases_upstreams \
  -- --ignored --exact --nocapture
```

The WebSocket check uses 16 concurrent clients, orderly and abrupt disconnects,
binary messages, notifications and mock/forwarded calls; the duration applies
to each of the two mock modes. It verifies that all upstream connections are
released. CI runs a shorter five-second-per-mode churn check on Linux, macOS
and Windows.

## Release

Releases are cut with [`cargo-release`](https://github.com/crate-ci/cargo-release):

```sh
cargo install cargo-release --locked   # one-time

# Dry-run first to preview
cargo release patch

# Apply
cargo release patch --execute   # 0.1.0 → 0.1.1
# or: cargo release minor --execute     0.1.0 → 0.2.0
# or: cargo release 0.5.0  --execute    explicit
```

This runs `cargo test --locked --all-features` as a gate, then bumps `Cargo.toml` + `Cargo.lock`,
commits as `chore: release vX.Y.Z`, tags `vX.Y.Z`, and pushes both. The `v*` tag
push triggers `.github/workflows/release.yml`, which:

1. Verifies `Cargo.toml` version matches the tag
2. Creates a GitHub Release with auto-generated notes
3. Generates license notices from `Cargo.lock` for each release target
4. Builds binaries with `--locked`, verifies the archive contents, and attaches
   them with `.sha256` checksums and build provenance attestations:
   - `ethonyx-vX.Y.Z-x86_64-unknown-linux-musl.tar.gz`
   - `ethonyx-vX.Y.Z-aarch64-unknown-linux-musl.tar.gz`
   - `ethonyx-vX.Y.Z-aarch64-apple-darwin.tar.gz`
   - `ethonyx-vX.Y.Z-x86_64-pc-windows-msvc.zip`

Every binary archive includes `LICENSE` (Ethonyx's dual-license notice),
`LICENSE-MIT` and `LICENSE-APACHE` (the full license texts),
`THIRD_PARTY_LICENSES.txt` (dependency inventory, license texts and upstream
copyright/notice files, plus musl's copyright notice on Linux musl targets),
and `RUST_LIBRARY_LICENSES.html` (the active Rust toolchain's library notices).
Notice generation or archive verification failures stop the release upload.

To regenerate notices locally with Python 3.9+:

```sh
cargo install cargo-about --version 0.9.3 --locked --features cli
python3 scripts/generate-licenses.py --target x86_64-unknown-linux-musl
# For a custom build, use the same --features / --all-features /
# --no-default-features flags as cargo build.
```

Generated files live in `target/release-licenses/` and are not checked in.
`about.toml` controls accepted licenses; keep it aligned with `deny.toml`.
CI builds and verifies actual archives with both default and all features for
every release target, using the same packaging action as releases. Run the
archive verifier's missing/altered-file regression tests with
`python3 -m unittest discover -s scripts -p 'test_*.py'`. Build
dependencies and complete upstream license bundles are retained conservatively,
so notices can include code that the final binary does not use. When changing
Rust or the native toolchain, also review [the musl notice](licenses/README.md).

`cargo-release` is configured under `[package.metadata.release]` in `Cargo.toml`
(main-branch only, no crates.io publish, `cargo test --locked --all-features` as pre-hook).

## License

Ethonyx is available under either the [MIT license](LICENSE-MIT) or the
[Apache License, Version 2.0](LICENSE-APACHE), at your option
(`MIT OR Apache-2.0`). Third-party dependencies retain their own licenses.

## Contributions

By submitting a contribution for inclusion in Ethonyx, you agree to license it
under both MIT and Apache-2.0, unless you explicitly state otherwise.
Contributions carry no additional terms or conditions unless separately agreed.
