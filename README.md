# ethonyx

[![CI](https://github.com/DELIGHT-LABS/ethonyx/actions/workflows/ci.yml/badge.svg)](https://github.com/DELIGHT-LABS/ethonyx/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/DELIGHT-LABS/ethonyx?include_prereleases&sort=semver)](https://github.com/DELIGHT-LABS/ethonyx/releases)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/License-MIT%20OR%20Apache--2.0-yellow.svg)](LICENSE)
[![Rust 1.99+](https://img.shields.io/badge/rust-1.99%2B-blue.svg)](rust-toolchain.toml)

Lightweight Rust reverse proxy and health-check sidecar for Ethereum **Execution Layer** (EL)
and **Consensus Layer** (CL) node pairs.

## Routing

| Request                              | Forwarded to        |
|--------------------------------------|---------------------|
| `GET/HEAD /livez`                    | 200 OK (process liveness) |
| `GET/HEAD /readyz`                   | EL + CL readiness gate (200 / 503) |
| `GET/HEAD /healthz`                  | EL + CL state snapshot (always 200) |
| `GET/HEAD /metrics`                  | Prometheus metrics scrape endpoint |
| `GET/HEAD /eth/v1/node/health`       | ethonyx node health (200 / 206 / 503) if `--mock-cl-node-health` set, else `--cl-beacon-url` |
| `POST /eth/v1/validator/register_validator` | `--cl-mev-relay-url` (rewritten to `/eth/v1/builder/validators`) if set, else `--cl-beacon-url` |
| `POST /eth/v1/validator/{prepare_beacon_proposer, beacon_committee_subscriptions, sync_committee_subscriptions}` | `200 OK` if `--mock-cl-subscriptions` set, else `--cl-beacon-url` |
| `/eth/...`, `/lighthouse/...`, `/prysm/...`, `/teku/...`, `/lodestar/...`, `/nimbus/...` | `--cl-beacon-url`   |
| `Upgrade: websocket`                 | `--el-ws-url`       |
| everything else (JSON-RPC `POST /`)  | `--el-http-url`     |

## Probes

Three endpoints, split by purpose (Kubernetes `z` convention):

| Endpoint            | Returns                       | Wire to                     | Gates on                                              |
|---------------------|-------------------------------|-----------------------------|-------------------------------------------------------|
| `GET/HEAD /livez`   | always `200`, body `ok`       | liveness probe (restart)    | nothing — only that the process is up; no upstream call |
| `GET/HEAD /readyz`  | `200` ready / `503` not ready | readiness probe (LB gate)   | EL + CL **sync status** (plus freshness with `--readyz-strict`) |
| `GET/HEAD /healthz` | always `200` + JSON snapshot  | monitoring / alerting / curl | nothing — reports state, never judges                 |
| `GET/HEAD /metrics` | Prometheus metrics text format | Prometheus scraper          | nothing — metrics scraper                             |

`/livez` and `/readyz` differ exactly when the node is up but not serving yet
(startup, mid-sync, or an upstream is down): `/livez` stays `200` (don't restart
me) while `/readyz` returns `503` (don't route to me). Once synced, both are
`200`. So **default `/readyz` is `/livez` plus an EL+CL sync check.**

### `/readyz` — the traffic gate

Gates **only on EL + CL sync status** by default. A caught-up node reports
`eth_syncing == false` even when the chain stalls network-wide, so a chain
incident (or a fleet-wide peer dip) won't drop every backend out of the load
balancer at once — which would turn a chain incident into a total RPC outage.
Sync status is the node-local signal that tells "this node can serve" apart from
"this node is behind its peers".

```jsonc
// 200 — synced. el_block_fresh / cl_slot_fresh appear only under --readyz-strict.
{ "status": "ready",
  "el_syncing": { "ok": true, "detail": "synced" },
  "cl_syncing": { "ok": true, "detail": "synced (slot 9412341, distance 0)" } }
```

`--readyz-strict` additionally gates on EL block age (`--el-max-block-age-secs`)
and CL slot age (`--cl-max-slot-age-secs`) — choose it when serving strictly
at-head data matters more than fleet availability during a stall.

### `/healthz` — state for monitoring

Always `200`. Reports each live EL/CL value as a machine-readable field for your
monitoring stack to threshold; it applies no thresholds and renders no verdict.
A signal whose upstream call failed is omitted and the error is recorded under
that layer's `errors` array.

| Field                                              | Source                                |
|----------------------------------------------------|---------------------------------------|
| `el.transport` / `cl.transport` (`h2c`/`h2`/`http/1.1`) | upstream HTTP version ethonyx uses |
| `el.syncing` (`false` = synced)                    | EL `eth_syncing`                      |
| `el.sync_distance` (while syncing)                 | EL `eth_syncing` highest − current    |
| `el.peers`                                         | EL `net_peerCount`                    |
| `el.block_number` / `el.block_age_secs`            | EL `eth_getBlockByNumber("latest")`   |
| `cl.syncing` / `cl.sync_distance` / `cl.head_slot` | Beacon `/eth/v1/node/syncing`         |
| `cl.slot_age_secs`                                 | Beacon `head_slot` vs. wall-clock     |
| `cl.peers`                                         | Beacon `/eth/v1/node/peer_count`      |
| `el.errors` / `cl.errors`                          | any upstream call that failed         |

```json
{ "el": { "transport": "h2c", "syncing": false, "peers": 23, "block_number": 21000000, "block_age_secs": 5 },
  "cl": { "transport": "http/1.1", "syncing": false, "sync_distance": 0, "peers": 78, "head_slot": 9412341, "slot_age_secs": 3 } }
```

CL slot age is derived from `head_slot * --cl-seconds-per-slot +
--cl-genesis-time`. Use `--network <name>` for a preset (defaults to mainnet)
instead of typing both; `--cl-genesis-time 0` omits `cl.slot_age_secs`.

### Polling

`/readyz` and `/healthz` don't query upstream per request. A background task
polls the EL batch and both CL endpoints concurrently, then waits
`--health-poll-interval` (default 1s; each call
bounded by `--health-timeout`, including the response body) before the next poll, and the endpoints return
the latest snapshot instantly — so upstream load is constant regardless of probe
rate, a slow upstream never blocks a probe, and the poller pauses a full interval
between polls rather than hammering a struggling node. Block / slot ages are
recomputed live per request, so they stay accurate between polls. The cache is
warmed by one poll before the listener accepts, so the process serves a real
snapshot from its first probe.

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
explicit `--cl-genesis-time <unix>` and `--cl-seconds-per-slot <secs>`. The
sidecar refuses to start if either is missing under `custom`.

## HTTP/2

ethonyx serves HTTP/1.1 and HTTP/2 on the same `--listen` port — the protocol is
auto-detected per connection. Cleartext HTTP/2 (**h2c**, prior-knowledge) is
supported, covering the common "TLS-terminating LB / mesh forwards h2c to the
backend" shape (Envoy, Istio, HAProxy `proto h2`); plain HTTP/1.1 and the
HTTP/1.1 WebSocket upgrade are unchanged.

WebSocket works over both transports: the HTTP/1.1 `Upgrade` handshake and
HTTP/2 Extended CONNECT (RFC 8441, `:protocol=websocket`). Either is bridged to
the upstream's HTTP/1.1 WebSocket (`--el-ws-url`).

Subprotocol offers from all request header lines are combined, and the selected
protocol is relayed from the upstream. Unsupported extensions, including
compression, are not negotiated; unsolicited extension responses are rejected.
Frames remain limited to 16 MiB and complete messages to 64 MiB, including
fragmented messages. The
relay reads in 8 KiB chunks with frame storage that grows for larger frames,
and uses 16 KiB write buffers; ordinary forwarding needs no additional shared
sink mutex. Tungstenite
continues to validate masking, UTF-8, control frames, and upstream handshakes.

Shutdown sets one deadline shared by HTTP connections, HTTP/2 request tasks
and WebSocket relays. Existing work can finish during `--shutdown-grace`; late
handshakes use the time remaining. At the deadline, pending work is cancelled,
and the library waits for its futures and sockets to be released before returning,
including with zero grace or an already completed shutdown future.

The upstream client auto-negotiates h2 for `https://` upstreams via ALPN. A
cleartext EL JSON-RPC upstream can't be auto-negotiated, so the health poller
probes it: it prefers cleartext **h2c** and forwards over HTTP/2 when the upstream
serves it (geth ≥v1.17, erigon, reth), falling back to HTTP/1.1 otherwise — no
flag. The verdict starts at h2c and is confirmed by the first poll before traffic
is served. A running upstream that drops h2c is detected within one poll; one that
newly adds h2c is picked up on restart (while HTTP/1.1 works it isn't re-probed).
The data-plane follows the verdict and never retries across protocols (to avoid
double-sending a non-idempotent call like `eth_sendRawTransaction`), so while a
running upstream is switching away from h2c the data-plane can briefly return
`502`s — up to one poll interval — until the verdict updates. The CL Beacon hop
stays HTTP/1.1. (The gain is mainly under high request concurrency; for a
localhost sidecar hop it's modest.)

ethonyx does **not** terminate TLS — it serves plaintext and leaves TLS to the
LB / service mesh in front.

## Usage

```sh
ethonyx \
  --listen 0.0.0.0:8547 \
  --el-http-url   http://127.0.0.1:8545 \
  --el-ws-url     ws://127.0.0.1:8546 \
  --cl-beacon-url http://127.0.0.1:5052
```

Every flag also accepts an `ETHONYX_*` env var (see `ethonyx --help`).

### Multi-port listen

`--listen` is repeatable (or comma-separated). All ports serve identical
routes — useful when some traffic hits the box via LB on one port while
operators / scrapers reach it directly on another:

```sh
ethonyx \
  --listen 0.0.0.0:8547 \
  --listen 127.0.0.1:9547 \
  ...
# or
ETHONYX_LISTEN=0.0.0.0:8547,127.0.0.1:9547 ethonyx ...
```

Each listener runs an independent accept loop on the tokio runtime, so cores
saturate naturally without cross-listener locks.

## Logging

Structured JSON to stdout. Levels follow a sidecar-appropriate discipline:

| Level   | What                                                                |
|---------|---------------------------------------------------------------------|
| `error` | genuine internal faults                                             |
| `warn`  | readiness became **not-ready** (LB will deroute); `accept()` failed |
| `info`  | lifecycle (start w/ version / listen / shutdown), readiness **recovered**, the EL upstream h2c↔h1 switch |
| `debug` | routine activity: per-request proxy / WS outcomes, each health poll, connection errors |
| `trace` | fine-grained internal flow (request routing, connection accept / close) |

`info` is reserved for notable, low-frequency events, so a healthy sidecar is
nearly silent between state changes — request rates and latencies belong in
metrics, not one log line per request. Routine upstream / client failures (a 502,
a dropped WebSocket) are `debug`, not `error` — for a sidecar they are everyday.
Readiness *changes* are logged once by the poller (not per probe).

The startup line carries two version fields: `version` (the crate version, e.g.
`v0.1.2`) and `git` (the build's `git describe`). For a tagged release the two
match (`git` is `v0.1.2`); an ad-hoc build off a later commit shows
`v0.1.2-5-g20537f9`, with a `-dirty` suffix when the tree had uncommitted
changes — so a binary's exact provenance is always visible in its logs. (`git`
is `unknown` when built without a git checkout, e.g. from a source tarball.)

Set the level with `--log-level <trace|debug|info|warn|error>` (default `info`).
`RUST_LOG` overrides it and allows per-target directives:

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

ethonyx exports core application and proxy metrics on `GET /metrics`.

The following metrics are exposed:
- `ethonyx_active_connections`: Number of currently active client connections (labeled by `protocol` as `tcp` or `ws`).
- `ethonyx_proxy_requests_total`: Total number of proxied HTTP/WS requests (labeled by `upstream`, `method`, `status`).
- `ethonyx_proxy_request_duration_seconds`: Latency histogram of upstream proxied requests (labeled by `upstream`).
- `ethonyx_upstream_peers`: Number of peers reported by upstream nodes (labeled by `layer` as `el` or `cl`).
- `ethonyx_upstream_sync_distance`: Remaining sync distance in blocks or slots (labeled by `layer` as `el` or `cl`).
- `ethonyx_upstream_block_number`: Latest execution layer block number.
- `ethonyx_upstream_slot_number`: Latest consensus layer head slot number.
- `ethonyx_upstream_health_status`: Upstream health status (1 = healthy/synced, 0 = degraded/down, labeled by `layer`).

## OpenTelemetry

If built with the `otel` feature, ethonyx supports exporting traces and metrics to an OTLP endpoint (HTTP protocol).

Pass an HTTP(S) **base URL**, optionally with a path prefix and without a query or fragment.
Ethonyx appends `/v1/traces` and `/v1/metrics` for the respective signals. The
exporters use Rustls and run their blocking work outside Tokio workers. Both
providers flush on orderly shutdown (Ctrl-C, or SIGTERM on Unix).

```sh
# Build with OpenTelemetry support
cargo build --features otel

# Run with OTLP endpoint
ethonyx --otel-endpoint http://localhost:4318 ...
```

When enabled:
- **Distributed Tracing**: Context propagation is hooked into client requests (W3C standard `traceparent` headers are injected into upstream requests).
- **Span Attributes**: Upstream requests are instrumented with tracing spans containing method, path, and upstream layer details.

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

### Readiness tuning

`/readyz` is sync-only by default and needs no tuning — it tracks whether the
node is caught up to its peers. Add `--readyz-strict` if you also want it to
fail when the node stops advancing at head; freshness then gates on:

| Flag                      | Default | Gates `/readyz` on            |
|---------------------------|---------|-------------------------------|
| `--el-max-block-age-secs` | `60`    | EL latest-block wall-clock age |
| `--cl-max-slot-age-secs`  | `60`    | CL head-slot wall-clock age    |

These two flags are inert without `--readyz-strict`; `/healthz` always reports
the raw age regardless. On young testnets or low-traffic private chains, widen
them (e.g. `120`) so normal slot gaps don't flap readiness. Peer counts no
longer gate anything — `/healthz` simply reports the live count for your
monitoring stack to threshold.

### Trusted / Commercial Endpoint & Data-Plane Mocking Tuning

When running `ethonyx` against a fallback/secondary node or a commercial paid RPC provider (e.g., Alchemy, Infura, QuickNode):

| Flag                                     | Default | Description                                                                 |
|------------------------------------------|---------|-----------------------------------------------------------------------------|
| `--trust-upstream`                       | `false` | Disable background health polling (0 RPC cost) and always report ready/200. |
| `--mock-cl-subscriptions`                | `false` | Intercept `prepare_beacon_proposer`, `beacon_committee_subscriptions`, and `sync_committee_subscriptions` with `200 OK`. |
| `--mock-eth-syncing`                     | `false` | Statically mock `eth_syncing` JSON-RPC (HTTP & WebSocket) with `result: false`. |
| `--mock-cl-node-health`                  | `false` | Intercept `GET /eth/v1/node/health` locally with `200 OK`.                  |
| `--cl-mev-relay-url`                     | `none`  | Optional upstream MEV Relay URL (`ETHONYX_CL_MEV_RELAY_URL`).                |

- **`--trust-upstream`**: When enabled, `ethonyx` trusts the upstream node and skips background EL/CL health polling (consuming zero Compute Units / API credits on commercial providers).
- **`--mock-cl-subscriptions`**: Intercepts `POST /eth/v1/validator/prepare_beacon_proposer`, `POST /eth/v1/validator/beacon_committee_subscriptions`, and `POST /eth/v1/validator/sync_committee_subscriptions` requests (sent periodically by SSV Node / Validator Clients) and returns `HTTP 200 OK` (`{"code":200,"message":"success"}`) locally, shielding against 400 errors from commercial providers that block write-only validator APIs.
- **`--mock-eth-syncing`**: Intercepts `eth_syncing` JSON-RPC calls over HTTP POST and WebSocket frames locally (`result: false`), shielding commercial providers from downstream client sync polls.
- **`--mock-cl-node-health`**: Intercepts `GET/HEAD /eth/v1/node/health` requests locally and returns `HTTP 200 OK` (Beacon API standard node health), shielding commercial providers from beacon node health probes.
- **`--cl-mev-relay-url`**: When configured (e.g., `https://boost-relay-hoodi.flashbots.net`), `ethonyx` intercepts `POST /eth/v1/validator/register_validator` requests from validator clients (such as SSV Node), rewrites the request path to `/eth/v1/builder/validators`, and forwards them directly to the specified MEV Relay.

With `--trust-upstream`, cleartext HTTP uses HTTP/1.1 because health-based h2c
detection is disabled; HTTPS still negotiates via ALPN. Data-plane requests are
never retried across protocols.

The `eth_syncing` mock handles JSON-RPC 2.0 calls and entirely local batches.
Notifications (no `id`) produce HTTP `204` or no WebSocket message; explicit
`id: null` still receives a reply. Mixed batches and invalid requests are
forwarded intact. HTTP POST bodies inspected by this opt-in mock are limited to
64 MiB (`413` on overflow) and `--proxy-timeout` (`408` on a stalled body).
Ordinary HTTP forwarding continues to stream bodies.

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
| `just check`    | fmt + clippy + test + audit (full local gate) |
| `just fmt`      | `cargo fmt --all`                             |
| `just deny`     | `cargo deny check` (supply-chain audit)       |
| `just coverage` | HTML coverage report under `target/llvm-cov/` |
| `just release`  | `cargo release patch --execute`               |

Open in a devcontainer (VSCode / Codespaces) and `.devcontainer/devcontainer.json`
installs all of the above plus the cross-compile targets automatically.

### Extended validation

Run the longer correctness checks explicitly:

```sh
cargo test --release --locked --lib rpc::tests::prefilter_seeded_stress_matches_full_parser \
  -- --ignored --exact
ETHONYX_STRESS_SECONDS=60 cargo test --release --locked --test integration \
  websocket_sustained_churn_preserves_replies_and_releases_upstreams \
  -- --ignored --exact --nocapture
```

The JSON check compares one million deterministic inputs with the full parser.
The WebSocket check uses 16 concurrent clients, orderly and abrupt disconnects,
binary messages, notifications and mock/forwarded calls; the duration applies
to each of the two mock modes. It verifies that all upstream connections are
released. CI runs the JSON check and a shorter five-second-per-mode churn check
on Linux, macOS and Windows.

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

This runs `cargo test --locked` as a gate, then bumps `Cargo.toml` + `Cargo.lock`,
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
(main-branch only, no crates.io publish, `cargo test --locked` as pre-hook).

## License

Ethonyx is available under either the [MIT license](LICENSE-MIT) or the
[Apache License, Version 2.0](LICENSE-APACHE), at your option
(`MIT OR Apache-2.0`). Third-party dependencies retain their own licenses.

## Contributions

By submitting a contribution for inclusion in Ethonyx, you agree to license it
under both MIT and Apache-2.0, unless you explicitly state otherwise.
Contributions carry no additional terms or conditions unless separately agreed.
