# ethonyx Architecture & Engineering Rules

These rules govern all design, refactoring, and implementation work across the `ethonyx` sidecar.

- **Constitution vs. Specification Separation**:
  - Keep this file strictly limited to **timeless architectural invariants, module boundaries, proxy/security non-negotiables, Rust coding discipline, and verification gates**.
  - Place concrete CLI flags, environment variable names, endpoint paths, metric names, timeout budgets, and release notes exclusively in `README.md` and `CHANGELOG.md` (never hardcode volatile specification details here, and never violate these invariants in design or code).

## 1. Architecture Philosophy, Boundaries & Supply-Chain Discipline
- **Hot-Path vs. Non-Hot-Path & Threat-Boundary Principle**:
  - **Hot Paths (Performance Within Clean Structural Bounds)**: Focus on throughput, tail latency, wait-free observability, zero-copy streaming/framing, and compact async futures along true request/relay hot paths (HTTP proxy forwarding, streaming body/SSE relay, WebSocket frame relay, and per-request metric recording), **strictly without breaking module boundaries or lifecycle invariants**.
  - **Non-Hot Paths & Threat-Exposed External Interfaces (Security & Operability First)**: Across configuration/CLI/env parsing, upstream URL validation, HTTP/WebSocket handshake sanitization, health/readiness evaluation, and administrative/diagnostic endpoints, prioritize **fail-closed security, zero-bypass validation, and operational ergonomics** over micro-optimizations.
- **Untrusted Input Boundary, Fail-Closed Semantics & Zero-Leakage Telemetry**:
  - Treat all downstream client requests, proxy/hop-by-hop headers, upstream responses, and process environment variables as untrusted: strip hop-by-hop headers and validate strictly before routing, tracing, or protocol classification, keep upstream TLS verification intact, and fail closed on malformed inputs or stale/unverified EL/CL health state.
  - Never expose upstream credentials, secret URL paths/queries, internal stack details, or raw backend error bodies across logs (including third-party telemetry exporter logs), traces, metrics, or client-facing error responses; keep synthetic gateway errors opaque and static.
- **Minimal TCB, Cross-Platform Portability & Capability-Additive Features Only**:
  - Minimize external dependencies and transitive surface; avoid adding crates where small, clear `std` or existing-crate primitives suffice. Keep all optional Cargo features strictly capability-additive (compiling cleanly with zero behavior change or dead-code warnings when disabled), and forbid security-weakening or test-backdoor features.
  - Maintain cross-platform support across Linux, macOS, and Windows: isolate OS-specific socket or signal handling behind narrow helpers without leaking platform assumptions across modules, and keep module visibility tight (crate-wide `unreachable_pub` warnings, minimal public library API, and `src/main.rs` as a thin entrypoint).

## 2. Runtime Pipeline, Control Survivability & Transport Discipline
- **Control Survivability & Plane Isolation**:
  - Statically isolate health/readiness probes and diagnostics from data-plane saturation and slow-client exhaustion: when an isolated administrative listener is configured, serve control endpoints exclusively there with bounded connection/lifetime limits and hide them from the data plane.
  - Preserve strict graceful shutdown and cancellation safety across listeners, in-flight requests, streaming bodies, WebSocket relays, and background pollers without leaking tasks, permits, or upstream connections.
- **Sans-IO Core, Thin Wire Adapters & Bounded Streaming/Watchdogs**:
  - Keep protocol classification, health/sync evaluation, URI rewriting, header sanitization, and JSON-RPC parsing as pure, synchronous Sans-IO modules beneath thin async transport/server adapters.
  - Stream proxy payloads by default on the data plane and cap response inspection on health/probe paths; enforce bounded memory, concurrency, header/handshake, idle, and minimum-progress stream watchdogs on every network-facing lane so stalled peers or slow readers cannot exhaust memory or connection pools.

## 3. Idiomatic Rust, Concurrency Discipline & Verification Gates
- **Eliminate Avoidable Allocations & Memory Copies**:
  - Prefer ownership moves (`into_*`, consuming `self`, `std::mem::take`), borrowed slices (`&[u8]`, `&str`), stack arrays, and refcounted byte slicing (`Bytes`); place all validation and early-return guards (`?`, `return`) **before** allocating owned buffers, building requests, or spawning tasks.
  - Keep per-connection and per-request async futures compact by separating cold/admin paths from hot data-plane loops and avoiding unnecessary stack bloat or redundant I/O and deserialization.
- **Memory Safety, Async Lock & Panic Discipline**:
  - Forbid `unsafe_code` across the entire crate and never hold `Mutex` / `RwLock` guards across `.await` points or heavy work; prefer lock-free atomics or brief critical sections.
  - Release builds run with `panic = "abort"`: strictly forbid `.unwrap()`, `.expect()`, and unchecked indexing/arithmetic on any external, network, or configuration input path (eliminate internal invariant `.expect()` calls via types or pattern matching wherever practical; reserve `.unwrap()` / `.expect()` for tests).
  - Keep a single consolidated `impl` block per type, expressive `Option`/`Result`/iterator combinators, `Copy` on lightweight value types, and zero over-abstracted trait or macro gymnastics.
- **Deterministic Testing & Verification Gates**:
  - Pass explicit timestamps into Sans-IO health/clock cores, and drive async timeouts, graceful-drain deadlines, and stream watchdogs deterministically with simulated Tokio time (`start_paused = true`) rather than wall-clock sleeps wherever possible.
  - Accompany behavior or protocol changes with unit conformance tests, integration/adversarial coverage, compile-time layout/invariant assertions where appropriate, and `README.md`/`CHANGELOG.md` synchronization.
  - Before finalizing any change, keep `cargo fmt --all -- --check`, `cargo clippy --all-targets --locked -- -D warnings`, `cargo test --locked`, and warning-free `cargo doc --no-deps --locked` (both default and `--all-features`), plus `cargo deny --all-features check` and `cargo audit -D warnings` green.
