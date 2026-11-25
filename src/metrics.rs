//! Lock-free per-server atomic Prometheus metric registry, text exposition renderer,
//! and typed OpenTelemetry fanout.

use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use crate::otel::OtelMetrics;

pub(crate) const ACTIVE_CONNECTIONS: &str = "ethonyx_active_connections";
pub(crate) const CONNECTION_LIMIT_SATURATED: &str = "ethonyx_connection_limit_saturated_total";
pub(crate) const PROXY_REQUEST_DURATION: &str = "ethonyx_proxy_request_duration_seconds";
pub(crate) const PROXY_REQUESTS_TOTAL: &str = "ethonyx_proxy_requests_total";

/// Explicit histogram buckets `(bound, Prometheus le label)` shared by Prometheus
/// and OpenTelemetry for RPC/Beacon proxy latencies so `histogram_quantile()`
/// aggregates accurately across replicas.
const LATENCY_BUCKETS: [(Duration, &str); 11] = [
    (Duration::from_millis(5), "0.005"),
    (Duration::from_millis(10), "0.01"),
    (Duration::from_millis(25), "0.025"),
    (Duration::from_millis(50), "0.05"),
    (Duration::from_millis(100), "0.1"),
    (Duration::from_millis(250), "0.25"),
    (Duration::from_millis(500), "0.5"),
    (Duration::from_secs(1), "1"),
    (Duration::from_millis(2_500), "2.5"),
    (Duration::from_secs(5), "5"),
    (Duration::from_secs(10), "10"),
];

/// Packed `Duration`-only mirror of [`LATENCY_BUCKETS`] so `AtomicHistogram::record`
/// scans 176 bytes instead of interleaving `&str` pointers on the hot path.
pub(crate) const LATENCY_BUCKET_BOUNDS: [Duration; LATENCY_BUCKETS.len()] = {
    assert!(!LATENCY_BUCKETS.is_empty());
    let mut bounds = [Duration::ZERO; LATENCY_BUCKETS.len()];
    let mut i = 0;
    while i < LATENCY_BUCKETS.len() {
        bounds[i] = LATENCY_BUCKETS[i].0;
        if i > 0 {
            assert!(bounds[i - 1].as_nanos() < bounds[i].as_nanos());
        }
        i += 1;
    }
    bounds
};

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ConnProtocol {
    Tcp = 0,
    // Must remain last; `Self::COUNT` is derived from this variant.
    Ws = 1,
}

impl ConnProtocol {
    const COUNT: usize = Self::Ws as usize + 1;
    const ALL: [Self; Self::COUNT] = [Self::Tcp, Self::Ws];

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Ws => "ws",
        }
    }
}

#[repr(align(64))]
struct ActiveSlot {
    active: AtomicI64,
    seen: AtomicBool,
}

impl ActiveSlot {
    const fn new() -> Self {
        Self {
            active: AtomicI64::new(0),
            seen: AtomicBool::new(false),
        }
    }

    fn mark_seen(&self) {
        if !self.seen.load(Ordering::Relaxed) {
            self.seen.store(true, Ordering::Release);
        }
    }

    fn load(&self) -> Option<i64> {
        self.seen
            .load(Ordering::Acquire)
            .then(|| self.active.load(Ordering::Relaxed))
    }
}

/// Unarmed Prometheus + OpenTelemetry active-connection handle.
///
/// Captured before `tokio::spawn` for WebSocket relays so sessions activate the `ws`
/// gauge only after `on_upgrade` succeeds without retaining `Arc<AppState>`.
#[must_use]
pub(crate) struct ActiveGauge {
    metrics: Arc<ServerMetrics>,
    proto: ConnProtocol,
}

impl ActiveGauge {
    pub(crate) fn activate(self) -> ActiveConnectionGuard {
        let slot = &self.metrics.active[self.proto as usize];
        slot.active.fetch_add(1, Ordering::Relaxed);
        slot.mark_seen();
        self.metrics.otel.add_active(self.proto, 1);
        ActiveConnectionGuard(self)
    }
}

/// Armed active-connection guard that decrements both gauges on drop.
#[must_use]
pub(crate) struct ActiveConnectionGuard(ActiveGauge);

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        self.0.metrics.active[self.0.proto as usize]
            .active
            .fetch_sub(1, Ordering::Relaxed);
        self.0.metrics.otel.add_active(self.0.proto, -1);
    }
}

struct AtomicGauge {
    bits: AtomicU64,
    set: AtomicBool,
}

impl AtomicGauge {
    const fn new() -> Self {
        Self {
            bits: AtomicU64::new(0),
            set: AtomicBool::new(false),
        }
    }

    fn set(&self, value: f64) {
        self.bits.store(value.to_bits(), Ordering::Relaxed);
        if !self.set.load(Ordering::Relaxed) {
            self.set.store(true, Ordering::Release);
        }
    }

    fn load(&self) -> Option<f64> {
        self.set
            .load(Ordering::Acquire)
            .then(|| f64::from_bits(self.bits.load(Ordering::Relaxed)))
    }
}

/// Lock-free explicit-bucket histogram storing a wait-free microsecond sum and
/// non-cumulative per-bucket counts so each sample touches two atomics without CAS loops
/// and every scrape emits a monotonically non-decreasing cumulative snapshot
/// (`b[0] <= ... <= b[+Inf] == _count`).
///
/// `#[repr(C, align(64))]` places `sum_micros` (8B) and the seven sub-second hot buckets
/// (`<= 5ms` through `<= 500ms`, 56B) on the first 64-byte cache line so typical RPC
/// requests dirty only one cache line instead of two.
#[repr(C, align(64))]
struct AtomicHistogram {
    sum_micros: AtomicU64,
    buckets: [AtomicU64; LATENCY_BUCKET_BOUNDS.len() + 1],
}

const _: () = {
    assert!(std::mem::size_of::<AtomicHistogram>() == 128);
    assert!(std::mem::offset_of!(AtomicHistogram, sum_micros) == 0);
    assert!(
        std::mem::offset_of!(AtomicHistogram, buckets) + 7 * std::mem::size_of::<AtomicU64>() == 64
    );
};

impl AtomicHistogram {
    const fn new() -> Self {
        Self {
            sum_micros: AtomicU64::new(0),
            buckets: [const { AtomicU64::new(0) }; LATENCY_BUCKET_BOUNDS.len() + 1],
        }
    }

    fn record(&self, elapsed: Duration) {
        let micros = elapsed
            .as_secs()
            .saturating_mul(1_000_000)
            .saturating_add(u64::from(elapsed.subsec_nanos() + 500) / 1_000);
        if micros > 0 {
            self.sum_micros.fetch_add(micros, Ordering::Relaxed);
        }
        let idx = LATENCY_BUCKET_BOUNDS
            .iter()
            .position(|&bound| elapsed <= bound)
            .unwrap_or(LATENCY_BUCKET_BOUNDS.len());
        self.buckets[idx].fetch_add(1, Ordering::Release);
    }

    fn snapshot(&self) -> Option<([u64; LATENCY_BUCKET_BOUNDS.len() + 1], u64, f64)> {
        let mut counts = [0u64; LATENCY_BUCKET_BOUNDS.len() + 1];
        let mut total = 0u64;
        for (dst, src) in counts.iter_mut().zip(&self.buckets) {
            let n = src.load(Ordering::Acquire);
            *dst = n;
            total = total.saturating_add(n);
        }
        if total == 0 {
            return None;
        }
        let sum = self.sum_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        Some((counts, total, sum))
    }
}

fn write_type(out: &mut String, name: &str, kind: &str) {
    if out.is_empty() {
        out.reserve(5120);
    }
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

fn write_type_once(out: &mut String, wrote: &mut bool, name: &str, kind: &str) {
    if !*wrote {
        write_type(out, name, kind);
        *wrote = true;
    }
}

fn write_prom_f64(out: &mut String, val: f64) {
    if val.is_nan() {
        out.push_str("NaN");
    } else if val == f64::INFINITY {
        out.push_str("+Inf");
    } else if val == f64::NEG_INFINITY {
        out.push_str("-Inf");
    } else {
        let normalized = if val == 0.0 { 0.0 } else { val };
        let _ = write!(out, "{normalized}");
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UpstreamLayer {
    El = 0,
    // Must remain last; `Self::COUNT` is derived from this variant.
    Cl = 1,
}

impl UpstreamLayer {
    const COUNT: usize = Self::Cl as usize + 1;
    const ALL: [Self; Self::COUNT] = [Self::El, Self::Cl];

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::El => "EL",
            Self::Cl => "CL",
        }
    }

    pub(crate) const fn as_lower_str(self) -> &'static str {
        match self {
            Self::El => "el",
            Self::Cl => "cl",
        }
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MethodBucket {
    Post = 0,
    Get,
    Connect,
    Head,
    Put,
    Delete,
    Options,
    Patch,
    Trace,
    // Must remain last; `Self::COUNT` is derived from this variant.
    Other,
}

impl MethodBucket {
    const COUNT: usize = Self::Other as usize + 1;
    const ALL: [Self; Self::COUNT] = [
        Self::Post,
        Self::Get,
        Self::Connect,
        Self::Head,
        Self::Put,
        Self::Delete,
        Self::Options,
        Self::Patch,
        Self::Trace,
        Self::Other,
    ];

    pub(crate) fn classify(m: &http::Method) -> Self {
        match *m {
            http::Method::POST => Self::Post,
            http::Method::GET => Self::Get,
            http::Method::CONNECT => Self::Connect,
            http::Method::HEAD => Self::Head,
            http::Method::PUT => Self::Put,
            http::Method::DELETE => Self::Delete,
            http::Method::OPTIONS => Self::Options,
            http::Method::PATCH => Self::Patch,
            http::Method::TRACE => Self::Trace,
            _ => Self::Other,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Post => "POST",
            Self::Get => "GET",
            Self::Connect => "CONNECT",
            Self::Head => "HEAD",
            Self::Put => "PUT",
            Self::Delete => "DELETE",
            Self::Options => "OPTIONS",
            Self::Patch => "PATCH",
            Self::Trace => "TRACE",
            Self::Other => "OTHER",
        }
    }
}

macro_rules! define_tracked_statuses {
    ($($code:literal),+ $(,)?) => {
        /// Status codes that get their own `ethonyx_proxy_requests_total` series (`58` bytes,
        /// strictly sorted for `binary_search`). Every other code shares the trailing
        /// `status="OTHER"` slot, which bounds the label set.
        const TRACKED_STATUS: &[u16] = &[$($code),+];
        const STATUS_SLOTS: usize = TRACKED_STATUS.len() + 1;
        const STATUS_LABELS: [&str; STATUS_SLOTS] = [$(stringify!($code)),+, "OTHER"];
    };
}

define_tracked_statuses![
    101, 200, 201, 202, 204, 206, 301, 302, 304, 307, 308, 400, 401, 403, 404, 405, 408, 409, 413,
    414, 415, 417, 422, 429, 500, 501, 502, 503, 504,
];

const _: () = {
    assert!(STATUS_SLOTS <= u8::MAX as usize);
    let mut i = 1;
    while i < TRACKED_STATUS.len() {
        assert!(TRACKED_STATUS[i - 1] < TRACKED_STATUS[i]);
        i += 1;
    }
};

/// Bounded index into `STATUS_LABELS` (`0..STATUS_SLOTS`), guaranteeing in-bounds
/// table indexing across `ServerMetrics` and `OtelMetrics` by construction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StatusSlot(u8);

impl StatusSlot {
    fn classify(status: u16) -> Self {
        let idx = TRACKED_STATUS
            .binary_search(&status)
            .unwrap_or(TRACKED_STATUS.len());
        Self(idx as u8)
    }

    const fn index(self) -> usize {
        self.0 as usize
    }

    #[cfg(feature = "otel")]
    pub(crate) const fn as_str(self) -> &'static str {
        STATUS_LABELS[self.0 as usize]
    }
}

// `render_prometheus` pairs each `ALL` array with its table rows by position while
// writers index by discriminant, so every `ALL` must list variants in discriminant order.
const _: () = {
    let mut i = 0;
    while i < ConnProtocol::COUNT {
        assert!(ConnProtocol::ALL[i] as usize == i);
        i += 1;
    }
    let mut i = 0;
    while i < UpstreamLayer::COUNT {
        assert!(UpstreamLayer::ALL[i] as usize == i);
        i += 1;
    }
    let mut i = 0;
    while i < MethodBucket::COUNT {
        assert!(MethodBucket::ALL[i] as usize == i);
        i += 1;
    }
    let mut i = 0;
    while i < HealthInstrument::COUNT {
        assert!(HealthInstrument::ALL[i] as usize == i);
        i += 1;
    }
};

/// Per-server lock-free atomic metric registry and typed [`OtelMetrics`] fanout.
///
/// Each [`crate::Bound`] server owns its own isolated atomic counters, gauges, and
/// explicit-bucket histograms (with zero background upkeep timers, zero virtual
/// dispatch, and zero per-series heap allocations on steady-state traffic), plus an
/// [`OtelMetrics`] mirror (zero-sized when `otel` is disabled).
///
/// Ordered cold-to-hot under `#[repr(C)]` with 64-byte-aligned hot slots so
/// `ArcInner<ServerMetrics>.strong` (updated by connection guards and streaming
/// bodies) and each active/histogram lane occupy distinct cache lines.
#[repr(C)]
pub(crate) struct ServerMetrics {
    otel: OtelMetrics,
    saturated: AtomicU64,
    health: [AtomicGauge; HealthGauge::COUNT],
    active: [ActiveSlot; ConnProtocol::COUNT],
    duration: [AtomicHistogram; UpstreamLayer::COUNT],
    /// `(UpstreamLayer, MethodBucket, StatusSlot)` table (`2 × 10 × 30 = 600` slots,
    /// `4.8 KB` inline inside `Arc<ServerMetrics>`); untouched zero slots are omitted from `/metrics`.
    requests: [[[AtomicU64; STATUS_SLOTS]; MethodBucket::COUNT]; UpstreamLayer::COUNT],
}

const _: () = assert!(
    std::mem::align_of::<ServerMetrics>() >= 64
        && std::mem::size_of::<ActiveSlot>() == 64
        && std::mem::size_of::<AtomicHistogram>() == 128
);

impl ServerMetrics {
    pub(crate) fn new(otel: OtelMetrics) -> Arc<Self> {
        Arc::new(Self {
            otel,
            saturated: AtomicU64::new(0),
            health: [const { AtomicGauge::new() }; HealthGauge::COUNT],
            active: [const { ActiveSlot::new() }; ConnProtocol::COUNT],
            duration: [const { AtomicHistogram::new() }; UpstreamLayer::COUNT],
            requests: [const {
                [const { [const { AtomicU64::new(0) }; STATUS_SLOTS] }; MethodBucket::COUNT]
            }; UpstreamLayer::COUNT],
        })
    }

    #[cfg(test)]
    pub(crate) fn disabled() -> Arc<Self> {
        Self::new(OtelMetrics::disabled())
    }

    /// Render every touched metric family in Prometheus text exposition format (`0.0.4`).
    pub(crate) fn render_prometheus(&self) -> String {
        let mut out = String::new();

        let mut wrote_active = false;
        for (proto, slot) in ConnProtocol::ALL.into_iter().zip(&self.active) {
            if let Some(val) = slot.load() {
                write_type_once(&mut out, &mut wrote_active, ACTIVE_CONNECTIONS, "gauge");
                let _ = writeln!(
                    out,
                    "{ACTIVE_CONNECTIONS}{{protocol=\"{}\"}} {val}",
                    proto.as_str()
                );
            }
        }

        let saturated = self.saturated.load(Ordering::Relaxed);
        if saturated > 0 {
            write_type(&mut out, CONNECTION_LIMIT_SATURATED, "counter");
            let _ = writeln!(out, "{CONNECTION_LIMIT_SATURATED} {saturated}");
        }

        let mut wrote_duration = false;
        for (layer, hist) in UpstreamLayer::ALL.into_iter().zip(&self.duration) {
            let Some((counts, total, sum)) = hist.snapshot() else {
                continue;
            };
            write_type_once(
                &mut out,
                &mut wrote_duration,
                PROXY_REQUEST_DURATION,
                "histogram",
            );
            let upstream = layer.as_str();
            let mut cumulative = 0u64;
            for (&(_, le), &count) in LATENCY_BUCKETS.iter().zip(&counts) {
                cumulative = cumulative.saturating_add(count);
                let _ = writeln!(
                    out,
                    "{PROXY_REQUEST_DURATION}_bucket{{upstream=\"{upstream}\",le=\"{le}\"}} {cumulative}"
                );
            }
            let _ = writeln!(
                out,
                "{PROXY_REQUEST_DURATION}_bucket{{upstream=\"{upstream}\",le=\"+Inf\"}} {total}"
            );
            let _ = write!(
                out,
                "{PROXY_REQUEST_DURATION}_sum{{upstream=\"{upstream}\"}} "
            );
            write_prom_f64(&mut out, sum);
            out.push('\n');
            let _ = writeln!(
                out,
                "{PROXY_REQUEST_DURATION}_count{{upstream=\"{upstream}\"}} {total}"
            );
        }

        let mut wrote_requests = false;
        for (layer, methods) in UpstreamLayer::ALL.into_iter().zip(&self.requests) {
            let upstream = layer.as_str();
            for (method, statuses) in MethodBucket::ALL.into_iter().zip(methods) {
                let method_str = method.as_str();
                for (&status_str, counter) in STATUS_LABELS.iter().zip(statuses) {
                    let count = counter.load(Ordering::Relaxed);
                    if count == 0 {
                        continue;
                    }
                    write_type_once(
                        &mut out,
                        &mut wrote_requests,
                        PROXY_REQUESTS_TOTAL,
                        "counter",
                    );
                    let _ = writeln!(
                        out,
                        "{PROXY_REQUESTS_TOTAL}{{upstream=\"{upstream}\",method=\"{method_str}\",status=\"{status_str}\"}} {count}"
                    );
                }
            }
        }

        for instrument in HealthInstrument::ALL {
            match instrument.slots() {
                HealthSlots::Layered { el, cl } => {
                    let name = instrument.name();
                    let mut wrote = false;
                    for (layer, gauge) in [(UpstreamLayer::El, el), (UpstreamLayer::Cl, cl)] {
                        if let Some(val) = self.health[gauge as usize].load() {
                            write_type_once(&mut out, &mut wrote, name, "gauge");
                            let _ = write!(out, "{name}{{layer=\"{}\"}} ", layer.as_lower_str());
                            write_prom_f64(&mut out, val);
                            out.push('\n');
                        }
                    }
                }
                HealthSlots::Single(gauge) => {
                    if let Some(val) = self.health[gauge as usize].load() {
                        let name = instrument.name();
                        write_type(&mut out, name, "gauge");
                        let _ = write!(out, "{name} ");
                        write_prom_f64(&mut out, val);
                        out.push('\n');
                    }
                }
            }
        }

        out
    }

    pub(crate) fn active_tcp(self: &Arc<Self>) -> ActiveConnectionGuard {
        ActiveGauge {
            metrics: Arc::clone(self),
            proto: ConnProtocol::Tcp,
        }
        .activate()
    }

    pub(crate) fn ws_slot(self: &Arc<Self>) -> ActiveGauge {
        self.active[ConnProtocol::Ws as usize].mark_seen();
        ActiveGauge {
            metrics: Arc::clone(self),
            proto: ConnProtocol::Ws,
        }
    }

    pub(crate) fn inc_saturated(&self) {
        self.saturated.fetch_add(1, Ordering::Relaxed);
        self.otel.inc_saturated();
    }

    pub(crate) fn record_request(
        &self,
        layer: UpstreamLayer,
        method: MethodBucket,
        status: u16,
        elapsed: Duration,
    ) {
        self.duration[layer as usize].record(elapsed);
        let status = StatusSlot::classify(status);
        self.requests[layer as usize][method as usize][status.index()]
            .fetch_add(1, Ordering::Relaxed);
        self.otel.record_request(layer, method, status, elapsed);
    }

    /// Set one health gauge. Unset gauges (a disabled layer, or everything under
    /// `--trust-upstream`) remain omitted from `/metrics`.
    pub(crate) fn set_health(&self, gauge: HealthGauge, value: f64) {
        let (instrument, layer) = gauge.spec();
        self.health[gauge as usize].set(value);
        self.otel.set_health(instrument, layer, value);
    }
}

/// Distinct health gauge instruments shared between Prometheus and OpenTelemetry;
/// the only place their series names appear.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HealthInstrument {
    UpstreamHealth = 0,
    UpstreamPeers,
    UpstreamSyncDistance,
    UpstreamBlockNumber,
    UpstreamSlotNumber,
    ReferenceHealth,
    ReferenceBlockNumber,
    // Must remain last; `Self::COUNT` is derived from this variant.
    ReferenceBlockLag,
}

#[derive(Clone, Copy)]
enum HealthSlots {
    Layered { el: HealthGauge, cl: HealthGauge },
    Single(HealthGauge),
}

impl HealthInstrument {
    pub(crate) const COUNT: usize = Self::ReferenceBlockLag as usize + 1;
    const ALL: [Self; Self::COUNT] = [
        Self::UpstreamHealth,
        Self::UpstreamPeers,
        Self::UpstreamSyncDistance,
        Self::UpstreamBlockNumber,
        Self::UpstreamSlotNumber,
        Self::ReferenceHealth,
        Self::ReferenceBlockNumber,
        Self::ReferenceBlockLag,
    ];

    const fn slots(self) -> HealthSlots {
        match self {
            Self::UpstreamHealth => HealthSlots::Layered {
                el: HealthGauge::ElHealth,
                cl: HealthGauge::ClHealth,
            },
            Self::UpstreamPeers => HealthSlots::Layered {
                el: HealthGauge::ElPeers,
                cl: HealthGauge::ClPeers,
            },
            Self::UpstreamSyncDistance => HealthSlots::Layered {
                el: HealthGauge::ElSyncDistance,
                cl: HealthGauge::ClSyncDistance,
            },
            Self::UpstreamBlockNumber => HealthSlots::Single(HealthGauge::ElBlockNumber),
            Self::UpstreamSlotNumber => HealthSlots::Single(HealthGauge::ClSlotNumber),
            Self::ReferenceHealth => HealthSlots::Single(HealthGauge::ReferenceHealth),
            Self::ReferenceBlockNumber => HealthSlots::Single(HealthGauge::ReferenceBlockNumber),
            Self::ReferenceBlockLag => HealthSlots::Single(HealthGauge::ReferenceBlockLag),
        }
    }

    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::UpstreamHealth => "ethonyx_upstream_health_status",
            Self::UpstreamPeers => "ethonyx_upstream_peers",
            Self::UpstreamSyncDistance => "ethonyx_upstream_sync_distance",
            Self::UpstreamBlockNumber => "ethonyx_upstream_block_number",
            Self::UpstreamSlotNumber => "ethonyx_upstream_slot_number",
            Self::ReferenceHealth => "ethonyx_reference_health_status",
            Self::ReferenceBlockNumber => "ethonyx_reference_block_number",
            Self::ReferenceBlockLag => "ethonyx_reference_block_lag",
        }
    }
}

/// Every gauge series the health poller sets (EL/CL share 3 layered instruments).
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HealthGauge {
    ElHealth = 0,
    ElPeers,
    ElSyncDistance,
    ElBlockNumber,
    ClHealth,
    ClPeers,
    ClSyncDistance,
    ClSlotNumber,
    ReferenceHealth,
    ReferenceBlockNumber,
    // Must remain last; `Self::COUNT` is derived from this variant.
    ReferenceBlockLag,
}

/// Inverse table built at compile time from [`HealthInstrument::slots`], proving
/// a 1:1 bijection with every [`HealthGauge`] variant.
const HEALTH_SPECS: [(HealthInstrument, Option<UpstreamLayer>); HealthGauge::COUNT] = {
    let mut specs = [(HealthInstrument::UpstreamHealth, None); HealthGauge::COUNT];
    let mut seen = [false; HealthGauge::COUNT];
    let mut i = 0;
    while i < HealthInstrument::ALL.len() {
        let instrument = HealthInstrument::ALL[i];
        match instrument.slots() {
            HealthSlots::Layered { el, cl } => {
                let el_idx = el as usize;
                let cl_idx = cl as usize;
                assert!(!seen[el_idx] && !seen[cl_idx]);
                seen[el_idx] = true;
                seen[cl_idx] = true;
                specs[el_idx] = (instrument, Some(UpstreamLayer::El));
                specs[cl_idx] = (instrument, Some(UpstreamLayer::Cl));
            }
            HealthSlots::Single(gauge) => {
                let idx = gauge as usize;
                assert!(!seen[idx]);
                seen[idx] = true;
                specs[idx] = (instrument, None);
            }
        }
        i += 1;
    }
    let mut j = 0;
    while j < seen.len() {
        assert!(seen[j]);
        j += 1;
    }
    specs
};

impl HealthGauge {
    const COUNT: usize = Self::ReferenceBlockLag as usize + 1;

    const fn spec(self) -> (HealthInstrument, Option<UpstreamLayer>) {
        HEALTH_SPECS[self as usize]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Method;

    #[test]
    fn method_buckets_cover_every_label() {
        let methods = [
            (Method::POST, MethodBucket::Post, "POST"),
            (Method::GET, MethodBucket::Get, "GET"),
            (Method::CONNECT, MethodBucket::Connect, "CONNECT"),
            (Method::HEAD, MethodBucket::Head, "HEAD"),
            (Method::PUT, MethodBucket::Put, "PUT"),
            (Method::DELETE, MethodBucket::Delete, "DELETE"),
            (Method::OPTIONS, MethodBucket::Options, "OPTIONS"),
            (Method::PATCH, MethodBucket::Patch, "PATCH"),
            (Method::TRACE, MethodBucket::Trace, "TRACE"),
            (
                Method::from_bytes(b"PURGE").unwrap(),
                MethodBucket::Other,
                "OTHER",
            ),
        ];
        assert_eq!(methods.len(), MethodBucket::COUNT);
        for (method, expected, label) in &methods {
            let bucket = MethodBucket::classify(method);
            assert_eq!(bucket, *expected);
            assert_eq!(bucket.as_str(), *label);
        }
    }

    #[test]
    fn latency_bucket_labels_match_bounds_and_histogram_is_monotonic() {
        for (bound, label) in LATENCY_BUCKETS {
            assert_eq!(label, bound.as_secs_f64().to_string());
        }

        let hist = AtomicHistogram::new();
        assert!(hist.snapshot().is_none());

        // Exact lower boundary (`5ms`), upper boundary (`10s`), zero, and overflow (`15s`).
        for ms in [0, 5, 250, 10_000, 15_000] {
            hist.record(Duration::from_millis(ms));
        }
        let (counts, total, sum) = hist.snapshot().unwrap();
        assert_eq!(total, 5);
        assert_eq!(sum, 25.255);
        // Non-cumulative bucket counts: [0.005]=2, [0.25]=1, [10]=1, [+Inf]=1.
        assert_eq!(counts[0], 2);
        assert_eq!(counts[5], 1);
        assert_eq!(counts[10], 1);
        assert_eq!(counts[11], 1);

        // Sub-microsecond rounding (`499ns -> 0us`, `500ns -> 1us`, `999_999_500ns -> 1s` carry)
        // and saturation on the `u64::MAX` microsecond boundary and `Duration::MAX`.
        let rounding = AtomicHistogram::new();
        rounding.record(Duration::from_nanos(499));
        rounding.record(Duration::from_nanos(500));
        rounding.record(Duration::new(0, 999_999_500));
        let (_, r_total, r_sum) = rounding.snapshot().unwrap();
        assert_eq!(r_total, 3);
        assert_eq!(r_sum, 1.000_001);

        let exact_max = AtomicHistogram::new();
        exact_max.record(Duration::new(u64::MAX / 1_000_000, 551_615_499));
        assert_eq!(exact_max.sum_micros.load(Ordering::Relaxed), u64::MAX);

        let sat = AtomicHistogram::new();
        sat.record(Duration::new(u64::MAX / 1_000_000, 551_615_500));
        assert_eq!(sat.sum_micros.load(Ordering::Relaxed), u64::MAX);

        let max_dur = AtomicHistogram::new();
        max_dur.record(Duration::MAX);
        let (s_counts, s_total, s_sum) = max_dur.snapshot().unwrap();
        assert_eq!(s_total, 1);
        assert_eq!(s_counts[LATENCY_BUCKET_BOUNDS.len()], 1);
        assert_eq!(max_dur.sum_micros.load(Ordering::Relaxed), u64::MAX);
        assert!(s_sum.is_finite());

        let m = ServerMetrics::disabled();
        assert_eq!(m.render_prometheus().capacity(), 0);
        for ms in [0, 5, 250, 10_000, 15_000] {
            m.record_request(
                UpstreamLayer::El,
                MethodBucket::Post,
                200,
                Duration::from_millis(ms),
            );
        }
        let rendered = m.render_prometheus();
        let prefix = r#"ethonyx_proxy_request_duration_seconds_bucket{upstream="EL",le=""#;
        let bucket_values: Vec<u64> = rendered
            .lines()
            .filter_map(|line| {
                let rest = line.strip_prefix(prefix)?;
                let (_, val) = rest.split_once("\"} ")?;
                val.parse::<u64>().ok()
            })
            .collect();
        assert_eq!(
            bucket_values,
            [2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 4, 5],
            "cumulative buckets must be monotonically non-decreasing, got:\n{rendered}"
        );
        assert!(
            rendered.contains("ethonyx_proxy_request_duration_seconds_count{upstream=\"EL\"} 5\n")
        );
        assert!(
            rendered
                .contains("ethonyx_proxy_request_duration_seconds_sum{upstream=\"EL\"} 25.255\n"),
            "exact decimal sum expected without f64 CAS drift, got:\n{rendered}"
        );
    }

    #[test]
    fn prometheus_format_handles_special_floats_and_never_duplicates_type_headers() {
        let mut buf = String::new();
        for (input, expected) in [
            (0.0, "0"),
            (-0.0, "0"),
            (42.0, "42"),
            (0.005, "0.005"),
            (f64::NAN, "NaN"),
            (f64::INFINITY, "+Inf"),
            (f64::NEG_INFINITY, "-Inf"),
        ] {
            buf.clear();
            write_prom_f64(&mut buf, input);
            assert_eq!(buf, expected);
        }

        let m = ServerMetrics::disabled();
        let _tcp = m.active_tcp();
        let _ws = m.ws_slot().activate();
        m.inc_saturated();
        m.record_request(
            UpstreamLayer::El,
            MethodBucket::Post,
            200,
            Duration::from_millis(5),
        );
        m.record_request(
            UpstreamLayer::Cl,
            MethodBucket::Get,
            200,
            Duration::from_millis(10),
        );
        for (gauge, val) in [
            (HealthGauge::ElHealth, 1.0),
            (HealthGauge::ClHealth, 1.0),
            (HealthGauge::ElPeers, 12.0),
            (HealthGauge::ClPeers, 34.0),
            (HealthGauge::ElSyncDistance, 0.0),
            (HealthGauge::ClSyncDistance, 2.0),
            (HealthGauge::ElBlockNumber, 21_000_000.0),
            (HealthGauge::ClSlotNumber, 9_400_000.0),
            (HealthGauge::ReferenceHealth, 1.0),
            (HealthGauge::ReferenceBlockNumber, 21_000_001.0),
            (HealthGauge::ReferenceBlockLag, 1.0),
        ] {
            m.set_health(gauge, val);
        }

        let rendered = m.render_prometheus();
        assert!(rendered.ends_with('\n'));
        let mut seen_types = std::collections::HashSet::new();
        for line in rendered.lines() {
            if let Some(rest) = line.strip_prefix("# TYPE ") {
                let name = rest.split_whitespace().next().unwrap();
                assert!(
                    seen_types.insert(name),
                    "duplicate # TYPE header for {name} in:\n{rendered}"
                );
            }
        }
        assert_eq!(seen_types.len(), 4 + HealthInstrument::COUNT);
    }

    #[test]
    fn tracked_statuses_get_their_own_series_and_the_rest_share_other() {
        let tracked = [
            101, 200, 201, 202, 204, 206, 301, 302, 304, 307, 308, 400, 401, 403, 404, 405, 408,
            409, 413, 414, 415, 417, 422, 429, 500, 501, 502, 503, 504,
        ];
        assert_eq!(tracked.len(), TRACKED_STATUS.len());
        let m = ServerMetrics::disabled();
        for status in tracked.into_iter().chain([303, 418, 599]) {
            m.record_request(
                UpstreamLayer::Cl,
                MethodBucket::Other,
                status,
                Duration::ZERO,
            );
        }

        let rendered = m.render_prometheus();
        assert!(
            rendered.contains("# TYPE ethonyx_proxy_request_duration_seconds histogram"),
            "duration must export as explicit Prometheus histogram, got:\n{rendered}"
        );
        let bucket_prefix = r#"ethonyx_proxy_request_duration_seconds_bucket{upstream="CL",le=""#;
        let buckets: Vec<&str> = rendered
            .lines()
            .filter_map(|line| line.strip_prefix(bucket_prefix))
            .collect();
        let total = tracked.len() + 3;
        let expected_le = [
            "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "+Inf",
        ];
        assert_eq!(expected_le.len(), LATENCY_BUCKET_BOUNDS.len() + 1);
        let expected_buckets: Vec<String> = expected_le
            .iter()
            .map(|le| format!("{le}\"}} {total}"))
            .collect();
        assert_eq!(
            buckets, expected_buckets,
            "expected all 11 LATENCY_BUCKET_BOUNDS plus +Inf, got:\n{rendered}"
        );
        let prefix = r#"ethonyx_proxy_requests_total{upstream="CL",method="OTHER",status=""#;
        let mut series: Vec<&str> = rendered
            .lines()
            .filter_map(|line| line.strip_prefix(prefix))
            .collect();
        series.sort_unstable();
        let mut expected: Vec<String> = tracked
            .iter()
            .map(|status| format!("{status}\"}} 1"))
            .collect();
        expected.push("OTHER\"} 3".to_owned());
        assert_eq!(series, expected);
    }

    #[test]
    fn request_series_that_share_two_coordinates_stay_separate() {
        let m = ServerMetrics::disabled();
        for (layer, method, status) in [
            (UpstreamLayer::El, MethodBucket::Post, 200),
            (UpstreamLayer::Cl, MethodBucket::Post, 200),
            (UpstreamLayer::El, MethodBucket::Get, 200),
            (UpstreamLayer::El, MethodBucket::Post, 404),
            (UpstreamLayer::El, MethodBucket::Post, 418),
        ] {
            m.record_request(layer, method, status, Duration::ZERO);
        }

        let rendered = m.render_prometheus();
        let mut series: Vec<&str> = rendered
            .lines()
            .filter_map(|line| line.strip_prefix("ethonyx_proxy_requests_total"))
            .collect();
        series.sort_unstable();
        assert_eq!(
            series,
            [
                r#"{upstream="CL",method="POST",status="200"} 1"#,
                r#"{upstream="EL",method="GET",status="200"} 1"#,
                r#"{upstream="EL",method="POST",status="200"} 1"#,
                r#"{upstream="EL",method="POST",status="404"} 1"#,
                r#"{upstream="EL",method="POST",status="OTHER"} 1"#,
            ]
        );
    }

    #[test]
    fn server_metrics_instances_are_isolated_across_all_request_buckets() {
        let m1 = ServerMetrics::disabled();
        let m2 = ServerMetrics::disabled();

        let _tcp = m1.active_tcp();
        drop(m1.ws_slot().activate());
        m1.inc_saturated();
        m1.record_request(
            UpstreamLayer::El,
            MethodBucket::Post,
            200,
            Duration::from_millis(5),
        );
        m1.record_request(
            UpstreamLayer::Cl,
            MethodBucket::Options,
            414,
            Duration::from_millis(1),
        );
        m1.record_request(
            UpstreamLayer::Cl,
            MethodBucket::Get,
            206,
            Duration::from_millis(2),
        );
        m1.record_request(
            UpstreamLayer::El,
            MethodBucket::Delete,
            429,
            Duration::from_millis(3),
        );
        for (gauge, value) in [
            (HealthGauge::ElBlockNumber, 1234.0),
            (HealthGauge::ReferenceHealth, 1.0),
            (HealthGauge::ReferenceBlockNumber, 1235.0),
            (HealthGauge::ReferenceBlockLag, 1.0),
        ] {
            m1.set_health(gauge, value);
        }

        let r1 = m1.render_prometheus();
        assert!(r1.contains("ethonyx_connection_limit_saturated_total 1"));
        assert!(r1.contains("status=\"200\""));
        assert!(r1.contains("status=\"414\""));
        assert!(r1.contains("status=\"206\""));
        assert!(r1.contains("status=\"429\""));
        assert!(!r1.contains("status=\"500\""));
        assert!(r1.contains("ethonyx_upstream_block_number 1234"));
        assert!(r1.contains("ethonyx_reference_health_status 1"));
        assert!(r1.contains("ethonyx_reference_block_number 1235"));
        assert!(r1.contains("ethonyx_reference_block_lag 1"));

        let r2 = m2.render_prometheus();
        assert!(
            r2.is_empty(),
            "second ServerMetrics instance must not share series from m1, got:\n{r2}"
        );

        // A gauge that is never set is never registered.
        m2.set_health(HealthGauge::ElBlockNumber, 50.0);
        let r2_el_only = m2.render_prometheus();
        assert!(r2_el_only.contains("ethonyx_upstream_block_number 50"));
        assert!(!r2_el_only.contains("layer=\"cl\""));
        assert!(!r2_el_only.contains("ethonyx_upstream_slot_number"));
        assert!(!r2_el_only.contains("ethonyx_upstream_peers"));
        assert!(!r2_el_only.contains("ethonyx_reference_"));
    }

    #[test]
    fn active_connection_gauges_follow_their_guards() {
        let metrics = ServerMetrics::disabled();
        let active = |protocol: &str| {
            let prefix = format!("{ACTIVE_CONNECTIONS}{{protocol=\"{protocol}\"}} ");
            metrics
                .render_prometheus()
                .lines()
                .find_map(|line| line.strip_prefix(prefix.as_str()).map(str::to_owned))
        };
        let tcp = metrics.active_tcp();
        // Dropping an unarmed WS slot registers the series without counting or underflowing.
        let unarmed_ws = metrics.ws_slot();
        assert_eq!(active("tcp").as_deref(), Some("1"));
        assert_eq!(active("ws").as_deref(), Some("0"));
        drop(unarmed_ws);
        assert_eq!(active("ws").as_deref(), Some("0"));

        let ws = metrics.ws_slot().activate();
        let second_tcp = metrics.active_tcp();
        assert_eq!(active("tcp").as_deref(), Some("2"));
        assert_eq!(active("ws").as_deref(), Some("1"));
        drop((tcp, ws, second_tcp));
        assert_eq!(active("tcp").as_deref(), Some("0"));
        assert_eq!(active("ws").as_deref(), Some("0"));
    }
}
