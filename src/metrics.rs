use ::metrics::Gauge;

pub(crate) const ACTIVE_CONNECTIONS: &str = "ethonyx_active_connections";

pub(crate) struct ActiveConnectionGuard(Gauge);

impl ActiveConnectionGuard {
    pub(crate) fn tcp() -> Self {
        Self::new(::metrics::gauge!(ACTIVE_CONNECTIONS, "protocol" => "tcp"))
    }

    pub(crate) fn ws() -> Self {
        Self::new(::metrics::gauge!(ACTIVE_CONNECTIONS, "protocol" => "ws"))
    }

    fn new(gauge: Gauge) -> Self {
        gauge.increment(1.0);
        Self(gauge)
    }
}

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        self.0.decrement(1.0);
    }
}
