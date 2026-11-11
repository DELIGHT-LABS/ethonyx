pub struct ActiveConnectionGuard(&'static str);

impl ActiveConnectionGuard {
    pub fn new(protocol: &'static str) -> Self {
        ::metrics::gauge!("ethonyx_active_connections", "protocol" => protocol).increment(1.0);
        Self(protocol)
    }
}

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        ::metrics::gauge!("ethonyx_active_connections", "protocol" => self.0).decrement(1.0);
    }
}
