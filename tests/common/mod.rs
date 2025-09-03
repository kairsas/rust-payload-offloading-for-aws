use std::sync::Once;

use tracing_subscriber::EnvFilter;

static INIT: Once = Once::new();

pub fn init() {
    INIT.call_once(setup_tracing);
}

pub fn setup_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_thread_names(true)
        .with_ansi(true)
        .init();
}
