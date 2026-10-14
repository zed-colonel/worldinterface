//! Feature gated process barriers for integration crash tests.
#[cfg(not(test))]
pub(crate) fn checkpoint(name: &str) {
    if std::env::var("WI_CRASH_POINT").as_deref() == Ok(name) {
        if let Some(path) = std::env::var_os("WI_CRASH_READY") {
            std::fs::write(path, b"ready").expect("crash barrier ready marker");
        }
        loop {
            std::thread::park();
        }
    }
}
