//! Feature gated process barriers for integration crash tests.
#[cfg(not(test))]
pub(crate) fn checkpoint(name: &str) {
    if std::env::var("WI_CRASH_POINT").as_deref() == Ok(name) {
        if let Some(path) = std::env::var_os("WI_CRASH_READY") {
            std::fs::write(path, b"ready").expect("crash barrier ready marker");
        }
        // The optional release marker is for the deterministic supersession
        // test. It is compiled under the same debug-only crash feature.
        let release = std::env::var_os("WI_CRASH_RELEASE");
        loop {
            if release.as_ref().is_some_and(|path| std::path::Path::new(path).is_file()) {
                return;
            }
            std::thread::park_timeout(std::time::Duration::from_millis(10));
        }
    }
}
