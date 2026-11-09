// Compile the reviewed adapter sources directly so the helper executable does not
// link the unrelated AQ runtime, store and connector implementation.
#[path = "../worker_process/mod.rs"]
pub mod worker_process;
fn main() {
    if worker_process::fixture_main().is_err() {
        std::process::exit(1);
    }
}
