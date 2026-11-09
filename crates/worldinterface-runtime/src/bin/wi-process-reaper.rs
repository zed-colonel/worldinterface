#[path = "../worker_process/mod.rs"]
pub mod worker_process;
fn main() {
    if worker_process::helper_main().is_err() {
        std::process::exit(1);
    }
}
