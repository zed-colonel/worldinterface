//! WI-owned reviewed fixtures. No shell, network access, or external consumer code.
use super::{
    bootstrap::{self, FixtureLaunch, Message},
    io, Result,
};
use std::{
    fs,
    io::Write,
    os::fd::FromRawFd,
    os::unix::net::UnixStream,
    time::{Duration, Instant},
};
pub(super) fn run() -> Result<()> {
    let mut socket = unsafe { UnixStream::from_raw_fd(3) };
    let launch: FixtureLaunch = bootstrap::receive(&mut socket)?;
    bootstrap::send(
        &mut socket,
        &Message::new(
            worldinterface_protocol::ContentHash::of(launch.token.as_bytes()).as_str(),
            "ready",
        ),
    )?;
    let go: Message = bootstrap::receive(&mut socket)?;
    if go.nonce != launch.handshake || go.kind != "go" {
        return Err("fixture ownership handshake".into());
    }
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args != launch.task.args() {
        return Err("fixture argument drift".into());
    }
    match launch.task {
        super::admission::Task::Coding => {
            let path = "sample-rust/src/lib.rs";
            let before = io(fs::read_to_string(path))?;
            let after = before.replace("averge", "average");
            if before == after {
                return Err("sample task precondition".into());
            }
            io(fs::write(path, &after))?;
            io(fs::write("../export/change.patch", coding_patch()))?;
            io(fs::write("../export/report.json", coding_report()))?;
        }
        super::admission::Task::Transform { xor } => {
            let bytes = io(fs::read("../input/bytes"))?;
            if bytes.len() > 1024 * 1024 {
                return Err("fixture input bound".into());
            }
            io(fs::write(
                "../export/output.bin",
                bytes.into_iter().map(|b| b ^ xor).collect::<Vec<_>>(),
            ))?;
        }
        super::admission::Task::Probe => {
            let clean = super::launch::environment_and_descriptors_clean(3)?;
            io(fs::write(
                "../export/probe.json",
                if clean {
                    b"{\"clean\":true}".as_slice()
                } else {
                    b"{\"clean\":false}".as_slice()
                },
            ))?;
        }
        super::admission::Task::Sleep { millis } => {
            std::thread::sleep(Duration::from_millis(millis))
        }
        super::admission::Task::Flood { bytes } => {
            // Two separate children flood both streams concurrently; each inherits open pipes.
            let pid = unsafe { libc::fork() };
            if pid < 0 {
                return Err("fixture fork failed".into());
            }
            let data = [b'x'; 4096];
            let mut count = 0;
            while count < bytes {
                let n = (bytes - count).min(data.len() as u64) as usize;
                if pid == 0 {
                    io(std::io::stderr().write_all(&data[..n]))?;
                } else {
                    io(std::io::stdout().write_all(&data[..n]))?;
                }
                count += n as u64;
            }
            if pid == 0 {
                unsafe { libc::_exit(0) }
            }
            std::thread::sleep(Duration::from_secs(30));
        }
        super::admission::Task::Tree { early_exit, change_session, resist_term } => {
            let child = unsafe { libc::fork() };
            if child < 0 {
                return Err("fixture fork failed".into());
            }
            if child == 0 {
                if change_session {
                    unsafe {
                        libc::setsid();
                    }
                }
                if resist_term {
                    unsafe {
                        libc::signal(libc::SIGTERM, libc::SIG_IGN);
                    }
                }
                let grandchild = unsafe { libc::fork() };
                if grandchild < 0 {
                    unsafe { libc::_exit(1) }
                }
                // Both child and grandchild keep inherited pipes open.
                let until = Instant::now() + Duration::from_secs(30);
                while Instant::now() < until {
                    std::thread::sleep(Duration::from_millis(10));
                }
                unsafe { libc::_exit(0) }
            }
            if !early_exit {
                std::thread::sleep(Duration::from_secs(30));
            }
        }
        super::admission::Task::Churn { children } => {
            // The fixture is the grandparent: its child reaps short-lived grandchildren.
            let parent = unsafe { libc::fork() };
            if parent < 0 {
                return Err("fixture fork failed".into());
            }
            let target = if parent == 0 {
                for _ in 0..children {
                    let child = unsafe { libc::fork() };
                    if child < 0 {
                        unsafe { libc::_exit(1) }
                    }
                    if child == 0 {
                        std::thread::sleep(Duration::from_millis(5));
                        unsafe { libc::_exit(0) }
                    }
                    let mut status = 0;
                    if unsafe { libc::waitpid(child, &mut status, 0) } != child || status != 0 {
                        unsafe { libc::_exit(1) }
                    }
                }
                unsafe { libc::_exit(0) }
            } else {
                parent
            };
            let mut status = 0;
            if unsafe { libc::waitpid(target, &mut status, 0) } != target || status != 0 {
                return Err("fixture churn failed".into());
            }
            io(fs::write("../export/churn.json", b"{\"reaped\":true}"))?;
        }
    }
    Ok(())
}
pub(super) fn coding_patch() -> &'static [u8] {
    b"--- a/src/lib.rs\n+++ b/src/lib.rs\n@@ -10,10 +10,10 @@\n     Some((key.trim(), value.trim()))\n }\n \n-/// Calculate the averge of a slice of numbers.\n+/// Calculate the average of a slice of numbers.\n ///\n /// Returns 0.0 for an empty slice.\n-pub fn calculate_averge(numbers: &[f64]) -> f64 {\n+pub fn calculate_average(numbers: &[f64]) -> f64 {\n     if numbers.is_empty() {\n         return 0.0;\n     }\n"
}
pub(super) fn coding_report() -> &'static [u8] {
    b"{\"changed\":[\"src/lib.rs\"],\"task\":\"rename-average\",\"schema\":1}"
}
/// Entry point for the separately reviewed fixture binary.
#[doc(hidden)]
pub fn fixture_main() -> Result<()> {
    run()
}
