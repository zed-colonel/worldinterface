use super::{ChannelPolicy, Server};
use crate::Result;
use std::{
    io::{Read, Write},
    os::{fd::AsRawFd, unix::net::UnixStream},
    sync::Mutex,
    time::{Duration, Instant},
};
use worldinterface_worker::supervisor::*;
/// Anonymous Unix socket pair: no guest-discoverable address, bearer token or
/// client-chosen session. Pass the client endpoint only to trusted WI composition.
pub struct PrivateChannel {
    stream: UnixStream,
    policy: ChannelPolicy,
}
impl PrivateChannel {
    pub fn provision(policy: ChannelPolicy) -> Result<(Client, Self)> {
        if policy.grants.len() > 32 {
            return Err("supervisor grant capacity".into());
        }
        let (client, server) = UnixStream::pair()?;
        Ok((Client { stream: Mutex::new(client) }, Self { stream: server, policy }))
    }
    /// Poll an idle endpoint for up to one second. A frame gets a separate
    /// one-second budget starting at its first byte. Any I/O error closes the
    /// endpoint: a partial frame cannot be resumed on this channel. The host
    /// must provision a fresh authenticated channel and inspect the durable key;
    /// loss of a reply does not establish whether Submit was admitted.
    pub fn serve_one(&mut self, server: &Server, now_ms: u64) -> Result<()> {
        let result = self.serve_frame(server, now_ms);
        if result.is_err() {
            let _ = self.stream.shutdown(std::net::Shutdown::Both);
        }
        result
    }
    fn serve_frame(&mut self, server: &Server, now_ms: u64) -> Result<()> {
        let Some(bytes) = read_frame(&mut self.stream)? else {
            return Ok(());
        };
        let response = match decode::<Request>(&bytes)
            .map_err(Into::into)
            .and_then(|q| server.request(&self.policy, q, now_ms))
        {
            Ok(r) => r,
            Err(_) => Response::Unavailable,
        };
        write_frame(&mut self.stream, &encode(&response)?)
    }
    /// Trusted process-launch integration. UnixStream is close-on-exec by
    /// default; a consumer explicitly transfers only the supervisor endpoint.
    pub fn into_parts(self) -> (UnixStream, ChannelPolicy) {
        (self.stream, self.policy)
    }
    pub fn from_provisioned(stream: UnixStream, policy: ChannelPolicy) -> Self {
        Self { stream, policy }
    }
}
pub struct Client {
    stream: Mutex<UnixStream>,
}
impl Client {
    pub fn from_provisioned(stream: UnixStream) -> Self {
        Self { stream: Mutex::new(stream) }
    }
    pub fn request(&self, request: &Request) -> Result<Response> {
        let mut stream = self.stream.try_lock().map_err(|_| "supervisor channel busy")?;
        if let Err(error) = write_frame(&mut stream, &encode(request)?) {
            let _ = stream.shutdown(std::net::Shutdown::Both);
            return Err(error);
        }
        let response = read_frame(&mut stream).and_then(|b| {
            let bytes = b.ok_or("supervisor response deadline")?;
            Ok(decode(&bytes)?)
        });
        if response.is_err() {
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }
        response
    }
    pub fn into_stream(self) -> Result<UnixStream> {
        Ok(self.stream.into_inner().map_err(|_| "supervisor channel poisoned")?)
    }
}
fn remaining(start: Instant) -> Result<Duration> {
    Duration::from_millis(1000)
        .checked_sub(start.elapsed())
        .filter(|d| !d.is_zero())
        .ok_or_else(|| "supervisor frame deadline".into())
}
fn read_exact(stream: &mut UnixStream, mut bytes: &mut [u8], start: Instant) -> Result<()> {
    while !bytes.is_empty() {
        // Drain buffered bytes before checking the deadline. MSG_DONTWAIT is
        // per-call, so it cannot change flags on a transferred/duplicated fd.
        // SAFETY: bytes is a live writable slice and stream owns this socket.
        let buffered = unsafe {
            libc::recv(
                stream.as_raw_fd(),
                bytes.as_mut_ptr().cast(),
                bytes.len(),
                libc::MSG_DONTWAIT,
            )
        };
        let read = if buffered >= 0 {
            buffered as usize
        } else {
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::WouldBlock {
                return Err(error.into());
            }
            stream.set_read_timeout(Some(remaining(start)?))?;
            stream.read(bytes)?
        };
        if read == 0 {
            return Err("closed supervisor channel".into());
        }
        bytes = &mut bytes[read..];
    }
    Ok(())
}
fn read_frame(stream: &mut UnixStream) -> Result<Option<Vec<u8>>> {
    let mut size = [0; 4];
    // Idle polling consumes no frame budget and no bytes on timeout.
    stream.set_read_timeout(Some(Duration::from_millis(1000)))?;
    match stream.read(&mut size[..1]) {
        Ok(0) => return Err("closed supervisor channel".into()),
        Ok(_) => {}
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            ) =>
        {
            return Ok(None)
        }
        Err(e) => return Err(e.into()),
    }
    let start = Instant::now();
    read_exact(stream, &mut size[1..], start)?;
    let len = u32::from_be_bytes(size) as usize;
    if len > MAX_FRAME_BYTES {
        let _ = stream.shutdown(std::net::Shutdown::Both);
        return Err("supervisor frame bound".into());
    }
    let mut bytes = vec![0; len];
    read_exact(stream, &mut bytes, start)?;
    Ok(Some(bytes))
}

fn write_frame(stream: &mut UnixStream, bytes: &[u8]) -> Result<()> {
    if bytes.len() > MAX_FRAME_BYTES {
        return Err("supervisor frame bound".into());
    }
    let start = Instant::now();
    let frame = [(bytes.len() as u32).to_be_bytes().as_slice(), bytes].concat();
    let mut pending = frame.as_slice();
    while !pending.is_empty() {
        stream.set_write_timeout(Some(remaining(start)?))?;
        let written = stream.write(pending)?;
        if written == 0 {
            return Err("closed supervisor channel".into());
        }
        pending = &pending[written..];
    }
    Ok(())
}

#[cfg(test)]
#[test]
fn buffered_frame_bytes_are_read_even_when_reader_wakes_after_deadline() {
    let (mut sender, mut reader) = UnixStream::pair().unwrap();
    sender.write_all(b"buffered").unwrap();
    let expired = Instant::now() - Duration::from_secs(2);
    let mut bytes = [0; 8];
    read_exact(&mut reader, &mut bytes, expired).unwrap();
    assert_eq!(&bytes, b"buffered");
    // Buffered progress must not renew an expired budget for missing bytes.
    assert!(read_exact(&mut reader, &mut bytes, expired).is_err());
}
