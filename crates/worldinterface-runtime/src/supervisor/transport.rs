use super::{ChannelPolicy, Server};
use crate::Result;
use std::{
    io::{Read, Write},
    os::unix::net::UnixStream,
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
    pub fn serve_one(&mut self, server: &Server, now_ms: u64) -> Result<()> {
        let bytes = read_frame(&mut self.stream)?;
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
        let response = read_frame(&mut stream).and_then(|b| Ok(decode(&b)?));
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
        stream.set_read_timeout(Some(remaining(start)?))?;
        let read = stream.read(bytes)?;
        if read == 0 {
            return Err("closed supervisor channel".into());
        }
        bytes = &mut bytes[read..];
    }
    Ok(())
}
fn read_frame(stream: &mut UnixStream) -> Result<Vec<u8>> {
    let start = Instant::now();
    let mut size = [0; 4];
    read_exact(stream, &mut size, start)?;
    let len = u32::from_be_bytes(size) as usize;
    if len > MAX_FRAME_BYTES {
        let _ = stream.shutdown(std::net::Shutdown::Both);
        return Err("supervisor frame bound".into());
    }
    let mut bytes = vec![0; len];
    read_exact(stream, &mut bytes, start)?;
    Ok(bytes)
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
