//! Incremental framing: inspect the five-byte header before reserving a payload.
use super::{io, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    io::{Read, Write},
    os::unix::net::UnixStream,
    time::{Duration, Instant},
};
use worldinterface_worker::credential::CredentialBindingV1;

pub(super) const FRAME_LIMIT: usize = 32 * 1024;
const TRAFFIC_LIMIT: usize = 128 * 1024;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Launch {
    pub nonce: String,
    pub allocation: String,
    pub token: String,
    pub credential: CredentialBindingV1,
    pub task: super::admission::Task,
    pub wall_ms: u64,
    pub output_bytes: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Message {
    pub nonce: String,
    pub kind: String,
    pub report: Option<super::supervision::CleanupReport>,
}
impl Message {
    pub fn new(nonce: &str, kind: &str) -> Self {
        Self { nonce: nonce.into(), kind: kind.into(), report: None }
    }
}
#[derive(Default)]
pub(super) struct Decoder {
    header: Vec<u8>,
    payload: Vec<u8>,
    length: Option<usize>,
    traffic: usize,
    messages: usize,
}
impl Decoder {
    pub fn poll<T: DeserializeOwned>(&mut self, stream: &mut UnixStream) -> Result<Option<T>> {
        // One bounded read per turn; no unbounded queue or read-until-idle loop.
        let need = match self.length {
            Some(n) => n - self.payload.len(),
            None => 5 - self.header.len(),
        };
        let mut buf = [0u8; 4096];
        let count = need.min(buf.len());
        let n = match stream.read(&mut buf[..count]) {
            Ok(0) => return Err("bootstrap closed or truncated".into()),
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(None),
            Err(_) => return Err("bootstrap read failed".into()),
        };
        self.traffic += n;
        if self.traffic > TRAFFIC_LIMIT {
            return Err("bootstrap traffic bound".into());
        }
        if self.length.is_none() {
            self.header.extend_from_slice(&buf[..n]);
            if self.header.len() == 5 {
                let n = u32::from_be_bytes(self.header[1..5].try_into().unwrap()) as usize;
                if self.header[0] != 1 || n == 0 || n > FRAME_LIMIT {
                    return Err("invalid bootstrap header".into());
                }
                self.length = Some(n);
                self.payload = Vec::with_capacity(n);
            }
        } else {
            self.payload.extend_from_slice(&buf[..n]);
        }
        if self.length == Some(self.payload.len()) {
            self.messages += 1;
            if self.messages > 8 {
                return Err("bootstrap message bound".into());
            }
            let result = serde_json::from_slice(&self.payload)
                .map_err(|_| "malformed bootstrap payload".to_string())?;
            self.header.clear();
            self.payload.clear();
            self.length = None;
            return Ok(Some(result));
        }
        Ok(None)
    }
}
pub(super) fn receive<T: DeserializeOwned>(stream: &mut UnixStream) -> Result<T> {
    io(stream.set_nonblocking(true))?;
    let until = Instant::now() + Duration::from_secs(2);
    let mut decoder = Decoder::default();
    while Instant::now() < until {
        if let Some(value) = decoder.poll(stream)? {
            return Ok(value);
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    Err("bootstrap deadline".into())
}
pub(super) fn send<T: Serialize>(stream: &mut UnixStream, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec(value).map_err(|_| "bootstrap encode failed")?;
    if bytes.len() > FRAME_LIMIT {
        return Err("bootstrap frame bound".into());
    }
    let framed = worldinterface_worker::ports::frame(&bytes)?;
    // All messages are small and finite; nonblocking writes have an overall deadline.
    io(stream.set_nonblocking(true))?;
    let until = Instant::now() + Duration::from_millis(200);
    let mut offset = 0;
    while offset < framed.len() && Instant::now() < until {
        match stream.write(&framed[offset..]) {
            Ok(0) => return Err("bootstrap write closed".into()),
            Ok(n) => offset += n,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(1))
            }
            Err(_) => return Err("bootstrap write failed".into()),
        }
    }
    if offset != framed.len() {
        return Err("bootstrap write deadline".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fragmented_and_malformed_frames_are_bounded() {
        let (mut a, mut b) = UnixStream::pair().unwrap();
        b.set_nonblocking(true).unwrap();
        let bytes =
            worldinterface_worker::ports::frame(br#"{"nonce":"n","kind":"ready","report":null}"#)
                .unwrap();
        let mut decoder = Decoder::default();
        for (i, byte) in bytes.iter().enumerate() {
            a.write_all(&[*byte]).unwrap();
            let got = decoder.poll::<Message>(&mut b).unwrap();
            assert_eq!(got.is_some(), i == bytes.len() - 1);
        }
        for header in [[2, 0, 0, 0, 1], [1, 0, 1, 0, 0], [1, 0, 0, 0, 0]] {
            let (mut a, mut b) = UnixStream::pair().unwrap();
            b.set_nonblocking(true).unwrap();
            a.write_all(&header).unwrap();
            let mut d = Decoder::default();
            assert!(d.poll::<Message>(&mut b).is_err());
            assert_eq!(d.payload.capacity(), 0);
        }
        for bytes in [&b"\x01\0\0\0\x03{}"[..], &b"\x01\0\0\0\x01!"[..]] {
            let (mut a, mut b) = UnixStream::pair().unwrap();
            b.set_nonblocking(true).unwrap();
            a.write_all(bytes).unwrap();
            drop(a);
            let mut d = Decoder::default();
            assert!(d.poll::<Message>(&mut b).unwrap().is_none());
            if d.poll::<Message>(&mut b).is_ok() {
                assert!(d.poll::<Message>(&mut b).is_err());
            }
        }
    }
}

/// Proof is useful only on this private socket and for this exact run's short-lived credential.
pub(super) fn verify_ready(
    launch: &Launch,
    message: &Message,
    expected: &worldinterface_worker::credential::CredentialExpectation<'_>,
    now: u64,
    generation: u64,
) -> Result<()> {
    launch.credential.permits(
        expected,
        worldinterface_worker::mapping::Operation::Start,
        None,
        now,
        generation,
    )?;
    let digest = worldinterface_protocol::ContentHash::of(launch.token.as_bytes());
    if message.kind != "ready" || message.nonce != digest.as_str() || message.report.is_some() {
        return Err("run credential refused".into());
    }
    Ok(())
}
#[cfg(test)]
mod credential_tests {
    use super::*;
    use worldinterface_protocol::ContentHash;
    use worldinterface_worker::{
        credential::{CredentialBindingV1, CredentialExpectation},
        mapping::Operation,
    };
    #[test]
    fn bootstrap_credentials_reject_cross_run_expiry_revocation_audience_operation_and_scope() {
        let hash = ContentHash::of(b"trusted authorization");
        let binding = CredentialBindingV1 {
            schema: 1,
            reference: "reference".into(),
            issuer: "worldinterface".into(),
            audience: "process-managed-proof-1".into(),
            principal: "principal".into(),
            provider: "provider".into(),
            run: "run-a".into(),
            operations: vec![Operation::Start, Operation::CollectArtifacts],
            artifact_scope: vec!["artifact-a".into()],
            authorization_digest: hash.clone(),
            expires_unix_ms: 100,
            revocation_generation: 7,
        };
        let expected = CredentialExpectation {
            issuer: "worldinterface",
            audience: "process-managed-proof-1",
            principal: "principal",
            provider: "provider",
            run: "run-a",
            authorization_digest: &hash,
        };
        let mut launch = Launch {
            nonce: super::super::ownership::random().unwrap(),
            allocation: super::super::ownership::random().unwrap(),
            token: super::super::ownership::random().unwrap(),
            credential: binding,
            task: super::super::admission::Task::Probe,
            wall_ms: 10,
            output_bytes: 100,
        };
        let proof = Message::new(ContentHash::of(launch.token.as_bytes()).as_str(), "ready");
        assert!(verify_ready(&launch, &proof, &expected, 99, 7).is_ok());
        assert!(verify_ready(&launch, &proof, &expected, 100, 7).is_err());
        assert!(verify_ready(&launch, &proof, &expected, 99, 8).is_err());
        let cross = CredentialExpectation { run: "run-b", ..expected };
        assert!(verify_ready(&launch, &proof, &cross, 99, 7).is_err());
        let wrong_hash = ContentHash::of(b"other");
        let drift = CredentialExpectation { authorization_digest: &wrong_hash, ..expected };
        assert!(verify_ready(&launch, &proof, &drift, 99, 7).is_err());
        for expected in [
            CredentialExpectation { principal: "other", ..expected },
            CredentialExpectation { provider: "other", ..expected },
            CredentialExpectation { audience: "other", ..expected },
            CredentialExpectation { issuer: "other", ..expected },
        ] {
            assert!(verify_ready(&launch, &proof, &expected, 99, 7).is_err());
        }
        assert!(launch.credential.permits(&expected, Operation::Terminate, None, 99, 7).is_err());
        assert!(launch
            .credential
            .permits(&expected, Operation::CollectArtifacts, Some("artifact-b"), 99, 7)
            .is_err());
        assert!(launch
            .credential
            .permits(&expected, Operation::CollectArtifacts, Some("artifact-a"), 99, 7)
            .is_ok());
        launch.token = super::super::ownership::random().unwrap();
        assert!(verify_ready(&launch, &proof, &expected, 99, 7).is_err());
    }
    #[test]
    fn incomplete_bootstrap_has_an_independent_handshake_deadline() {
        let (mut a, _peer) = UnixStream::pair().unwrap();
        let started = Instant::now();
        assert!(receive::<Message>(&mut a).is_err());
        assert!(started.elapsed() < Duration::from_secs(3));
    }
    #[test]
    fn protocol_queue_and_total_traffic_are_bounded() {
        let (mut a, mut b) = UnixStream::pair().unwrap();
        b.set_nonblocking(true).unwrap();
        let mut d = Decoder::default();
        for _ in 0..8 {
            send(&mut a, &Message::new("n", "ready")).unwrap();
            while d.poll::<Message>(&mut b).unwrap().is_none() {}
        }
        send(&mut a, &Message::new("n", "ready")).unwrap();
        assert!(d.poll::<Message>(&mut b).unwrap().is_none());
        assert!(d.poll::<Message>(&mut b).is_err());
        let (mut a, mut b) = UnixStream::pair().unwrap();
        b.set_nonblocking(true).unwrap();
        let mut d = Decoder { traffic: TRAFFIC_LIMIT, ..Decoder::default() };
        a.write_all(&[1]).unwrap();
        assert!(d.poll::<Message>(&mut b).is_err());
    }
}
