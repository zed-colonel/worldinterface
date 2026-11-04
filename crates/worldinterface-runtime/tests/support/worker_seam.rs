//! WI-owned seam simulation: bounded asynchronous messages, no path, PID or store access.
//! This is neither remote-provider nor isolation qualification.
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{mpsc, Arc, Mutex},
    thread::JoinHandle,
    time::Duration,
};
use worldinterface_protocol::*;
use worldinterface_runtime::{worker::provider::*, worker_process::*, Result};
use worldinterface_worker::{
    artifacts::{ArtifactLocatorV2, ExportManifestV1, ExportV1},
    mapping::{Operation, ResolvedWorkerV1, WorkerRequestV1},
    ports::*,
};
pub const LABEL: &str = "seam simulation";
pub fn provenance() -> ProviderProvenance {
    let implementation = ContentHash::of(include_bytes!("worker_seam.rs"));
    ProviderProvenance {
        adapter: implementation.clone(),
        implementation,
        helper: ContentHash::of(b"seam-simulation:no-helper"),
        seam_simulation: true,
    }
}
struct Run {
    auth: AuthorizedRun,
    events: Vec<Observation>,
    acks: BTreeSet<String>,
    artifacts: BTreeMap<String, Vec<u8>>,
    finished: bool,
    stopped: bool,
    launches: usize,
}
impl Run {
    fn event(&mut self, kind: ObservationKind) {
        let sequence = self.events.len() as u64 + 1;
        self.events.push(Observation {
            provider: self.auth.controls.provider.clone(),
            run: self.auth.controls.run.clone(),
            event: format!("seam-{sequence}"),
            sequence,
            kind,
        });
    }
}
type Runs = Arc<Mutex<BTreeMap<(String, String), Run>>>;
pub struct Seam {
    runs: Runs,
    sender: Option<mpsc::SyncSender<WorkerRequestV1>>,
    thread: Option<JoinHandle<()>>,
}
impl Seam {
    pub fn new() -> Self {
        let runs = Runs::default();
        let shared = runs.clone();
        let (sender, receiver) = mpsc::sync_channel::<WorkerRequestV1>(8);
        let thread = std::thread::spawn(move || {
            while let Ok(request) = receiver.recv() {
                // Delay belongs to this test provider, never to the connector or executor.
                std::thread::sleep(Duration::from_millis(30));
                let mut runs = shared.lock().unwrap();
                let run = runs.get_mut(&(request.provider.clone(), request.run.clone())).unwrap();
                if !run.acks.insert(request.request.clone()) {
                    continue;
                }
                match request.operation {
                    Operation::Provision => run.event(ObservationKind::Provisioned),
                    Operation::Start => {
                        if run.stopped {
                            run.finished = true;
                            run.event(ObservationKind::Exited);
                            continue;
                        }
                        run.launches += 1;
                        run.event(ObservationKind::Accepted);
                        run.event(ObservationKind::Running);
                        let bytes = match run.auth.task {
                            Task::Transform { xor } => run
                                .auth
                                .inputs
                                .first()
                                .map(|i| i.bytes.iter().map(|b| b ^ xor).collect())
                                .unwrap_or_default(),
                            _ => b"{\"clean\":true}".to_vec(),
                        };
                        for e in &run.auth.exports {
                            run.artifacts.insert(e.reference.clone(), bytes.clone());
                        }
                        run.finished = true;
                        run.event(ObservationKind::Exited);
                    }
                    _ => {}
                }
            }
        });
        Self { runs, sender: Some(sender), thread: Some(thread) }
    }
    pub fn launches(&self) -> usize {
        self.runs.lock().unwrap().values().map(|r| r.launches).sum()
    }
}
impl Drop for Seam {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}
impl AdmittedProvider for Seam {
    fn admit(&self, auth: AuthorizedRun) -> Result<()> {
        if auth.inputs.len() > 1
            || auth.exports.len() > 4
            || auth
                .inputs
                .iter()
                .any(|i| i.bytes.len() > 1024 * 1024 || ContentHash::of(&i.bytes) != i.digest)
        {
            return Err("seam bounds".into());
        }
        let key = (auth.controls.provider.clone(), auth.controls.run.clone());
        let mut runs = self.runs.lock().unwrap();
        if runs.contains_key(&key) || runs.len() >= 8 {
            return Err("seam run conflict".into());
        }
        runs.insert(
            key,
            Run {
                auth,
                events: vec![],
                acks: BTreeSet::new(),
                artifacts: BTreeMap::new(),
                finished: false,
                stopped: false,
                launches: 0,
            },
        );
        Ok(())
    }
    fn acknowledged(&self, request: &WorkerRequestV1) -> Result<bool> {
        Ok(self
            .runs
            .lock()
            .unwrap()
            .get(&(request.provider.clone(), request.run.clone()))
            .is_some_and(|r| r.acks.contains(&request.request)))
    }
    fn submit(&self, request: &WorkerRequestV1) -> Result<Acknowledgment> {
        let mut runs = self.runs.lock().unwrap();
        let run = runs
            .get_mut(&(request.provider.clone(), request.run.clone()))
            .ok_or("seam run absent")?;
        if !run.auth.operations.iter().any(|(r, _)| r == request) {
            return Err("seam exact request denied".into());
        }
        if matches!(request.operation, Operation::CollectArtifacts | Operation::Release) {
            if !run.finished {
                return Ok(Acknowledgment::Uncertain);
            }
            if run.acks.insert(request.request.clone()) {
                run.event(if request.operation == Operation::Release {
                    ObservationKind::Released
                } else {
                    ObservationKind::CollectionReady
                });
            }
            return Ok(Acknowledgment::Accepted);
        }
        if run.acks.contains(&request.request) {
            return Ok(Acknowledgment::Accepted);
        }
        self.sender.as_ref().unwrap().try_send(request.clone()).map_err(|_| "seam mailbox full")?;
        Ok(if request.operation == Operation::Start {
            Acknowledgment::Uncertain
        } else {
            Acknowledgment::Accepted
        })
    }
    fn observe(&self, provider: &str, run: &str) -> Result<Vec<Observation>> {
        let runs = self.runs.lock().unwrap();
        let events = &runs.get(&(provider.into(), run.into())).ok_or("seam run denied")?.events;
        // Deliberately reordered and duplicated. WI preserves gaps and deduplicates.
        let mut result: Vec<_> = events.iter().rev().cloned().collect();
        if let Some(first) = events.first() {
            result.push(first.clone());
        }
        Ok(result)
    }
    fn read(&self, locator: &ArtifactLocatorV2) -> Result<Vec<u8>> {
        let runs = self.runs.lock().unwrap();
        let run =
            runs.get(&(locator.provider.clone(), locator.run.clone())).ok_or("seam run denied")?;
        if locator.authorization != run.auth.credential.authorization_digest {
            return Err("seam realization denied".into());
        }
        let bytes = run.artifacts.get(&locator.reference).ok_or("seam artifact denied")?;
        if bytes.len() as u64 != locator.size || ContentHash::of(bytes) != locator.digest {
            return Err("seam locator mismatch".into());
        }
        Ok(bytes.clone())
    }
    fn manifest(&self, provider: &str, run: &str) -> Result<Vec<u8>> {
        let runs = self.runs.lock().unwrap();
        let run = runs.get(&(provider.into(), run.into())).ok_or("seam run denied")?;
        Ok(canonical_data(&ExportManifestV1 {
            schema: 1,
            provider: provider.into(),
            run: run.auth.controls.run.clone(),
            owner: run.auth.scope.principal.clone(),
            exports: run
                .auth
                .exports
                .iter()
                .map(|e| ExportV1 {
                    reference: e.reference.clone(),
                    relative_name: e.name.clone(),
                    digest: e.digest.clone(),
                    size: e.size,
                    required_verifiers: vec![],
                    passed_verifiers: vec![],
                })
                .collect(),
        })?)
    }
    fn evidence(&self, provider: &str, run: &str) -> Result<Option<CleanupReport>> {
        let runs = self.runs.lock().unwrap();
        let run = runs.get(&(provider.into(), run.into())).ok_or("seam run denied")?;
        Ok(run.finished.then(|| CleanupReport {
            exit_confirmed: true,
            cleanup_confirmed: true,
            exit_success: Some(true),
            reason: LABEL.into(),
            ..CleanupReport::default()
        }))
    }
    fn stop(&self, provider: &str, run: &str) -> Result<()> {
        let mut runs = self.runs.lock().unwrap();
        let run = runs.get_mut(&(provider.into(), run.into())).ok_or("seam run denied")?;
        run.stopped = true;
        Ok(())
    }
    fn recover_cleanup(
        &self,
        provider: &str,
        run: &str,
        _operations: &[(WorkerRequestV1, ResolvedWorkerV1)],
    ) -> Result<Option<CleanupReport>> {
        self.evidence(provider, run)
    }
}
