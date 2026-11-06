//! WI-owned consumer: every control and disclosure uses the application API.
use std::time::{Duration, Instant};
use worldinterface_domain::{
    control_plane as cp,
    flow::DirectOperationSelection,
    worker::{WorkerCollection, WorkerState},
};
use worldinterface_protocol::{control_plane::*, *};
use worldinterface_runtime::embedded::Application;

pub async fn operation<A: Application>(
    app: &mut A,
    session: &A::Session,
    submission: EffectSubmission,
    selection: DirectOperationSelection,
    worker: &ObjectRef,
    provider: &OpaqueRef,
    run: &OpaqueRef,
) -> OpaqueRef {
    let op = selection.operation.clone();
    let id = app.submit(session, submission.clone(), selection.clone()).unwrap();
    assert_eq!(app.submit(session, submission, selection).unwrap(), id);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        app.advance(session).await.unwrap();
        if let Ok(state) = app.inspect_worker(session, worker, provider, run) {
            if op.as_str() == "inspect"
                || state.operations.values().any(|o| o.operation == op && o.acknowledged)
            {
                break;
            }
        }
        assert!(Instant::now() < deadline, "embedded operation {op:?}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // Complete the Flow coordinator's durable wake and receipt publication.
    for _ in 0..12 {
        app.advance(session).await.unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    id
}
pub async fn exited<A: Application>(
    app: &mut A,
    session: &A::Session,
    worker: &ObjectRef,
    provider: &OpaqueRef,
    run: &OpaqueRef,
) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        app.advance(session).await.unwrap();
        if app.inspect_worker(session, worker, provider, run).unwrap().exited {
            return;
        }
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
pub fn published<A: Application>(
    app: &A,
    session: &A::Session,
    worker: &ObjectRef,
    provider: &OpaqueRef,
    run: &OpaqueRef,
    expected: &[(ObjectRef, Vec<u8>)],
) -> WorkerState {
    let state = app.inspect_worker(session, worker, provider, run).unwrap();
    assert!(state.completed && matches!(state.collection, WorkerCollection::Published(_)));
    for (reference, bytes) in expected {
        assert_eq!(app.read_artifact(session, reference).unwrap(), *bytes);
    }
    state
}
pub fn denied<A: Application>(app: &A, session: &A::Session, reference: &ObjectRef) {
    assert!(app
        .control(
            session,
            &cp::Request {
                id: "denied-read".to_string().try_into().unwrap(),
                operation: cp::Operation::Get { reference: reference.clone() }
            }
        )
        .is_err());
}
pub fn receipts<A: Application>(app: &A, session: &A::Session, references: &[ObjectRef]) {
    assert!(references.len() >= 5, "lifecycle receipts missing");
    for reference in references {
        let receipt = app.receipt(session, reference).unwrap();
        assert_eq!(receipt.id.as_str(), reference.id.as_str());
        assert!(receipt.canonical.is_none());
    }
}
