# Idempotency Guide

WorldInterface guarantees that connector invocations are idempotent: re-executing
the same operation with the same `run_id` produces no duplicate side effects.

## How It Works

### The RunId

Every connector invocation receives a `run_id` (UUID) via the `InvocationContext`.
This ID is:
- **Deterministic:** Derived from the AQ task's current run
- **Unique per attempt cycle:** A new run_id is generated for each retry cycle
- **Stable across retries:** The same run_id is reused if the same run is re-dispatched

### Sacred Invariant 3

> `run_id` is the idempotency key for every connector invocation.

Connectors use the `run_id` to detect and skip duplicate executions.

## fs.write Idempotency

The `fs.write` connector uses marker files:

1. Before writing, check if `{path}.wid-{run_id}` exists
2. If the marker exists, skip the write (idempotent no-op)
3. Write the output file
4. Create the marker file

This ensures that even if the process crashes after writing but before completing
the AQ task, a re-dispatch will detect the marker and skip the duplicate write.

### Marker File Format

```
/path/to/output.txt                              # Output file
/path/to/output.txt.wid-550e8400-e29b-41d4-...   # Marker (empty)
```

### Write Modes

- `create`: Fails if the file already exists (unless marker found)
- `overwrite`: Overwrites existing content
- `append`: Appends to existing file

## http.request Idempotency

The `http.request` connector passes the `run_id` as a header:

```
X-Idempotency-Key: <run_id>
```

The target service is responsible for honoring this header. WorldInterface
ensures the same key is sent on retry.

## Crash-Resume Flow

1. AQ dispatches task → connector receives `run_id`
2. Connector writes output + marker
3. Coordinator writes result to ContextStore
4. AQ marks task complete

If the process crashes at step 2 (after write, before AQ completion):
- On restart, AQ replays WAL → task is still "executing"
- After lease expiry, AQ re-dispatches the task
- Connector checks marker → marker exists → returns cached result
- No duplicate side effect

## Custom Connectors

When implementing a custom connector:

```rust
impl Connector for MyConnector {
    fn invoke(&self, ctx: &InvocationContext, params: &Value) -> Result<Value, ConnectorError> {
        let run_id = ctx.run_id; // Use this as idempotency key

        // Check if this operation was already performed
        if already_done(run_id) {
            return Ok(cached_result(run_id));
        }

        // Perform the operation
        let result = do_work(params)?;

        // Record that this operation completed
        record_done(run_id, &result);

        Ok(result)
    }
}
```

## Testing Idempotency

The acceptance test suite verifies idempotency:
- `acceptance_c_idempotency`: Verifies marker files exist after fs.write
- `acceptance_b_crash_resume`: Verifies restart doesn't produce duplicate effects
- Host-level `crash_resume_no_duplicate_fs_write`: Verifies at the host API level
