# Issue Tracker Runner

[日本語](README_ja.md)

Issue Tracker Runner is a local tonic gRPC server for AI agents and business applications that need to manage issues consistently. It uses TaskChampion SQLite as its source of truth and provides issue creation, state transitions, temporary ownership, comments, relations, history, and backups.

The listener address is configurable; its default is a loopback address. A proxy is responsible for TLS, authentication, authorization, and any external exposure.

## Features

- **Issue management**: Create, update, retrieve, and list issues; manage state transitions, priorities, history, and stable pagination.
- **Collaboration**: Manage directed issue relations, comments, temporary lease-based claims, heartbeats, and releases.
- **Safe updates**: Use revisions and operation IDs to safely handle retries and detect conflicts or relation cycles.
- **Data portability**: Create validated SQLite backup artifacts and export issues, relations, and history as JSON.
- **Manager integration**: When manager settings are supplied, use candidates, evidence, durable operations, an outbox, and delivery to an external backend. Call `manager_get_contract_info` to check availability and health.
- **Listener surfaces**: When manager settings are supplied, the primary trusted-consumer listener exposes `ManagerConsumerService`, `ManagerTrustedReadService`, `IssueExtract`, `ManagerInfoService`, and `OutboxMaintenanceService`. `ManagerConsumerService` includes the trusted-consumer-only `UpdateCandidate`, `ListEvidence`, and `AddIssueRelation` operations. Each configured Gateway worker listener exposes `IssueGateway` only—`ListReady`, `Claim`, `Heartbeat`, `Release`, `Transition`, and `CompleteExecutionToAwaitingReview`—plus read-only `grpc.reflection.v1.ServerReflection` for that service's descriptor closure.
- **IssueGateway**: A reduced surface available when manager settings are on. Callers do not send manager write context, actor fields, or lease handles. Create synthesizes evidence on the server; claim returns a process-local capability id. The contract lives in [`issue_gateway.proto`](protobuf/rpc/v1/issue_gateway.proto).
- **IssueExtract**: When manager settings are on, evaluates the complete candidate input under a versioned structured policy. It records an immutable policy snapshot and truncation data in an ExtractionRun, and may register an `UNREVIEWED` issue. Disabled extraction returns a non-persistent skipped outcome. It does not run an LLM. The contract lives in [`issue_extract.proto`](protobuf/rpc/v1/issue_extract.proto).

## Usage

1. Build the server binary. A Rust toolchain supporting edition 2024 and `protoc` (Protocol Buffers compiler) are required.

   ```text
   cargo build --workspace --release
   ```

2. Create a TOML configuration file and run `issue-tracker-server --config <path>`. The default listener is `127.0.0.1:50051`.
   For file and OTLP logging configured through `.env`, see the [logging operations guide](docs/logging-operations.md).
3. Call `GetContractInfo` through a gRPC client to verify the contract, then invoke purpose-specific RPCs such as `CreateIssue`, `ListIssues`, and `TransitionIssue`.
4. To use manager features, configure `manager_settings`. The server keeps the ownership-maintenance and durable outbox worker resident; confirm that manager `GetContractInfo` reports `HEALTH_STATUS_HEALTHY` and `ACTIVATION_STATUS_READY` before issuing backend writes. `RunOutboxCycle` remains available for an explicit maintenance cycle.

Every method uses typed protobuf input and output. Callers must handle each method's success or error outcome.

A typical workflow is to call `create_issue`, let a worker call `claim_issue`, use `heartbeat_issue_claim` when needed, then call `transition_issue` and `release_issue_claim` for ordinary state changes. When an IssueGateway worker submits completed-run evidence for review, use `CompleteExecutionToAwaitingReview` instead of `Transition`. Every write requires an operation ID to identify retries.

### Awaiting-review completion

Workers that have completed a run with evidence must use the dedicated
`IssueGateway/CompleteExecutionToAwaitingReview` RPC. Send
`state_contract_version: "awaiting_review/v1"`, the active capability, the
latest issue revision, the run ID, the full commit ID, and the evidence and
completion report. The server validates the current claim and revision, then
atomically records `AWAITING_REVIEW`, its history/evidence, and claim removal.
This state remains a native TaskChampion `Pending` task, but is excluded from
`ListReady`, cannot be claimed, and cannot be entered through the generic
`Transition` RPC. It leaves only through a review decision: `OPEN` (rework),
`DONE` (approved), or `CANCELED`; moving it to any other state is rejected.

Completion fields are bounded: `run_id` up to 256 bytes, a 40- or 64-character
hexadecimal commit ID, 1–1000 evidence references of up to 2048 bytes each,
up to 1000 verified evidence entries, and evidence summaries, URIs, unverified
reasons, and residual risks of up to 16 KiB each.

The RPC consumes its process-local capability. If the response is lost, retry
with the same idempotency key and identical request (apart from the expired
capability); the server returns the immutable receipt only while the endpoint's
Gateway registration identity is unchanged. A changed run, commit, report, or
registration is an idempotency conflict. If the RPC fails before reaching the
Manager, the capability is kept and the same request may be retried. Do not persist or reuse raw lease
handles.

Roll this contract out as a writer/consumer gate: first upgrade and drain all
older Gateway writers, and ensure every reader understands state value 7 and
fails closed on unknown state values. Then enable consumers to call the new RPC
with `awaiting_review/v1`. Do not mix old and new writers against the same
Tracker root or roll an older binary back after `AWAITING_REVIEW` has been
written; the retained v7 schema fingerprint is not a feature-negotiation token.
Take a backup before the coordinated rollout.

## Important notes

- Use a dedicated directory for `data_root`. The same root cannot be written concurrently by another writer.
- Manager contract `issue-tracker-manager/v14` uses SQLite schema v31. A v30 (or v29) manager root is upgraded in place; the migration validates operation-history projections and adds the direct/state/order index. Older roots are incompatible and require a new data root. See the [gRPC server specification](docs/grpc-server-spec.md) for the procedure.
- A `ClaimService` lease handle is returned only in a successful response. Do not expect to retrieve it again or reuse it through a retry. `IssueGateway/Claim` returns a capability id instead; that id is lost on process restart.
- Backups create artifacts. The API returns artifact metadata, not artifact bytes, and does not restore a live root. For artifact storage and restoration through the Rust library API, see the [backup and restore guide](docs/backup-restore-operations.md).
- Authentication, authorization, tenant isolation, and user confirmation are the responsibility of the proxy or consumer.

## API reference

- Core issue API and runner settings: [`protobuf/v1`](protobuf/v1)
- Manager API: [`protobuf/manager/v1`](protobuf/manager/v1)
- IssueGateway (reduced surface): [`protobuf/rpc/v1/issue_gateway.proto`](protobuf/rpc/v1/issue_gateway.proto)
- IssueExtract (apply extraction): [`protobuf/rpc/v1/issue_extract.proto`](protobuf/rpc/v1/issue_extract.proto)
- Service-specific RPC definitions: [`protobuf/rpc/v1`](protobuf/rpc/v1)

See the [gRPC server specification](docs/grpc-server-spec.md) and [protobuf service layout](docs/protobuf-service-layout.md) for listener boundaries and service split.

The protobuf definitions above are the source of truth for every method's input, output, and errors.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
