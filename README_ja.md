# Issue Tracker Runner

[English](README.md)

Issue Tracker Runner は、AI agent や業務アプリケーションが Issue を一貫した形で管理するためのローカル tonic gRPC server です。TaskChampion SQLite をデータの正本として使い、Issue の作成、状態遷移、担当の一時的な確保、コメント、関連付け、履歴、バックアップを提供します。

server の listen address は設定できます。既定値は loopback address ですが、外部公開時の TLS、認証認可、tenant 分離は、server 前段の proxy または consumer の責務です。詳細は[ローカル gRPC Server 仕様](docs/grpc-server-spec.md)を参照してください。

## できること

- **Issue の管理**: Issue の作成・更新・取得・一覧、状態遷移、優先度、履歴、安定したページングを扱えます。
- **協調作業**: Issue 間の方向付き relation、コメント、lease による一時的な claim、heartbeat、release を扱えます。
- **安全な更新**: revision と operation ID により、同じ要求の再送を安全に処理し、競合や循環 relation を検出します。
- **データの持ち運び**: 検証済み SQLite backup artifact の作成と、Issue・relation・履歴の JSON export を行えます。
- **統合向け manager 機能**: manager settings を指定した場合、候補、evidence、durable operation、outbox、外部 backend への配送を利用できます。`manager_get_contract_info` で利用可能な機能と正常性を確認してください。
- **listener の公開面**: manager settings を指定した場合、primary trusted-consumer listener は `ManagerConsumerService`、`ManagerTrustedReadService`、`IssueExtract`、`ManagerInfoService`、`OutboxMaintenanceService` を公開します。`ManagerConsumerService` には trusted consumer 専用の `UpdateCandidate`、`ListEvidence`、`AddIssueRelation` も含まれます。設定された各 Gateway worker listener は `ListReady`、`Claim`、`Heartbeat`、`Release`、`Transition`、`CompleteExecutionToAwaitingReview` を持つ `IssueGateway` と、その descriptor closure を読む専用の `grpc.reflection.v1.ServerReflection` だけを公開します。
- **IssueGateway**: manager 有効時に使う縮小面です。caller は manager write context、actor、lease handle を送りません。create はサーバが根拠を合成し、claim は過程メモリ上の capability id だけを返します。契約は [`issue_gateway.proto`](protobuf/rpc/v1/issue_gateway.proto) が正本です。
- **IssueExtract**: manager 有効時に、全 Candidate 入力を versioned な構造化 policy で評価します。ExtractionRun には immutable な policy snapshot と切詰め情報を残し、条件を満たせば `UNREVIEWED` Issue を登録します。抽出無効時は永続化しない skipped outcome を返します。LLM は実行しません。契約は [`issue_extract.proto`](protobuf/rpc/v1/issue_extract.proto) が正本です。

## 使い方

1. server binary を build します。edition 2024 に対応した Rust toolchain と `protoc`（Protocol Buffers compiler）が必要です。

   ```text
   cargo build --workspace --release
   ```

2. TOML 設定を用意し、`issue-tracker-server --config <path>` を実行します。listen address は `127.0.0.1:50051` が既定です。
   `.env` による file および OTLP の log 出力は[ログ出力の運用手順](docs/logging-operations.md)を参照してください。
3. gRPC client から `GetContractInfo` を呼び、その後 `CreateIssue`、`ListIssues`、`TransitionIssue` などの目的別 RPC を呼び出します。
4. manager 機能を使う場合は manager settings を指定します。server が所有権保守と durable outbox driver を常駐実行するため、manager の `GetContractInfo` が `HEALTH_STATUS_HEALTHY` と `ACTIVATION_STATUS_READY` を返すことを確認してから backend 書込みを開始します。`RunOutboxCycle` は明示的な保守周期の実行用に利用できます。

各 method の引数と結果は protobuf の型付き契約です。呼び出し側は、method ごとの success/error outcome を判定してください。

通常の作業フローは、`create_issue` で Issue を作成し、作業者が `claim_issue`、必要に応じて `heartbeat_issue_claim`、完了時に `transition_issue` と `release_issue_claim` を呼ぶ流れです。IssueGateway の worker が完了した run の証跡をレビューに回す場合は、`Transition` ではなく `CompleteExecutionToAwaitingReview` を使います。各書込みには、再送を識別する operation ID を指定します。

### レビュー待ちへの完了報告

証跡付きで run を完了した worker は、専用の `IssueGateway/CompleteExecutionToAwaitingReview` RPC を使います。`state_contract_version: "awaiting_review/v1"`、有効な capability、最新の Issue revision、run ID、完全な commit ID、証跡参照と完了報告を送ります。server は現在の claim と revision を検証し、`AWAITING_REVIEW` への遷移、履歴と証跡の記録、claim の解除を一つの commit で行います。この状態は TaskChampion 上は `Pending` のままですが、`ListReady` に含まれず、claim できず、汎用の `Transition` RPC からは遷移できません。この状態から出られるのはレビュー結果としての `OPEN`（差し戻し）、`DONE`（承認）、`CANCELED` だけで、それ以外の状態への遷移は拒否します。

完了報告の各項目には上限があります。`run_id` は 256 バイト以下、commit ID は 40 文字または 64 文字の 16 進数、証跡参照は 1〜1000 件で各 2048 バイト以下、verified の証跡は 1000 件以下、証跡の summary と URI、unverified の reason と residual risk はそれぞれ 16 KiB 以下です。

この RPC は過程メモリ上の capability を消費します。応答が失われた場合は、同じ idempotency key と同一の要求（失効した capability を除く）で再送してください。endpoint の Gateway 登録が変わっていない間は、不変の receipt を返します。run、commit、報告、登録のいずれかが変わった場合は idempotency conflict になります。Manager に届く前に失敗した場合は capability が維持されるため、同じ要求をそのまま再送できます。raw lease handle を保存・再利用しないでください。

この契約は writer/consumer の段階的な導入として展開します。まず旧 Gateway writer をすべて更新・停止し、すべての reader が状態値 7 を解釈し、未知の状態値では fail-closed になることを確認します。その後、consumer が `awaiting_review/v1` で新 RPC を呼べるようにします。同じ Tracker root に新旧の writer を混在させないでください。`AWAITING_REVIEW` が書き込まれた後に旧 binary へ戻すこともできません。保持している v7 schema fingerprint は機能交渉のための値ではありません。展開前に backup を取得してください。

## 利用時の注意

- `data_root` は runner 専用の directory にしてください。同じ root を別の writer から同時に扱うことはできません。
- manager contract `issue-tracker-manager/v14` は SQLite schema v31 を使います。v30（または v29）の manager root は operation-history projection を検証し、direct/state/order index を追加する in-place migration の対象です。それより古い root は互換性がないため、新しい data root を使用してください。手順は[ローカル gRPC Server 仕様](docs/grpc-server-spec.md)を参照してください。
- `ClaimService` の lease handle は成功応答でのみ返ります。再取得や再送を前提に保存しないでください。`IssueGateway/Claim` は handle の代わりに capability id を返し、再起動で失効します。
- backup は artifact を作成する機能です。artifact の metadata は取得できますが、bytes は API から返しません。稼働中 root の復元は plugin API では提供しません。artifact の保管と Rust library API による復元は[バックアップと復元の運用手順](docs/backup-restore-operations.md)を参照してください。
- 認証、認可、tenant 分離、利用者への確認は host または consumer の責務です。

## API リファレンス

- 基本 Issue API と runner settings: [`protobuf/v1`](protobuf/v1)
- manager API: [`protobuf/manager/v1`](protobuf/manager/v1)
- IssueGateway（縮小面）: [`protobuf/rpc/v1/issue_gateway.proto`](protobuf/rpc/v1/issue_gateway.proto)
- IssueExtract（抽出の適用）: [`protobuf/rpc/v1/issue_extract.proto`](protobuf/rpc/v1/issue_extract.proto)
- service 別 RPC 定義: [`protobuf/rpc/v1`](protobuf/rpc/v1)

listener 境界は[ローカル gRPC Server 仕様](docs/grpc-server-spec.md)、service の分割は[protobuf service 構成](docs/protobuf-service-layout.md)を参照してください。

各 method の入力・出力・エラーは、上記 protobuf 定義を正本とします。

## ライセンス

[Apache License, Version 2.0](LICENSE) で提供します。
