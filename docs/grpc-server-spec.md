# ローカル gRPC Server 仕様

Issue Tracker Server は tonic gRPC server として起動する。`server.listen` の既定値は `127.0.0.1:50051` である。TLS、認証、認可、外部公開、tenant 分離は server の責務ではなく、必要な場合は proxy 又は consumer が担う。

## listener の公開面

manager settings が有効なとき、primary trusted-consumer listener は次だけを公開する。

| service | 用途 |
| --- | --- |
| `ManagerConsumerService` | Candidate のレビュー（`ListReviewCandidates` は mapping と canonical Issue lifecycle を server 側で結合・選別）、Evidence と ExtractionRun の読取り、Issue の作成・状態遷移、管理終了・所有権回復、Issue operation の手動解決、所有権診断、operation 履歴 |
| `IssueExtract` | 抽出 hint の読取りと extraction の適用 |
| `ManagerInfoService` | `GetContractInfo` |
| `OutboxMaintenanceService` | `RunOutboxCycle` |

`ManagerConsumerService` は個別 entity service の代替ではない。trusted consumer に必要な既存 RPC だけを facade として公開し、`UpdateCandidate`、`ListEvidence`、`AddIssueRelation` を含む。`CandidateService`、`EvidenceService`、`CanonicalIssueService`、`ExtractionRunService` を service 全体として primary に登録してはならない。

設定済み Gateway worker listener は `IssueGateway` の `ListReady`、`Claim`、`Heartbeat`、`Release`、`Transition` だけを domain RPC として公開する。`ManagerConsumerService`、`IssueExtract`、manager の個別 entity service、汎用 Issue service、Gateway control service はこの listener で `UNIMPLEMENTED` とする。

`ListReviewCandidates` は primary trusted-consumer 専用であり、worker／control listener、Gateway、外部 MCP には公開しない。既存の Candidate の `AUTO_REGISTERED` 履歴は変更せず、終端 canonical Issue に対応する行だけをこの read model から除外する。
`ListReviewCandidates` の opaque continuation token は、先頭ページで pin した複合読取境界に対してのみ検証する。pin の TTL または保持上限で pin が失われた token は `PageTokenExpired` として拒否し、pin 有効期間内の tracked write では失効しない。行集合と同期情報は先頭ページの pin 時点で固定する。

Gateway control listener は `GatewayControlService` だけを公開する。primary と worker は control service を公開しない。

## 取消済み Issue の再開

`ManagerConsumerService.TransitionIssue` は、actor audit と `expected_revision` を持つ durable operation として `CANCELED → OPEN` を許可する。再開は cancellation report を受け取らず、元の取消 report と transition history を変更しない。`CANCELED` への遷移は active claim の owner、handle、expiry を同一の原子的な状態変更で解除する。再開も旧データの残存 claim を解除して正規化するため、確定後の current Issue は `OPEN` かつ `completed_at`・active claim 未指定となり、旧 lease の期限を待たずに再び claim できる。

Gateway worker が lease capability を伴って `Transition` する場合、server は対象 Issue 1 件の現在の lease handle、claim revision、expected revision、有効期限を照合する。取消又は再開の後にプロセス内に旧 capability のレコードが残っていても、永続 lease と一致しないため worker 書込みは拒否される。これは全 Issue の走査を行わず、Gateway capability map の早期削除にも依存しない。

`DONE → OPEN` は lifecycle precondition として拒否する。Gateway worker listener は終端 state（`DONE` 又は `CANCELED`）からの `Transition` を拒否し、trusted-consumer の再開操作を代行できない。`BLOCKED` の `issue_blocked` 拒否、Heartbeat、Release の既存契約は変えない。

`completed_at` は current Issue が `DONE` 又は `CANCELED` のときだけ返す。状態変更時はこの不変条件を永続化し、既存データに active state と `completed_at` が併存していても read projection では `completed_at` を返さない。これは互換性のための read-time normalization であり、schema migration、revision、updated_at、operation history を変更しない。

## Gateway worker reflection

worker listener は `grpc.reflection.v1.ServerReflection` を descriptor 読取り専用で公開する。旧 `grpc.reflection.v1alpha.ServerReflection` は公開しない。

worker reflection descriptor set は `issue_gateway.proto` とその import closure に限定する。reflection の ListServices と file descriptor 要求から `IssueGateway` 以外の domain service を発見又は解決できてはならない。reflection は worker の domain RPC allowlist を拡張しない。primary は、実際に登録する trusted-consumer service（`ManagerConsumerService`、`IssueExtract`、`ManagerInfoService`、`OutboxMaintenanceService`）と各 import closure を含む descriptor set を、`grpc.reflection.v1.ServerReflection` で公開する。これは listener の domain RPC 公開面を増やさず、dynamic client の discovery 面を登録済み service と一致させる。control listener は reflection を公開しない。

## manager v14 / schema v31 への移行

manager contract version は `issue-tracker-manager/v14`、Tracker contract version は `lookback-issue/v7`、manager SQLite schema は v31 である（fingerprint `issue-tracker-manager/v14/sqlite-schema-v31`）。v30 root からの in-place migration では operation-history の direct/state/order 複合 index を追加する。v29 root は v30 の mapping-index 再構築を経由して v31 へ更新される。これは read model の導出 index の補修を行い、旧 v30 の commit 時刻と payload の admission 時刻が異なる場合は source と projection の `created_at`/`updated_at` を payload の値へ正規化し、旧 lifecycle 更新で projection の state 等が遅れている場合は payload から再構築する。これらの修復を行った場合は manager generation を進め、旧 offset cursor を継続利用できないようにする。それ以外の正本 payload、revision、operation history は変更しない。

`ListOperationHistory` は keyset cursor v2 を新規発行する。移行期間に限り、`operation_history_v1_compat_enabled`（既定値 `true`）で旧 v1 cursor の受け入れを制御する。v2 cursor の generation 境界は追跡対象の durable Manager write generation に束縛され、phase-health telemetry の untracked write は generation を進めず cursor を期限切れにしない。履歴の直接対象・affected Issue、状態、method、decision、時刻範囲を SQL ページングへ渡す。旧 v30 の commit 時刻と payload の admission 時刻が異なる場合は、migration transaction 内で source と projection の `created_at`/`updated_at` を payload の値に揃える。

1. server と root を使用する consumer を停止する。
2. v29 root を保持する必要があれば、移行前に外部保管へ退避する。
3. v30 root（または v29 root）を v31 server に指定して起動し、migration の完了を待つ。移行は v29→v30 と v30→v31 の各 transaction で行われ、途中で停止した場合は次回起動時に未完了の段階から再開する。旧形式として判定できる commit/admission 時刻差と lifecycle projection の遅れは payload から修復する。修復時は generation が進むため、旧 v1 offset cursor は継続利用できない。それ以外の history payload と投影の不整合は該当 transaction 全体を rollback する。
4. server を起動し、primary の `GetContractInfo` で v14 の contract/schema fingerprint、health、activation を確認する。
5. v31 root を利用する consumer だけを再開する。v29 より古い root は再利用せず、新しい空の絶対パスの `data_root` を用意する。

旧 contract identity、v29/v30/v31 以外の schema version、又は metadata identity がある root は起動時に非破壊的に拒否する。移行に失敗した場合は transaction が rollback されるため、原因を解消して再試行する。v29 より古い version から v14/v31 へ保存済みデータを移行する場合は、この手順を使わず、専用データ移行仕様を先に定める。

各 RPC の wire 契約、入力、結果、業務エラーは `protobuf/` を正本とする。service ごとの配置は [protobuf service 構成](protobuf-service-layout.md)を参照する。

`EXPLICIT` かつ `CONFIRMED` の Issue 作成は、trusted consumer が `HUMAN` ActorAudit と安全な非空 reason を記録した場合に限り空の Evidence manifest を許可する。これは認可判定ではなく監査契約である。その他の作成は利用可能かつ検証済み Evidence を必要とする。
