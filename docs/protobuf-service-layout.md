# protobuf service 構成

RPC 定義は `protobuf/rpc/v1` に service ごとに置き、共通の request、response、enum は `protobuf/v1` 又は `protobuf/manager/v1` に置く。message の package と field number は、破壊的移行を明示した場合を除き維持する。

## service と listener の対応

| listener | 公開 service | 非公開の代表例 |
| --- | --- | --- |
| primary trusted-consumer（manager 有効時） | `ManagerConsumerService`、`ManagerTrustedReadService`、`IssueExtract`、`ManagerInfoService`、`OutboxMaintenanceService`、`grpc.reflection.v1.ServerReflection` | `CandidateService`、`EvidenceService`、`CanonicalIssueService`、`ExtractionRunService`、`IssueGateway` |
| Gateway worker | `IssueGateway`、`grpc.reflection.v1.ServerReflection` | `ManagerConsumerService`、`IssueExtract`、manager 個別 service、汎用 Issue service、`GatewayControlService` |
| Gateway control | `GatewayControlService` | primary と worker の service |

`IssueGateway` の domain method は `ListReady`、`Claim`、`Heartbeat`、`Release`、`Transition`、`CompleteExecutionToAwaitingReview` の六つだけである。worker reflection は `issue_gateway.proto` の import closure だけを公開し、ほかの service descriptor を含めない。

primary reflection は、実際に primary に登録する `ManagerConsumerService`、`ManagerTrustedReadService`、`IssueExtract`、`ManagerInfoService`、`OutboxMaintenanceService` と各 import closure を v1 で公開する。reflection は listener の domain RPC 公開面を変えず、dynamic client が登録済み trusted-consumer service を発見するためのものとする。未登録の個別 entity service は descriptor に含めない。v1alpha reflection は公開しない。

## ManagerConsumerService

`ManagerConsumerService` は primary trusted-consumer listener 専用の最小 facade である。entity service 全体を公開する代わりに、trusted consumer が必要とする既存 manager RPC をそのまま載せる。`ListReviewCandidates` は Candidate、mapping、canonical Issue lifecycle を server 側で結合する read-only review view であり、worker listener や外部 MCP には公開しない。

主な trusted-consumer RPC は次のとおりである。

| RPC | 目的 |
| --- | --- |
| `UpdateCandidate(UpdateCandidateArgs)` | `PROPOSED` Candidate の人による編集 |
| `ListEvidence(ListEvidenceArgs)` | source locator を使う Evidence の再照合 |
| `AddIssueRelation(AddIssueRelationArgs)` | 明示 Issue から元 Issue への `SUPERSEDES` 関係の記録 |
| `GetIssueRelations(GetIssueRelationsArgs)` | 単一 canonical Issue の inbound/outbound relation 読取り |
| `ListReviewCandidates(ListReviewCandidatesArgs)` | Candidate の status、mapping、Issue lifecycle を結合した要確認 read model |

これらは既存の `ManagerReadContext` / `ManagerWriteContext`、CAS、idempotency、ActorAudit、page token の契約を変更しない。worker listener では service 自体が未登録であり、これらを含む呼出しは `UNIMPLEMENTED` となる。

`UpdateEvidence` の revision 不一致は `REVISION_CONFLICT` と `revision_conflict.expected_revision`／`current_revision` を返す。`ConfirmAssociatedIssueResolution.target_state` は confirmation patch と同一の durable canonical update として適用し、未指定時は現在の lifecycle state を保持する。`DONE` を指定する場合は `CompletionReport`、`CANCELED` を指定する場合は `CancellationReport` を同一更新に含めなければならない。これらの report は通常の lifecycle transition と同じ監査履歴に永続化する。

`IssueExtract` は extraction apply 専用であり、LLM や外部 source の再読を行わない。Evidence の更新は primary の `ListEvidence` と既存 `UpdateEvidence` を使う consumer の責務である。

## ManagerTrustedReadService

`ManagerTrustedReadService` は primary trusted-consumer 専用の読み取り facade で、`ListIssueHistory` と `GetDownstreamOperation` の二つだけを公開する。すべての要求に既存の `ManagerReadContext` を要求し、Manager の active data root、contract fingerprint、schema fingerprint を既存 trusted-read gate で照合する。履歴要求は canonical Issue ID に限定し、Manager/downstream operation ID の組を与えた場合は `manager_transition_issue` の Manager row と保存済み要求 snapshot の schema、digest、target、遷移先 Issue、audit context が一致することを確認する。要求から再導出した marker expectation も admission 時の値と照合し、失敗時は native Tracker を読み取らない。Downstream operation 要求ではさらに native marker の method、scope、request/contract fingerprint、identity、durability、replayability を照合してから binding を返す。operation ID を指定しない履歴要求は Manager row なしで Issue 履歴を読み取れる。

`DurableOperation.request_digest` は元の Manager `TransitionIssueArgs` protobuf の SHA-256 digest であり、`request_snapshot_digest` と Outbox の `payload_digest` は内部 `DurableBackendRequest` の SHA-256 digest である。これらは異なる payload の digest なので直接比較しない。trusted read は保存済み内部要求と Manager audit から元の protobuf を再構成し、同じ Prost encoding と digest 経路で得た値が `request_digest` と完全一致することを要求する。内部 snapshot が省略する `lease_handle`（`Some([])` の presence を含む）や `awaiting_review` を元要求から再構成できない場合は fail-closed となる。そのような要求の trusted read をサポートするには、元の Manager protobuf snapshot を保存する上流変更が必要である。

履歴の `page_size` は 1〜100 に制限し、要求および符号化応答は 4 MiB 以下とする。履歴 service は既存 native `HistoryPage` を返し、TaskChampion の write RPC や `ActivityService` を登録しない。caller audit metadata は認証・認可ではなく、認証は従来どおり primary trusted-consumer 境界が担う。

### Actor audit history

Tracker の監査履歴は actor ID を維持し、protobuf の `ActorKind` を保存・返却する。Manager 管理操作の再起動復旧も、保存済み要求に含まれる actor kind、agent session ID、correlation ID を同じ履歴へ引き継ぐ。kind を持たない旧 TaskChampion 履歴は従来どおり `InternalAgent` と解釈し、不明または破損した kind は履歴読取時に拒否する。actor metadata は監査用であり、認可情報ではない。人間限定の管理操作の制約は従来どおり適用する。管理操作は actor ID が空、または actor kind が `ACTOR_KIND_UNSPECIFIED` の要求を拒否し、correlation ID が空白のみの場合は downstream operation ID を correlation として記録する。

この厳格な履歴読取とは異なり、Issue metadata の actor 投影で不正な kind を検出した場合は `Unspecified` を返す。`Unspecified`（enum 0）は正規値であり、Human や System として補完しない。

## 互換性と生成物

manager contract `issue-tracker-manager/v14` は SQLite schema v31 に対応する。v30（または v29）root は operation-history projection の検証と direct/state/order index の追加を行う migration に対応し、それより古い contract root は互換対象でない。移行手順は[ローカル gRPC Server 仕様](grpc-server-spec.md)を参照する。

protoc の生成物、descriptor set、service trait、listener allowlist は一つの変更として更新する。公開 service の method 集合と listener ごとの到達性は descriptor テスト及び実 TCP 結合テストで固定する。
