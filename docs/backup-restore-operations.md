# バックアップと復元の運用手順

この文書は、Tracker SQLite artifact の作成、保管、および Rust library API を使う復元の運用手順を定める。公開 protobuf 契約の詳細は [`protobuf/v1`](../protobuf/v1) を正本とし、manager 契約は [`protobuf/manager/v1`](../protobuf/manager/v1) を正本とする。

## 対象範囲と API 境界

canonical plugin API として公開される artifact 操作は `create_backup`、`export_issues`、`get_artifact`、`list_artifacts` のみである。`create_backup` と `export_issues` は artifact のメタデータを含む書込み結果を返し、`get_artifact` と `list_artifacts` は検証済み artifact のメタデータだけを返す。これらの API は artifact の bytes を返さない。

`Tracker::restore` と `Tracker::restore_current` は Rust library API であり、Plugin API ではない。Plugin 呼出しから任意パスへの復元や稼働中 root の置換を行ってはならない。以前検討された manager による coordinated artifact、backup/export/preflight/restore は manager 契約から撤回済みであり、公開機能ではない。

manager DB と manager artifacts は canonical backup の対象外である。日常バックアップの頻度、保持期間、外部保管先、復元を実施する判断は plugin の責務ではなく、consumer または運用者が定める。

## Artifact の内容

`create_backup` が生成・復元対象とする backup artifact は data root 配下の `artifacts/backup-<digest>/` にあり、次のファイルで構成される。

```text
artifacts/backup-<digest>/
├── manifest.json
├── taskchampion.sqlite3
├── taskchampion.sqlite3-wal  # 存在する場合だけ
└── taskchampion.sqlite3-shm  # 存在する場合だけ
```

対象は Tracker SQLite の主 DB と、存在時の WAL/SHM のみである。backup 実行時には SQLite checkpoint を行い、各対象ファイルに secret-like value がないことを検査する。`manifest.json` は contract version、storage schema fingerprint、および各対象ファイルの `sha256:` digest を持つ。artifact directory に上記以外のファイルがあっても、それらは復元対象外である。復元時は主 DB の存在、manifest に記載された artifact 名、manifest の digest、contract、schema fingerprint を検証する。

`export_issues` は `artifacts/export-<digest>/export.json` を作成する。これは backend-neutral な Issue export であり、復元入力ではない。`ExportIssuesArgs` にある `filter`、`include_history`、`include_comments` は現時点の artifact 生成に反映されないため、絞込み済み export や history/comment の含有を前提にしてはならない。

## バックアップの作成と外部保管

1. consumer 側で、書込み権限、呼出し主体、operation ID の一意性を確認してから `create_backup` を呼び出す。必要なら label を渡せるが、artifact 名や内容の選択に使われるものとして扱わない。
2. 成功結果の `ArtifactMutation.artifact` を記録する。`artifact_id`、`relative_path`、`digest`、`size_bytes`、`contract_fingerprint`、`storage_schema_fingerprint`、`artifact_kind`、`format`、`media_type`、`format_version`、`validation_status` を確認し、`artifact_kind=BACKUP` と `validation_status=VALIDATED` を満たすことを確認する。
3. 必要に応じて `get_artifact` または `list_artifacts` で同じ metadata を再確認する。これらは bytes を返さないため、artifact 本体の取得手段として使わない。
4. 運用者が data root 内の `relative_path` で示される artifact directory を、認可済みの外部保管先へ directory 構造を保ったままコピーする。コピー後は `manifest.json` と各 DB ファイルを使って digest を独立に照合し、artifact metadata と保管先の対応を記録する。

artifact directory は runner の data root にあるため、data root 全体をそのまま backup とみなしてはならない。manager DB、manager artifacts、他の root 内ファイルはこの手順の対象外である。

## Rust library API による復元

復元を実行する consumer は、停止、認可、稼働 root の置換を担う。以下の手順は Plugin API の呼出しではなく、同一バージョン互換の Rust library から実行する。

1. 外部保管から backup directory を完全な tree のまま取得する。`export.json` は入力に使わない。
2. 新規の destination path を選ぶ。`Tracker::restore(backup, destination)` は destination が未存在であり、その親 directory が存在する場合だけ受け付ける。既存の root を destination に指定しない。
3. `Tracker::restore` を呼び出す。manifest、digest、contract、schema fingerprint、manifest に記載された artifact 名を検証し、staging directory へコピーしてから reopen と Issue 読取りを検証する。検証に失敗した場合は destination を稼働 root として扱わない。metadata mapping schema の存在と整合性は、`restore_current` の追加検証対象である。
4. 別 root への復元結果を確認してから、consumer/運用者の停止・認可済みの切替手順で採用する。稼働中 root の置換判断と実施は consumer/運用の責務である。
5. library の `Tracker::restore_current(backup)` を利用する場合も、consumer が先に停止と認可を完了する。この API は書込みを freeze し、staging で検証してから SQLite artifacts を置換し、再 open と検証を行う。置換または再 open/検証に失敗すると rollback を試み、復旧できない場合は fail-closed になる。成功確認前に旧 root の削除・通常運用への復帰を行ってはならない。

復元後は consumer が application-level の稼働確認、接続先切替、監査記録を行う。これらと日常バックアップ、retention、migration rollback の設計は本 plugin の範囲外である。
