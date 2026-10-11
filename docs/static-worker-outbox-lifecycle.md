# Static worker の outbox driver 運用

`IssueTrackerRunner` を static worker として利用する際の manager availability の確認手順を定める。公開 API の詳細は [`protobuf/manager/v1`](../protobuf/manager/v1) を正本とする。

## 起動と確認

1. static worker を manager settings を含む `IssueTrackerRunnerSettings` で load する。
2. runner は manager と Tracker を開き、`AppResources::start_resident_maintenance` で resident task を開始する。marker recovery と ownership scan を完了するまで manager を ready として公開しない。
3. resident task は30秒周期・1周期100件上限で ownership recovery、marker recovery、outbox delivery を独立に実行する。片方の Issue の失敗は後続候補・配送を停止せず、health だけを `DEGRADED` にする。
4. private unary `manager_get_contract_info` を呼び出す。`contract_version` が `issue-tracker-manager/v14`、`health` が `HEALTH_STATUS_HEALTHY`、`activation` が `ACTIVATION_STATUS_READY`、かつ `features` に `MANAGER_FEATURE_DURABLE_OPERATIONS` があることを確認する。

保守異常により `health` が `DEGRADED`（通常は `activation` も `BLOCKED`）になっている間は、Manager は新規の durable mutation、lease capability、Gateway Claim の admission を fail-closed（型付き `BACKEND_UNAVAILABLE`、再試行可能）で拒否する。既に受付済みの操作の replay／照会、読み取り、及び primary trusted-consumer の管理回復・手動仲裁は継続して利用できる。これにより保守異常を隠さず、回復操作を行う経路も閉じない。

## 停止・再設定時の扱い

1. worker unload、再 load、または settings 更新の開始時、runner は `AppResources::shutdown` / `AppModule::shutdown` で既存 task に停止を通知する。
2. `MaintenanceHandle::shutdown` が task の終了を join してから manager と Tracker の root lock を解放する。handle の暗黙 drop は abort の安全弁であり、通常停止の代用にしない。
3. 新しい settings で同じ起動と確認手順を実行する。recovery 完了前に旧 driver の ready 状態を引き継いではならない。
4. driver が停止した場合（異常終了、unload、再 load を含む）、または recovery が失敗した場合は、manager を `HEALTH_STATUS_DEGRADED` / `ACTIVATION_STATUS_BLOCKED` として扱い、DurableOperations を利用可能と判断しない。
