# ログ出力の運用手順

`issue-tracker-server` は起動時に作業ディレクトリの `.env` を読み込み、その後に環境変数からログ設定を決定する。この文書は file、標準出力、および OpenTelemetry（OTLP）へのログ出力の設定手順を定める。

## file と標準出力

| 環境変数 | 既定値 | 内容 |
|---|---|---|
| `LOG_LEVEL` | `INFO` | 出力する最小レベル（`TRACE`、`DEBUG`、`INFO`、`WARN`、`ERROR`）。 |
| `LOG_FILE_DIR` | 作業ディレクトリ | ログ file の出力先。存在しない場合は作成する。 |
| `LOG_USE_JSON` | `false` | `true` の場合、file と標準出力を JSON 形式で出力する。 |
| `LOG_USE_STDOUT` | `true` | `true` の場合、標準出力にもログを出力する。 |
| `LOG_APP_NAME` | 既定のサービス名 | OTLP へ送るサービス名。 |

ログ file 名は `issue-tracker-server_<IPv4 アドレスの 16 進表記>.log` であり、起動のたびに作り直す。

`LOG_*` を一つでも設定する場合は、`LOG_USE_JSON` と `LOG_USE_STDOUT` の両方を明示する。どちらかが欠けると `LOG_*` 全体が読み込めず、すべての項目が既定値になる。

`RUST_LOG` を指定すると、`LOG_LEVEL` に加えて target 単位の絞り込みを適用できる。

### 設定例

```dotenv
LOG_LEVEL=INFO
LOG_FILE_DIR=/var/log/issue-tracker
LOG_USE_JSON=true
LOG_USE_STDOUT=false
```

## OpenTelemetry（OTLP）

`OTLP_ADDR` または `OTLP_HTTP_ADDR` を設定すると、trace と metric を OTLP collector へ送る。ログの送信は `OTLP_LOG_PROTOCOL` を設定した場合だけ有効になる。

| 環境変数 | 内容 |
|---|---|
| `OTLP_ADDR` | gRPC の OTLP endpoint（例: `http://127.0.0.1:4317`）。 |
| `OTLP_HTTP_ADDR` | HTTP の OTLP endpoint。`OTLP_ADDR` が未設定の場合に使う。 |
| `OTLP_AUTH_TOKEN` | 指定した場合、`Authorization: Basic <token>` を付与する。 |
| `OTLP_LOG_PROTOCOL` | ログの送信方式。`grpc`、`http`（`http/protobuf`）、`auto`、`none`（既定）。 |
| `OTLP_LOG_ENDPOINT` | ログ送信先。未設定の場合は `OTLP_ADDR` を使う。 |

### 設定例

```dotenv
OTLP_ADDR=http://127.0.0.1:4317
OTLP_LOG_PROTOCOL=grpc
LOG_APP_NAME=issue-tracker-server
LOG_USE_JSON=false
LOG_USE_STDOUT=true
```

## 注意事項

- 認証 token などの秘密情報は `.env` に直接記載せず、実行環境の secret 管理機構から環境変数として渡す。
- `.env` は作業ディレクトリから読み込むため、service manager から起動する場合は作業ディレクトリを明示する。
