use crate::error::TrackerError;
use sha2::{Digest, Sha256};

const TOKEN_VERSION: &str = "manager-page-v1";
const OPERATION_HISTORY_TOKEN_VERSION: &str = "operation-history-page-v2";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PageCursor {
    pub(crate) offset: usize,
    pub(crate) generation: u64,
    pub(crate) snapshot_id: String,
    pub(crate) order_fingerprint: String,
    pub(crate) query_fingerprint: String,
    pub(crate) expires_at_unix_epoch_millis: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct PageTokenCodec {
    secret: String,
}

/// Keyset cursor used exclusively by operation-history reads. Keeping this
/// codec separate prevents a v2 token from being accidentally accepted by
/// the offset-paged manager entity APIs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OperationHistoryPageCursor {
    pub(crate) created_at_unix_epoch_millis: i64,
    pub(crate) operation_id: String,
    pub(crate) generation: u64,
    pub(crate) snapshot_id: String,
    pub(crate) order_fingerprint: String,
    pub(crate) query_fingerprint: String,
    pub(crate) version: u32,
    pub(crate) expires_at_unix_epoch_millis: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct OperationHistoryPageTokenCodec {
    secret: String,
}

/// Cursor variants accepted by the operation-history compatibility boundary.
/// V1 remains offset based only while the migration flag is enabled; v2 is
/// always keyset based and uses its own discriminator and codec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HistoryCursor {
    Keyset(OperationHistoryPageCursor),
    Offset(usize),
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct HistoryCursorBoundary<'a> {
    pub(crate) generation: u64,
    pub(crate) snapshot_id: &'a str,
    pub(crate) order_fingerprint: &'a str,
    pub(crate) query_fingerprint: &'a str,
}

/// Decode an operation-history continuation token and bind it to the current
/// read boundary. Keeping discriminator handling here prevents the repository
/// list method from accidentally accepting a token issued for another API.
pub(crate) fn decode_operation_history_cursor(
    token: &str,
    secret: &str,
    boundary: HistoryCursorBoundary<'_>,
    operation_history_v1_compat_enabled: bool,
    now_unix_epoch_millis: i64,
) -> Result<HistoryCursor, TrackerError> {
    if token.starts_with("operation-history-page-v2|") {
        return OperationHistoryPageTokenCodec::new(secret)
            .decode(
                token,
                boundary.generation,
                boundary.snapshot_id,
                boundary.order_fingerprint,
                boundary.query_fingerprint,
                now_unix_epoch_millis,
            )
            .map(HistoryCursor::Keyset);
    }
    if token.starts_with("manager-page-v1|") && operation_history_v1_compat_enabled {
        let codec = PageTokenCodec::new(secret);
        return codec
            .decode_authenticated(token)
            .and_then(|cursor| {
                codec.validate_boundary(
                    cursor,
                    boundary.generation,
                    boundary.snapshot_id,
                    boundary.order_fingerprint,
                    boundary.query_fingerprint,
                    now_unix_epoch_millis,
                )
            })
            .map(|cursor| HistoryCursor::Offset(cursor.offset));
    }
    Err(TrackerError::InvalidPageToken)
}

impl OperationHistoryPageTokenCodec {
    pub(crate) fn new(secret: impl Into<String>) -> Self {
        Self {
            secret: secret.into(),
        }
    }

    pub(crate) fn encode(&self, cursor: &OperationHistoryPageCursor) -> String {
        let payload = format!(
            "{OPERATION_HISTORY_TOKEN_VERSION}|{}|{}|{}|{}|{}|{}|{}|{}",
            cursor.created_at_unix_epoch_millis,
            escape(&cursor.operation_id),
            cursor.generation,
            escape(&cursor.snapshot_id),
            escape(&cursor.order_fingerprint),
            escape(&cursor.query_fingerprint),
            cursor.version,
            cursor.expires_at_unix_epoch_millis,
        );
        let signature = digest(&self.secret, &payload);
        format!("{payload}|{signature}")
    }

    pub(crate) fn decode(
        &self,
        token: &str,
        expected_generation: u64,
        expected_snapshot_id: &str,
        expected_order_fingerprint: &str,
        expected_query_fingerprint: &str,
        now_unix_epoch_millis: i64,
    ) -> Result<OperationHistoryPageCursor, TrackerError> {
        let parts = token.split('|').collect::<Vec<_>>();
        if parts.len() != 10 || parts[0] != OPERATION_HISTORY_TOKEN_VERSION {
            return Err(TrackerError::InvalidPageToken);
        }
        let payload = parts[..9].join("|");
        if digest(&self.secret, &payload) != parts[9] {
            return Err(TrackerError::InvalidPageToken);
        }
        let cursor = OperationHistoryPageCursor {
            created_at_unix_epoch_millis: parts[1]
                .parse()
                .map_err(|_| TrackerError::InvalidPageToken)?,
            operation_id: unescape(parts[2])?,
            generation: parts[3]
                .parse()
                .map_err(|_| TrackerError::InvalidPageToken)?,
            snapshot_id: unescape(parts[4])?,
            order_fingerprint: unescape(parts[5])?,
            query_fingerprint: unescape(parts[6])?,
            version: parts[7]
                .parse()
                .map_err(|_| TrackerError::InvalidPageToken)?,
            expires_at_unix_epoch_millis: parts[8]
                .parse()
                .map_err(|_| TrackerError::InvalidPageToken)?,
        };
        if cursor.version != 2 || cursor.operation_id.is_empty() {
            return Err(TrackerError::InvalidPageToken);
        }
        if cursor.generation != expected_generation {
            return Err(TrackerError::PageTokenExpired {
                token_generation: cursor.generation,
                current_generation: expected_generation,
            });
        }
        if cursor.snapshot_id != expected_snapshot_id
            || cursor.order_fingerprint != expected_order_fingerprint
            || cursor.query_fingerprint != expected_query_fingerprint
        {
            return Err(TrackerError::PageTokenExpired {
                token_generation: cursor.generation,
                current_generation: expected_generation,
            });
        }
        if now_unix_epoch_millis > cursor.expires_at_unix_epoch_millis {
            return Err(TrackerError::PageTokenExpired {
                token_generation: cursor.generation,
                current_generation: expected_generation,
            });
        }
        Ok(cursor)
    }
}

impl PageTokenCodec {
    pub(crate) fn new(secret: impl Into<String>) -> Self {
        Self {
            secret: secret.into(),
        }
    }

    pub(crate) fn encode(&self, cursor: &PageCursor) -> String {
        let payload = format!(
            "{TOKEN_VERSION}|{}|{}|{}|{}|{}|{}",
            cursor.offset,
            cursor.generation,
            escape(&cursor.snapshot_id),
            escape(&cursor.order_fingerprint),
            escape(&cursor.query_fingerprint),
            cursor.expires_at_unix_epoch_millis,
        );
        let signature = digest(&self.secret, &payload);
        format!("{payload}|{signature}")
    }

    pub(crate) fn decode(
        &self,
        token: &str,
        expected_generation: u64,
        expected_snapshot_id: &str,
        expected_order_fingerprint: &str,
        expected_query_fingerprint: &str,
        now_unix_epoch_millis: i64,
    ) -> Result<PageCursor, TrackerError> {
        let cursor = self.decode_authenticated(token)?;
        self.validate_boundary(
            cursor,
            expected_generation,
            expected_snapshot_id,
            expected_order_fingerprint,
            expected_query_fingerprint,
            now_unix_epoch_millis,
        )
    }

    /// Decodes a token after authenticating its self-contained cursor data.
    ///
    /// Callers must still bind the result to their current read boundary with
    /// [`Self::validate_boundary`] before using its offset.
    pub(crate) fn decode_authenticated(&self, token: &str) -> Result<PageCursor, TrackerError> {
        let parts = token.split('|').collect::<Vec<_>>();
        if parts.len() != 8 || parts[0] != TOKEN_VERSION {
            return Err(TrackerError::InvalidPageToken);
        }
        let payload = parts[..7].join("|");
        if digest(&self.secret, &payload) != parts[7] {
            return Err(TrackerError::InvalidPageToken);
        }
        let offset = parts[1]
            .parse::<usize>()
            .map_err(|_| TrackerError::InvalidPageToken)?;
        let generation = parts[2]
            .parse::<u64>()
            .map_err(|_| TrackerError::InvalidPageToken)?;
        let snapshot_id = unescape(parts[3])?;
        let order_fingerprint = unescape(parts[4])?;
        let query_fingerprint = unescape(parts[5])?;
        let expires_at_unix_epoch_millis = parts[6]
            .parse::<i64>()
            .map_err(|_| TrackerError::InvalidPageToken)?;
        Ok(PageCursor {
            offset,
            generation,
            snapshot_id,
            order_fingerprint,
            query_fingerprint,
            expires_at_unix_epoch_millis,
        })
    }

    /// Binds an authenticated cursor to the current list traversal.
    pub(crate) fn validate_boundary(
        &self,
        cursor: PageCursor,
        expected_generation: u64,
        expected_snapshot_id: &str,
        expected_order_fingerprint: &str,
        expected_query_fingerprint: &str,
        now_unix_epoch_millis: i64,
    ) -> Result<PageCursor, TrackerError> {
        if cursor.generation != expected_generation {
            return Err(TrackerError::PageTokenExpired {
                token_generation: cursor.generation,
                current_generation: expected_generation,
            });
        }
        if cursor.snapshot_id != expected_snapshot_id
            || cursor.order_fingerprint != expected_order_fingerprint
            || cursor.query_fingerprint != expected_query_fingerprint
            || now_unix_epoch_millis > cursor.expires_at_unix_epoch_millis
        {
            return Err(TrackerError::PageTokenExpired {
                token_generation: cursor.generation,
                current_generation: expected_generation,
            });
        }
        Ok(cursor)
    }
}

/// Decodes a token and binds it to the current list traversal.
pub(crate) fn decode_bound_page_token(
    codec: &PageTokenCodec,
    token: &str,
    expected_generation: u64,
    expected_snapshot_id: &str,
    expected_order_fingerprint: &str,
    expected_query_fingerprint: &str,
    now_unix_epoch_millis: i64,
) -> Result<PageCursor, TrackerError> {
    let cursor = codec.decode_authenticated(token)?;
    codec.validate_boundary(
        cursor,
        expected_generation,
        expected_snapshot_id,
        expected_order_fingerprint,
        expected_query_fingerprint,
        now_unix_epoch_millis,
    )
}

/// Derive the token generation that binds a read to both stores.
pub(crate) fn composite_generation(backend_generation: u64, manager_generation: u64) -> u64 {
    let digest = Sha256::digest(format!("{backend_generation}:{manager_generation}").as_bytes());
    u64::from_be_bytes(
        digest[..8]
            .try_into()
            .expect("SHA-256 digest has eight bytes"),
    )
}

/// Derive the stable snapshot identifier for a paired backend/manager read.
pub(crate) fn composite_snapshot_id(backend_generation: u64, manager_generation: u64) -> String {
    format!("composite-{backend_generation}-{manager_generation}")
}

pub(crate) fn paginate<T>(
    values: &[T],
    page_size: usize,
    cursor: Option<&PageCursor>,
) -> (usize, usize, bool) {
    let start = cursor.map_or(0, |cursor| cursor.offset).min(values.len());
    let end = start.saturating_add(page_size.max(1)).min(values.len());
    (start, end, end < values.len())
}

/// Validated list limits that bound every externally requested page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PageSizeLimits {
    default_page_size: u32,
    maximum_page_size: u32,
}

impl PageSizeLimits {
    pub(crate) fn new(default_page_size: u32, maximum_page_size: u32) -> Self {
        debug_assert!(default_page_size > 0);
        debug_assert!(maximum_page_size > 0);
        debug_assert!(default_page_size <= maximum_page_size);
        Self {
            default_page_size,
            maximum_page_size,
        }
    }

    pub(crate) fn resolve(self, requested_page_size: u32) -> usize {
        let requested = if requested_page_size == 0 {
            self.default_page_size
        } else {
            requested_page_size
        };
        requested.min(self.maximum_page_size) as usize
    }
}

fn digest(secret: &str, payload: &str) -> String {
    format!(
        "{:x}",
        Sha256::digest(format!("{secret}\0{payload}").as_bytes())
    )
}

fn escape(value: &str) -> String {
    value.replace('%', "%25").replace('|', "%7C")
}

fn unescape(value: &str) -> Result<String, TrackerError> {
    let mut output = Vec::with_capacity(value.len());
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err(TrackerError::InvalidPageToken);
            }
            let hex = std::str::from_utf8(&bytes[index + 1..index + 3])
                .map_err(|_| TrackerError::InvalidPageToken)?;
            let value = u8::from_str_radix(hex, 16).map_err(|_| TrackerError::InvalidPageToken)?;
            output.push(value);
            index += 3;
        } else {
            output.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(output).map_err(|_| TrackerError::InvalidPageToken)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cursor() -> PageCursor {
        PageCursor {
            offset: 2,
            generation: 7,
            snapshot_id: "snapshot|1".into(),
            order_fingerprint: "candidate-id".into(),
            query_fingerprint: "filter-hash".into(),
            expires_at_unix_epoch_millis: 100,
        }
    }

    #[test]
    fn opaque_token_round_trips_and_binds_query_snapshot_and_generation() {
        let codec = PageTokenCodec::new("secret");
        let token = codec.encode(&cursor());
        assert!(!token.contains("snapshot|1"));
        assert_eq!(
            codec
                .decode(&token, 7, "snapshot|1", "candidate-id", "filter-hash", 99)
                .unwrap(),
            cursor()
        );
        assert_eq!(
            codec
                .decode(&token, 7, "snapshot|1", "candidate-id", "filter-hash", 100)
                .unwrap(),
            cursor()
        );
        assert!(matches!(
            codec.decode(&token, 8, "snapshot|1", "candidate-id", "filter-hash", 99),
            Err(TrackerError::PageTokenExpired { .. })
        ));
        assert!(matches!(
            codec.decode(&token, 7, "other", "candidate-id", "filter-hash", 99),
            Err(TrackerError::PageTokenExpired {
                token_generation: 7,
                current_generation: 7,
            })
        ));
    }

    #[test]
    fn modified_or_expired_tokens_are_rejected() {
        let codec = PageTokenCodec::new("secret");
        let token = codec.encode(&cursor());
        let mut modified = token.clone().into_bytes();
        let last = modified.len() - 1;
        modified[last] = if modified[last] == b'a' { b'b' } else { b'a' };
        assert!(matches!(
            codec.decode(
                std::str::from_utf8(&modified).unwrap(),
                7,
                "snapshot|1",
                "candidate-id",
                "filter-hash",
                99
            ),
            Err(TrackerError::InvalidPageToken)
        ));
        assert!(matches!(
            codec.decode(&token, 7, "snapshot|1", "candidate-id", "filter-hash", 101),
            Err(TrackerError::PageTokenExpired {
                token_generation: 7,
                current_generation: 7,
            })
        ));
    }

    #[test]
    fn operation_history_v2_cursor_round_trips_and_boundary_mismatch_expires_it() {
        let codec = OperationHistoryPageTokenCodec::new("secret");
        let cursor = OperationHistoryPageCursor {
            created_at_unix_epoch_millis: 42,
            operation_id: "op|1".into(),
            generation: 7,
            snapshot_id: "manager-7".into(),
            order_fingerprint: "created-at-desc-operation-id-desc".into(),
            query_fingerprint: "query".into(),
            version: 2,
            expires_at_unix_epoch_millis: i64::MAX,
        };
        let token = codec.encode(&cursor);
        assert_eq!(
            codec
                .decode(
                    &token,
                    7,
                    "manager-7",
                    "created-at-desc-operation-id-desc",
                    "query",
                    99,
                )
                .unwrap(),
            cursor
        );
        assert!(matches!(
            codec.decode(
                &token,
                8,
                "manager-8",
                "created-at-desc-operation-id-desc",
                "query",
                99,
            ),
            Err(TrackerError::PageTokenExpired { .. })
        ));
        assert!(matches!(
            codec.decode(&token, 7, "manager-7", "other-order", "query", 99),
            Err(TrackerError::PageTokenExpired {
                token_generation: 7,
                current_generation: 7,
            })
        ));
    }

    #[test]
    fn operation_history_v2_keeps_structural_failures_invalid_and_expiry_expired() {
        let codec = OperationHistoryPageTokenCodec::new("secret");
        let cursor = OperationHistoryPageCursor {
            created_at_unix_epoch_millis: 42,
            operation_id: "op-1".into(),
            generation: 7,
            snapshot_id: "manager-7".into(),
            order_fingerprint: "order".into(),
            query_fingerprint: "query".into(),
            version: 2,
            expires_at_unix_epoch_millis: 100,
        };
        let token = codec.encode(&cursor);
        assert!(matches!(
            codec.decode(&token, 7, "manager-7", "order", "query", 101),
            Err(TrackerError::PageTokenExpired {
                token_generation: 7,
                current_generation: 7,
            })
        ));

        let mut wrong_version = cursor.clone();
        wrong_version.version = 1;
        let token = codec.encode(&wrong_version);
        assert!(matches!(
            codec.decode(&token, 7, "manager-7", "order", "query", 0),
            Err(TrackerError::InvalidPageToken)
        ));

        let mut missing_operation_id = cursor;
        missing_operation_id.operation_id.clear();
        let token = codec.encode(&missing_operation_id);
        assert!(matches!(
            codec.decode(&token, 7, "manager-7", "order", "query", 0),
            Err(TrackerError::InvalidPageToken)
        ));
    }

    #[test]
    fn operation_history_cursor_decoder_keeps_v1_and_v2_discriminators_separate() {
        let v1 = PageTokenCodec::new("secret").encode(&PageCursor {
            offset: 1,
            generation: 7,
            snapshot_id: "manager-7".into(),
            order_fingerprint: "order".into(),
            query_fingerprint: "query".into(),
            expires_at_unix_epoch_millis: i64::MAX,
        });
        assert!(matches!(
            decode_operation_history_cursor(
                &v1,
                "secret",
                HistoryCursorBoundary {
                    generation: 7,
                    snapshot_id: "manager-7",
                    order_fingerprint: "order",
                    query_fingerprint: "query",
                },
                true,
                0,
            ),
            Ok(HistoryCursor::Offset(1))
        ));
        assert!(matches!(
            decode_operation_history_cursor(
                &v1,
                "secret",
                HistoryCursorBoundary {
                    generation: 7,
                    snapshot_id: "manager-7",
                    order_fingerprint: "order",
                    query_fingerprint: "query",
                },
                false,
                0,
            ),
            Err(TrackerError::InvalidPageToken)
        ));
        let unknown = v1.replacen("manager-page-v1", "unknown-page-v9", 1);
        assert!(matches!(
            decode_operation_history_cursor(
                &unknown,
                "secret",
                HistoryCursorBoundary {
                    generation: 7,
                    snapshot_id: "manager-7",
                    order_fingerprint: "order",
                    query_fingerprint: "query",
                },
                true,
                0,
            ),
            Err(TrackerError::InvalidPageToken)
        ));
    }

    #[test]
    fn utf8_query_values_round_trip_without_becoming_cursor_data() {
        let codec = PageTokenCodec::new("secret");
        let mut value = cursor();
        value.query_fingerprint = "要約/候補".into();
        let token = codec.encode(&value);
        assert_eq!(
            codec
                .decode(&token, 7, "snapshot|1", "candidate-id", "要約/候補", 99)
                .unwrap(),
            value
        );
    }

    #[test]
    fn encoded_token_does_not_reveal_the_codec_secret() {
        let codec = PageTokenCodec::new("not-for-clients");
        let token = codec.encode(&cursor());
        assert!(!token.contains("not-for-clients"));
    }

    #[test]
    fn authenticated_decode_preserves_cursor_before_boundary_validation() {
        let codec = PageTokenCodec::new("secret");
        let token = codec.encode(&cursor());

        assert_eq!(codec.decode_authenticated(&token).unwrap(), cursor());
    }

    #[test]
    fn bound_decode_classifies_authenticated_boundary_mismatches_as_expired() {
        let codec = PageTokenCodec::new("secret");
        let token = codec.encode(&cursor());

        assert!(matches!(
            decode_bound_page_token(
                &codec,
                &token,
                8,
                "other-snapshot",
                "other-order",
                "other-query",
                99,
            ),
            Err(TrackerError::PageTokenExpired {
                token_generation: 7,
                current_generation: 8,
            })
        ));
        assert!(matches!(
            decode_bound_page_token(
                &codec,
                &token,
                7,
                "other-snapshot",
                "candidate-id",
                "filter-hash",
                99
            ),
            Err(TrackerError::PageTokenExpired {
                token_generation: 7,
                current_generation: 7,
            })
        ));
        assert!(matches!(
            decode_bound_page_token(
                &codec,
                &token,
                7,
                "snapshot|1",
                "other-order",
                "filter-hash",
                99
            ),
            Err(TrackerError::PageTokenExpired {
                token_generation: 7,
                current_generation: 7,
            })
        ));
        assert!(matches!(
            decode_bound_page_token(
                &codec,
                &token,
                7,
                "snapshot|1",
                "candidate-id",
                "other-query",
                99
            ),
            Err(TrackerError::PageTokenExpired {
                token_generation: 7,
                current_generation: 7,
            })
        ));
        assert!(matches!(
            decode_bound_page_token(
                &codec,
                &token,
                8,
                "snapshot|1",
                "candidate-id",
                "filter-hash",
                101
            ),
            Err(TrackerError::PageTokenExpired {
                token_generation: 7,
                current_generation: 8,
            })
        ));
        assert!(matches!(
            decode_bound_page_token(
                &codec,
                &token,
                7,
                "snapshot|1",
                "candidate-id",
                "filter-hash",
                101
            ),
            Err(TrackerError::PageTokenExpired {
                token_generation: 7,
                current_generation: 7,
            })
        ));
    }

    #[test]
    fn pagination_never_returns_an_offset_beyond_the_collection() {
        let values = [1, 2, 3];
        assert_eq!(paginate(&values, 2, None), (0, 2, true));
        assert_eq!(
            paginate(
                &values,
                2,
                Some(&PageCursor {
                    offset: 99,
                    ..cursor()
                })
            ),
            (3, 3, false)
        );
    }

    #[test]
    fn page_size_limits_apply_the_default_and_maximum() {
        let limits = PageSizeLimits::new(2, 3);
        assert_eq!(limits.resolve(0), 2);
        assert_eq!(limits.resolve(1), 1);
        assert_eq!(limits.resolve(3), 3);
        assert_eq!(limits.resolve(u32::MAX), 3);
    }
}
