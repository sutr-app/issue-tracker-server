use super::{conversion::*, operation_log::marker_digest, *};

pub(crate) fn filter_issues(
    mut issues: Vec<Issue>,
    filter: Option<&v1::IssueFilter>,
    sort: i32,
) -> Vec<Issue> {
    if let Some(filter) = filter {
        issues.retain(|issue| {
            (filter.states.is_empty() || filter.states.contains(&issue_state(issue.state)))
                && (filter.kinds.is_empty() || filter.kinds.contains(&issue.kind))
                && (filter.priorities.is_empty() || filter.priorities.contains(&issue.priority))
                && (filter.creation_bases.is_empty()
                    || filter.creation_bases.contains(&issue.creation_basis))
                && (filter.confirmation_statuses.is_empty()
                    || filter
                        .confirmation_statuses
                        .contains(&issue.confirmation_status))
                && filter
                    .labels_all
                    .iter()
                    .all(|label| issue.labels.contains(label))
                && filter.text_query.as_ref().is_none_or(|query| {
                    let query = query.to_ascii_lowercase();
                    issue.title.to_ascii_lowercase().contains(&query)
                        || issue.description.to_ascii_lowercase().contains(&query)
                })
                && filter.due_before.as_ref().is_none_or(|until| {
                    issue
                        .due_at
                        .is_some_and(|due| due <= until.unix_epoch_millis)
                })
                && filter.updated_before.as_ref().is_none_or(|until| {
                    issue.updated_at.saturating_mul(1000) <= until.unix_epoch_millis as u64
                })
        });
    }
    match v1::IssueSort::try_from(sort).unwrap_or(v1::IssueSort::Unspecified) {
        v1::IssueSort::UpdatedDesc => {
            issues.sort_by_key(|issue| std::cmp::Reverse(issue.updated_at))
        }
        v1::IssueSort::CreatedDesc => {
            issues.sort_by_key(|issue| std::cmp::Reverse(issue.created_at))
        }
        v1::IssueSort::CreatedAsc => issues.sort_by_key(|issue| issue.created_at),
        _ => issues.sort_by_key(|issue| issue.id),
    }
    issues
}

pub(crate) fn page_issues(
    issues: Vec<Issue>,
    request: Option<&v1::PageRequest>,
    read_generation: u64,
    query_fingerprint: &str,
    token_secret: &str,
) -> Result<v1::IssuePage, TrackerError> {
    let total = issues.len() as u64;
    let mut issues = issues;
    let page_size = request
        .and_then(|request| (request.page_size > 0).then_some(request.page_size as usize))
        .unwrap_or(usize::MAX);
    if let Some(token) = request.and_then(|request| request.page_token.as_ref()) {
        let last_id =
            decode_issue_page_token(token, read_generation, query_fingerprint, token_secret)?;
        issues.retain(|issue| issue.id > last_id);
    }
    let has_more = issues.len() > page_size;
    issues.truncate(page_size);
    let next_page_token = has_more
        .then(|| {
            issues.last().map(|issue| {
                encode_page_token(
                    read_generation,
                    &issue.id.to_string(),
                    query_fingerprint,
                    token_secret,
                )
            })
        })
        .flatten();
    let watermark = issues.iter().map(|issue| issue.revision).max().unwrap_or(0);
    Ok(v1::IssuePage {
        issues: issues.iter().map(issue_proto).collect(),
        page: Some(v1::PageInfo {
            next_page_token,
            total_size: Some(total),
            snapshot: Some(v1::PageSnapshot {
                read_revision_watermark: watermark,
                snapshot_id: format!("read-{read_generation}"),
                stable_traversal: Some(v1::StableTraversal {
                    order_fingerprint: "issue-id".into(),
                }),
                read_generation,
            }),
        }),
    })
}

pub(crate) fn page_window(
    length: usize,
    request: Option<&v1::PageRequest>,
    read_generation: u64,
    query_fingerprint: &str,
    token_secret: &str,
) -> Result<(usize, usize, Option<String>), TrackerError> {
    let start = request
        .and_then(|request| request.page_token.as_deref())
        .map(|token| {
            decode_window_page_token(token, read_generation, query_fingerprint, token_secret)
        })
        .transpose()?
        .unwrap_or(0)
        .min(length);
    let page_size = request
        .and_then(|request| (request.page_size > 0).then_some(request.page_size as usize))
        .unwrap_or(length.max(1));
    let end = start.saturating_add(page_size).min(length);
    let next = (end < length).then(|| {
        encode_page_token(
            read_generation,
            &end.to_string(),
            query_fingerprint,
            token_secret,
        )
    });
    Ok((start, end, next))
}

pub(crate) fn encode_page_token(
    read_generation: u64,
    cursor: &str,
    query_fingerprint: &str,
    token_secret: &str,
) -> String {
    let signature = marker_digest(&[
        PAGE_TOKEN_VERSION,
        token_secret,
        read_generation.to_string().as_str(),
        cursor,
        query_fingerprint,
    ]);
    format!("{PAGE_TOKEN_VERSION}:{read_generation}:{cursor}:{signature}")
}

pub(crate) fn decode_page_token<'a>(
    token: &'a str,
    read_generation: u64,
    query_fingerprint: &str,
    token_secret: &str,
) -> Result<&'a str, TrackerError> {
    let mut parts = token.split(':');
    let version = parts.next().ok_or(TrackerError::InvalidPageToken)?;
    let token_generation = parts
        .next()
        .ok_or(TrackerError::InvalidPageToken)?
        .parse::<u64>()
        .map_err(|_| TrackerError::InvalidPageToken)?;
    let cursor = parts.next().ok_or(TrackerError::InvalidPageToken)?;
    let signature = parts.next().ok_or(TrackerError::InvalidPageToken)?;
    if parts.next().is_some() || version != PAGE_TOKEN_VERSION {
        return Err(TrackerError::InvalidPageToken);
    }
    let expected = marker_digest(&[
        PAGE_TOKEN_VERSION,
        token_secret,
        token_generation.to_string().as_str(),
        cursor,
        query_fingerprint,
    ]);
    if signature != expected {
        return Err(TrackerError::InvalidPageToken);
    }
    if token_generation != read_generation {
        return Err(TrackerError::PageTokenExpired {
            token_generation,
            current_generation: read_generation,
        });
    }
    Ok(cursor)
}

pub(crate) fn decode_issue_page_token(
    token: &str,
    read_generation: u64,
    query_fingerprint: &str,
    token_secret: &str,
) -> Result<Uuid, TrackerError> {
    let cursor = decode_page_token(token, read_generation, query_fingerprint, token_secret)?;
    Uuid::parse_str(cursor).map_err(|_| TrackerError::InvalidPageToken)
}

pub(crate) fn decode_window_page_token(
    token: &str,
    read_generation: u64,
    query_fingerprint: &str,
    token_secret: &str,
) -> Result<usize, TrackerError> {
    decode_page_token(token, read_generation, query_fingerprint, token_secret)?
        .parse::<usize>()
        .map_err(|_| TrackerError::InvalidPageToken)
}

pub(crate) fn page_snapshot(read_generation: u64, order_fingerprint: &str) -> v1::PageSnapshot {
    v1::PageSnapshot {
        read_revision_watermark: 0,
        snapshot_id: format!("read-{read_generation}"),
        stable_traversal: Some(v1::StableTraversal {
            order_fingerprint: order_fingerprint.to_owned(),
        }),
        read_generation,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tampered_stale_page_token_is_invalid_before_generation_expiry() {
        let token = encode_page_token(7, "cursor", "query", "secret");
        let tampered_and_stale = token.replacen(":7:", ":8:", 1);

        assert!(matches!(
            decode_page_token(&tampered_and_stale, 7, "query", "secret"),
            Err(TrackerError::InvalidPageToken)
        ));
    }
}
