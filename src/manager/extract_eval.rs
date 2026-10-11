use unicode_normalization::UnicodeNormalization;

const JACCARD_THRESHOLD: f64 = 0.8;

pub fn fingerprint(kind: &str, title: &str, completion: &str) -> String {
    format!(
        "{}|{}|{}",
        kind,
        normalize_fingerprint(title),
        normalize_fingerprint(completion)
    )
}

pub fn normalize_fingerprint(value: &str) -> String {
    value
        .nfkc()
        .collect::<String>()
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn normalize_search(value: &str) -> String {
    value
        .nfkc()
        .flat_map(char::to_lowercase)
        .filter(|ch| ch.is_alphanumeric())
        .collect()
}

pub fn is_lexical_duplicate(
    kind_a: &str,
    title_a: &str,
    completion_a: &str,
    kind_b: &str,
    title_b: &str,
    completion_b: &str,
) -> bool {
    if kind_a != kind_b {
        return false;
    }
    let title = jaccard(
        &char_bigrams(&normalize_search(title_a)),
        &char_bigrams(&normalize_search(title_b)),
    );
    let completion = jaccard(
        &char_bigrams(&normalize_search(completion_a)),
        &char_bigrams(&normalize_search(completion_b)),
    );
    title >= JACCARD_THRESHOLD && completion >= JACCARD_THRESHOLD
}

fn char_bigrams(value: &str) -> std::collections::HashSet<String> {
    let chars: Vec<char> = value.chars().collect();
    if chars.len() < 2 {
        return chars.into_iter().map(|ch| ch.to_string()).collect();
    }
    chars
        .windows(2)
        .map(|window| window.iter().collect())
        .collect()
}

fn jaccard(
    left: &std::collections::HashSet<String>,
    right: &std::collections::HashSet<String>,
) -> f64 {
    if left.is_empty() && right.is_empty() {
        return 1.0;
    }
    let intersection = left.intersection(right).count() as f64;
    let union = left.union(right).count() as f64;
    if union == 0.0 {
        0.0
    } else {
        intersection / union
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_keeps_particles_and_folds_width() {
        let left = fingerprint("TASK", "ログイン 修正", "完了");
        let right = fingerprint("TASK", "ログイン　修正", "完了");
        let particle = fingerprint("TASK", "ログインを修正", "完了");
        assert_eq!(left, right);
        assert_ne!(left, particle);
    }

    #[test]
    fn lexical_duplicate_folds_width_and_punctuation_at_threshold() {
        assert!(is_lexical_duplicate(
            "TASK",
            "API修正",
            "APIが直る",
            "TASK",
            "ＡＰＩ-修正",
            "APIが直る"
        ));
        assert!(!is_lexical_duplicate(
            "TASK",
            "ログイン修正",
            "ログインできる",
            "BUG",
            "ログイン修正",
            "ログインできる"
        ));
        assert!(!is_lexical_duplicate(
            "TASK",
            "ログイン修正",
            "ログインできる",
            "TASK",
            "認証バグ修正",
            "認証できる"
        ));
    }
}
