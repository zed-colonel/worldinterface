//! Layered string matching for edit resilience (E10-S1, W-95).
//!
//! Three matching layers, tried in order:
//! 1. Exact
//! 2. Whitespace-normalized
//! 3. Fuzzy

/// Which matching layer produced the result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchLayer {
    Exact,
    WhitespaceNormalized,
    Fuzzy,
}

/// Result of a successful match.
#[derive(Debug, Clone)]
pub struct MatchResult {
    pub layer: MatchLayer,
    pub matched_text: String,
    pub count: usize,
    pub similarity: f64,
    pub line_number: usize,
}

/// Info about the closest match when no match is found.
#[derive(Debug, Clone)]
pub struct ClosestMatchInfo {
    pub similarity: f64,
    pub line_number: usize,
    pub preview: String,
}

pub fn find_match(haystack: &str, needle: &str, fuzzy_threshold: f64) -> Option<MatchResult> {
    let exact_count = haystack.matches(needle).count();
    if exact_count > 0 {
        return Some(MatchResult {
            layer: MatchLayer::Exact,
            matched_text: needle.to_string(),
            count: exact_count,
            similarity: 1.0,
            line_number: line_number_of(haystack, needle),
        });
    }

    if let Some(result) = try_whitespace_normalized(haystack, needle) {
        return Some(result);
    }

    try_fuzzy_match(haystack, needle, fuzzy_threshold)
}

pub fn closest_match_info(haystack: &str, needle: &str) -> Option<ClosestMatchInfo> {
    let needle_lines: Vec<&str> = needle.lines().collect();
    let haystack_lines: Vec<&str> = haystack.lines().collect();
    let needle_len = needle_lines.len().max(1);

    if haystack_lines.is_empty() {
        return None;
    }

    let mut best_similarity = 0.0_f64;
    let mut best_line = 0_usize;

    for start in 0..haystack_lines.len() {
        let end = (start + needle_len).min(haystack_lines.len());
        let window = haystack_lines[start..end].join("\n");
        let sim = normalized_levenshtein(&window, needle);
        if sim > best_similarity {
            best_similarity = sim;
            best_line = start;
        }
    }

    let preview_end = (best_line + 3).min(haystack_lines.len());
    let preview = haystack_lines[best_line..preview_end].join("\n");

    Some(ClosestMatchInfo { similarity: best_similarity, line_number: best_line + 1, preview })
}

fn normalize_whitespace(s: &str) -> String {
    s.lines()
        .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
        .collect::<Vec<_>>()
        .join("\n")
}

fn try_whitespace_normalized(haystack: &str, needle: &str) -> Option<MatchResult> {
    let norm_needle = normalize_whitespace(needle);
    let norm_haystack = normalize_whitespace(haystack);

    if norm_needle.is_empty() || !norm_haystack.contains(&norm_needle) {
        return None;
    }

    let needle_lines: Vec<&str> = needle.lines().collect();
    let haystack_lines: Vec<&str> = haystack.lines().collect();
    let norm_needle_lines: Vec<String> =
        needle_lines.iter().map(|line| normalize_whitespace(line)).collect();

    let mut match_start = None;
    'outer: for start in 0..haystack_lines.len() {
        if start + norm_needle_lines.len() > haystack_lines.len() {
            break;
        }

        for (index, norm_line) in norm_needle_lines.iter().enumerate() {
            if normalize_whitespace(haystack_lines[start + index]) != *norm_line {
                continue 'outer;
            }
        }

        match_start = Some(start);
        break;
    }

    let start = match_start?;
    let original_text = haystack_lines[start..start + needle_lines.len()].join("\n");

    let mut count = 0;
    let mut offset = 0;
    while let Some(pos) = norm_haystack[offset..].find(&norm_needle) {
        count += 1;
        offset += pos + norm_needle.len();
    }

    Some(MatchResult {
        layer: MatchLayer::WhitespaceNormalized,
        matched_text: original_text,
        count,
        similarity: 1.0,
        line_number: start + 1,
    })
}

fn try_fuzzy_match(haystack: &str, needle: &str, threshold: f64) -> Option<MatchResult> {
    let needle_lines: Vec<&str> = needle.lines().collect();
    let haystack_lines: Vec<&str> = haystack.lines().collect();
    let needle_len = needle_lines.len().max(1);

    if haystack_lines.is_empty() {
        return None;
    }

    let mut best_similarity = 0.0_f64;
    let mut best_start = 0_usize;

    for start in 0..haystack_lines.len() {
        let end = (start + needle_len).min(haystack_lines.len());
        let window = haystack_lines[start..end].join("\n");
        let similarity = normalized_levenshtein(&window, needle);
        if similarity > best_similarity {
            best_similarity = similarity;
            best_start = start;
        }
    }

    if best_similarity < threshold {
        return None;
    }

    let end = (best_start + needle_len).min(haystack_lines.len());
    Some(MatchResult {
        layer: MatchLayer::Fuzzy,
        matched_text: haystack_lines[best_start..end].join("\n"),
        count: 1,
        similarity: best_similarity,
        line_number: best_start + 1,
    })
}

fn normalized_levenshtein(a: &str, b: &str) -> f64 {
    if a == b {
        return 1.0;
    }

    let max_len = a.chars().count().max(b.chars().count());
    if max_len == 0 {
        return 1.0;
    }

    let distance = levenshtein_distance(a, b);
    1.0 - (distance as f64 / max_len as f64)
}

fn levenshtein_distance(a: &str, b: &str) -> usize {
    let a_chars: Vec<char> = a.chars().collect();
    let b_chars: Vec<char> = b.chars().collect();
    let m = a_chars.len();
    let n = b_chars.len();

    let mut prev = (0..=n).collect::<Vec<_>>();
    let mut curr = vec![0; n + 1];

    for i in 1..=m {
        curr[0] = i;
        for j in 1..=n {
            let cost = usize::from(a_chars[i - 1] != b_chars[j - 1]);
            curr[j] = (prev[j] + 1).min(curr[j - 1] + 1).min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    prev[n]
}

fn line_number_of(haystack: &str, needle: &str) -> usize {
    match haystack.find(needle) {
        Some(pos) => haystack[..pos].lines().count() + 1,
        None => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_match_returns_exact() {
        let file = "fn main() {\n    println!(\"hello\");\n}\n";
        let needle = "    println!(\"hello\");";
        let result = find_match(file, needle, 0.85);
        assert!(result.is_some());
        let m = result.unwrap();
        assert_eq!(m.layer, MatchLayer::Exact);
        assert_eq!(m.count, 1);
        assert_eq!(m.matched_text, needle);
    }

    #[test]
    fn exact_match_multiple_returns_count() {
        let file = "x\nx\nx\n";
        let result = find_match(file, "x", 0.85);
        assert!(result.is_some());
        let m = result.unwrap();
        assert_eq!(m.layer, MatchLayer::Exact);
        assert_eq!(m.count, 3);
    }

    #[test]
    fn whitespace_normalized_matches_indentation_drift() {
        let file = "fn main() {\n\t\tprintln!(\"hello\");\n}\n";
        let needle = "    println!(\"hello\");";
        assert_eq!(file.matches(needle).count(), 0);

        let result = find_match(file, needle, 0.85);
        assert!(result.is_some());
        let m = result.unwrap();
        assert_eq!(m.layer, MatchLayer::WhitespaceNormalized);
        assert_eq!(m.count, 1);
        assert!(m.matched_text.contains('\t'));
    }

    #[test]
    fn whitespace_normalized_trailing_whitespace() {
        let file = "let x = 1;   \nlet y = 2;\n";
        let needle = "let x = 1;";
        let result = find_match(file, needle, 0.85);
        assert!(result.is_some());
        assert_eq!(result.unwrap().layer, MatchLayer::Exact);
    }

    #[test]
    fn fuzzy_matches_small_differences() {
        let file = "fn calculate_total(items: &[Item]) -> f64 {\n";
        let needle = "fn calculate_total(items: &[Item]) -> f32 {";
        assert_eq!(file.matches(needle).count(), 0);

        let result = find_match(file, needle, 0.85);
        assert!(result.is_some());
        let m = result.unwrap();
        assert_eq!(m.layer, MatchLayer::Fuzzy);
        assert!(m.similarity >= 0.85);
    }

    #[test]
    fn fuzzy_rejects_below_threshold() {
        let file = "completely different text here\n";
        let needle = "fn totally_unrelated_function()";
        assert!(find_match(file, needle, 0.85).is_none());
    }

    #[test]
    fn no_match_returns_none() {
        let file = "hello world\n";
        let needle = "goodbye universe";
        assert!(find_match(file, needle, 0.85).is_none());
    }

    #[test]
    fn multiline_whitespace_normalized() {
        let file = "if condition {\n  let a = 1;\n  let b = 2;\n}\n";
        let needle = "if condition {\n    let a = 1;\n    let b = 2;\n}";
        let result = find_match(file, needle, 0.85);
        assert!(result.is_some());
        let layer = result.unwrap().layer;
        assert!(layer == MatchLayer::Exact || layer == MatchLayer::WhitespaceNormalized);
    }

    #[test]
    fn whitespace_normalized_count_is_non_overlapping() {
        let file = "  a\n  a\n  a\n";
        let needle = "a\na";

        let result = find_match(file, needle, 0.85).unwrap();

        assert_eq!(result.layer, MatchLayer::WhitespaceNormalized);
        assert_eq!(result.count, 1);
    }

    #[test]
    fn closest_match_info_when_no_match() {
        let file = "fn alpha() {}\nfn beta() {}\nfn gamma() {}\n";
        let needle = "fn delta() {}";
        let info = closest_match_info(file, needle).unwrap();
        assert!(info.similarity > 0.0);
        assert!(info.line_number > 0);
        assert!(!info.preview.is_empty());
    }
}
