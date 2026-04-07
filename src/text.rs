use once_cell::sync::Lazy;
use regex::Regex;

/// Remove all symbols except letters, spaces, and punctuation for Ukrainian, Russian, and English texts.
pub fn clean_text(text: &str) -> String {
    static WHITESPACE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"\s+").expect("whitespace regex must compile"));
    static DROP_NON_L_P_SPACE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r#"[^ \p{L}\p{P}]"#).expect("cleanup regex must compile"));

    let spaced = WHITESPACE.replace_all(text, " ");
    let cleaned = DROP_NON_L_P_SPACE.replace_all(&spaced, "");
    let cleaned = cleaned.replace('\t', " ");
    let cleaned = cleaned.replace(&['@', '#', '%', '&', '*', '(', ')'][..], "");
    let cleaned = cleaned.replace("  ", " ");

    cleaned.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_text_removes_symbols_and_digits() {
        let raw = "Hello, world! 123 \n\t Привіт, світ! @#$%^&*() 456";
        let cleaned = clean_text(raw);
        assert_eq!(cleaned, "Hello, world! Привіт, світ!");
    }

    #[test]
    fn clean_text_handles_only_symbols() {
        let raw = "@#$%^&*() 12345 \n\t";
        let cleaned = clean_text(raw);
        assert_eq!(cleaned, "");
    }

    #[test]
    fn clean_text_handles_empty_input() {
        let cleaned = clean_text("");
        assert_eq!(cleaned, "");
    }

    #[test]
    fn clean_text_handles_whitespace_only() {
        let raw = "   \n\t  ";
        let cleaned = clean_text(raw);
        assert_eq!(cleaned, "");
    }

    #[test]
    fn clean_text_preserves_punctuation() {
        let raw = "Hello!!! How's it going???";
        let cleaned = clean_text(raw);
        assert_eq!(cleaned, "Hello!!! How's it going???");
    }
}
