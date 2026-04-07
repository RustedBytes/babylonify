use anyhow::{Result, anyhow};
use lingua::{Language, LanguageDetector, LanguageDetectorBuilder};
use std::collections::HashSet;

pub fn parse_languages(codes: &[String]) -> Result<HashSet<Language>> {
    let codes = if codes.is_empty() {
        vec!["uk".to_string()]
    } else {
        codes.to_vec()
    };

    codes
        .into_iter()
        .map(|code| parse_language(&code))
        .collect()
}

pub fn build_detector() -> LanguageDetector {
    LanguageDetectorBuilder::from_all_languages()
        .with_preloaded_language_models()
        .build()
}

fn parse_language(code: &str) -> Result<Language> {
    let code = code.trim().to_lowercase();
    match code.as_str() {
        "uk" | "ukr" | "ukrainian" | "українська" => Ok(Language::Ukrainian),
        "en" | "eng" | "english" => Ok(Language::English),
        "ru" | "rus" | "russian" | "русский" => Ok(Language::Russian),
        "pl" | "polish" => Ok(Language::Polish),
        "de" | "german" => Ok(Language::German),
        "fr" | "french" => Ok(Language::French),
        "es" | "spanish" => Ok(Language::Spanish),
        other => lingua::Language::all()
            .into_iter()
            .find(|language| format!("{language:?}").to_lowercase() == other)
            .ok_or_else(|| anyhow!("Unknown language: '{}'", code)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_language_aliases_work() {
        assert_eq!(parse_language("uk").unwrap(), Language::Ukrainian);
        assert_eq!(parse_language("UKR").unwrap(), Language::Ukrainian);
        assert_eq!(parse_language("українська").unwrap(), Language::Ukrainian);

        assert_eq!(parse_language("en").unwrap(), Language::English);
        assert_eq!(parse_language("English").unwrap(), Language::English);

        assert_eq!(parse_language("ru").unwrap(), Language::Russian);
        assert_eq!(parse_language("русский").unwrap(), Language::Russian);

        let err = parse_language("xx").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("Unknown language"));
    }

    #[test]
    fn parse_languages_defaults_to_ukrainian() {
        let langs = parse_languages(&[]).unwrap();
        assert_eq!(langs.len(), 1);
        assert!(langs.contains(&Language::Ukrainian));
    }

    #[test]
    fn parse_languages_supports_multiple_values() {
        let langs =
            parse_languages(&["uk".to_string(), "en".to_string(), "ru".to_string()]).unwrap();
        assert_eq!(langs.len(), 3);
        assert!(langs.contains(&Language::Ukrainian));
        assert!(langs.contains(&Language::English));
        assert!(langs.contains(&Language::Russian));
    }
}
