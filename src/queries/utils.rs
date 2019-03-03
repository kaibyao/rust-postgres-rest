use crate::errors::{generate_error, ApiError};
use regex::Regex;

/// Checks an SQL identifier (such as table or column name) and returns true if it is valid or false otherwise.
/// The identifier must start with a lower-case letter or underscore, and only contain alphanumeric or underscore characters.
/// (Sorry, I don’t have time or energy for UTF-8 shenanigans)
pub fn validate_sql_name(name: &str) -> Result<(), ApiError> {
    // Using lazy_static so that VALID_REGEX is only compiled once total (versus compiling the regex every time this function is called)
    lazy_static! {
        static ref VALID_REGEX: Regex = Regex::new(r"^[a-z_][a-z0-9_]*$").unwrap();
    }

    if name == "table" {
        return Err(generate_error("SQL_IDENTIFIER_KEYWORD", name.to_string()));
    }

    if !VALID_REGEX.is_match(name) {
        return Err(generate_error("INVALID_SQL_IDENTIFIER", name.to_string()));
    }

    Ok(())
}

/// Like `validate_sql_name`, but also allows for parentheses (for functions/aggregates like `COUNT()`) as well as " AS " aliases.
pub fn validate_where_column(name: &str) -> Result<(), ApiError> {
    lazy_static! {
        static ref VALID_REGEX: Regex = Regex::new(r"^[A-Za-z_][A-Za-z0-9_\(\)]*$").unwrap();
        static ref VALID_AS_REGEX: Regex = Regex::new(r"(?i) AS ").unwrap();
    }

    if name == "table" {
        return Err(generate_error("SQL_IDENTIFIER_KEYWORD", name.to_string()));
    }

    if !VALID_REGEX.is_match(name) && !VALID_AS_REGEX.is_match(name) {
        return Err(generate_error("INVALID_SQL_IDENTIFIER", name.to_string()));
    }

    Ok(())
}

#[cfg(test)]
mod validate_sql_name_tests {
    use super::*;

    #[test]
    fn simple_string() {
        assert!(validate_sql_name("test").is_ok());
    }

    #[test]
    fn with_underscore() {
        assert!(validate_sql_name("a_table").is_ok());
    }

    #[test]
    fn begins_with_underscore() {
        assert!(validate_sql_name("_a_table").is_ok());
        assert!(validate_sql_name("_0_table").is_ok());
    }

    #[test]
    fn uppercase() {
        assert!(validate_sql_name("TEST").is_err());
    }

    #[test]
    fn reserved_keywords() {
        assert!(validate_sql_name("table").is_err());
    }

    #[test]
    fn invalid_characters() {
        assert!(validate_sql_name("ü_table").is_err());
        assert!(validate_sql_name("table_ü").is_err());
    }

    #[test]
    fn empty_string() {
        assert!(validate_sql_name("").is_err());
    }

    #[test]
    fn white_space() {
        assert!(validate_sql_name(" ").is_err());
        assert!(validate_sql_name("\n").is_err());
        assert!(validate_sql_name("\t").is_err());
    }
}
