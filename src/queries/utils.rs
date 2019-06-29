use crate::Error;
use lazy_static::lazy_static;
use regex::Regex;

/// Checks a table name and returns true if it is valid (false otherwise).
/// The identifier must start with a lower-case letter or underscore, and only contain
/// alphanumeric or underscore characters. (Sorry, I don’t have time or energy for UTF-8
/// shenanigans)
pub fn validate_table_name(name: &str) -> Result<(), Error> {
    // Using lazy_static so that VALID_REGEX is only compiled once total (versus compiling the regex
    // every time this function is called)
    lazy_static! {
        static ref VALID_REGEX: Regex = Regex::new(r"^[a-z_][a-z0-9_]*$").unwrap();
    }

    if name == "table" {
        return Err(Error::generate_error(
            "SQL_IDENTIFIER_KEYWORD",
            name.to_string(),
        ));
    }

    if !VALID_REGEX.is_match(name) {
        return Err(Error::generate_error(
            "INVALID_SQL_IDENTIFIER",
            name.to_string(),
        ));
    }

    Ok(())
}

/// Like `validate_table_name`, but applies to all other identifiers. Allows parentheses (for
/// functions/aggregates like `COUNT()`), periods (for foreign key traversal), and AS aliases.
pub fn validate_where_column(name: &str) -> Result<(), Error> {
    lazy_static! {
        // Rules:
        // - Starts with a letter (or underscore).
        // - Only contains letters, numbers, underscores, and parentheses.
        // - Must not end in a dot (.) or asterisk (*).
        static ref VALID_REGEX: Regex = Regex::new(r"^[A-Za-z_][A-Za-z0-9_\(\)\.\*]*[^\.\*]$").unwrap();
        // static ref AS_REGEX: Regex = Regex::new(r"(?i) AS ").unwrap();
    }

    if name == "table" {
        return Err(Error::generate_error(
            "SQL_IDENTIFIER_KEYWORD",
            name.to_string(),
        ));
    }

    // check for " AS ", then validate both the original, non-aliased identifier, as well as the
    // alias not supporting aliases currently. probably a good thing to implement though
    // if AS_REGEX.is_match(name) {
    //     let matched = AS_REGEX.find(name).unwrap();

    //     let orig = &name[..matched.start()];
    //     dbg!(orig);
    //     if !VALID_REGEX.is_match(orig) {
    //         return Err(Error::generate_error(
    //             "INVALID_SQL_IDENTIFIER",
    //             orig.to_string(),
    //         ));
    //     }

    //     let alias = &name[matched.end()..];
    //     dbg!(alias);
    //     if !VALID_REGEX.is_match(alias) {
    //         return Err(Error::generate_error(
    //             "INVALID_SQL_IDENTIFIER",
    //             alias.to_string(),
    //         ));
    //     }

    //     return Ok(());
    // }

    dbg!(name);

    if !VALID_REGEX.is_match(name) {
        return Err(Error::generate_error(
            "INVALID_SQL_IDENTIFIER",
            name.to_string(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod validate_table_name_tests {
    use super::*;

    #[test]
    fn simple_string() {
        assert!(validate_table_name("test").is_ok());
    }

    #[test]
    fn with_underscore() {
        assert!(validate_table_name("a_table").is_ok());
    }

    #[test]
    fn begins_with_underscore() {
        assert!(validate_table_name("_a_table").is_ok());
        assert!(validate_table_name("_0_table").is_ok());
    }

    #[test]
    fn uppercase() {
        assert!(validate_table_name("TEST").is_err());
    }

    #[test]
    fn reserved_keywords() {
        assert!(validate_table_name("table").is_err());
    }

    #[test]
    fn invalid_characters() {
        assert!(validate_table_name("ü_table").is_err());
        assert!(validate_table_name("table_ü").is_err());
    }

    #[test]
    fn empty_string() {
        assert!(validate_table_name("").is_err());
    }

    #[test]
    fn white_space() {
        assert!(validate_table_name(" ").is_err());
        assert!(validate_table_name("\n").is_err());
        assert!(validate_table_name("\t").is_err());
    }
}

#[cfg(test)]
mod validate_where_column_tests {
    use super::*;

    #[test]
    fn count() {
        assert!(validate_where_column("COUNT(*)").is_ok());
        assert!(validate_where_column("COUNT(id)").is_ok());
    }

    #[test]
    fn foreign_keys() {
        assert!(validate_where_column("user_id.company_id.name").is_ok());
        assert!(validate_where_column("user_id.company_id.").is_err());
    }

    #[test]
    fn as_alias() {
        // may change later
        assert!(validate_where_column("arya AS arry").is_err());
        assert!(validate_where_column(" AS arry").is_err());
        assert!(validate_where_column("arya AS arry AS cat").is_err());
        assert!(validate_where_column("arya AS  AS arry").is_err());
    }
}
