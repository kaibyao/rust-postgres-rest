use super::postgres_types::RowFields;
use crate::Error;
use lazy_static::lazy_static;
use regex::Regex;

#[derive(Debug, PartialEq)]
/// Possible values that can be passed into a prepared statement Vec.
pub enum PreparedStatementValue {
    String(String),
    Int8(i64),
    Int4(i32),
}

/// Uused for returning either number of rows or actual row values in INSERT/UPDATE statements.
pub enum UpsertResult {
    Rows(Vec<RowFields>),
    NumRowsAffected(u64),
}

/// Takes a string of columns and returns the RETURNING clause of an INSERT or UPDATE statement.
pub fn generate_returning_clause(returning_columns_opt: &Option<Vec<String>>) -> Option<String> {
    if let Some(returning_columns) = returning_columns_opt {
        return Some([" RETURNING ", &returning_columns.join(", ")].join(""));
    }

    None
}

/// Given a string of column names separated by commas, convert and return a vector of lowercase
/// strings.
pub fn normalize_columns(columns_str: &str) -> Result<Vec<String>, Error> {
    columns_str
        .split(',')
        .map(|s| {
            if s == "" {
                return Err(Error::generate_error(
                    "INCORRECT_REQUEST_BODY",
                    ["`", s, "`", " is not a valid column name. Column names must be a comma-separated list and include at least one column name."].join(""),
                ));
            }
            Ok(s.trim().to_lowercase())
        })
        .collect()
}

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

/// Check for " AS ", then validate both the original, non-aliased identifier, as well as the alias
pub fn validate_alias_identifier(identifier: &str) -> Result<Option<(&str, &str)>, Error> {
    lazy_static! {
        // Searching for " AS " alias
        static ref AS_REGEX: Regex = Regex::new(r"(?i) AS ").unwrap();
    }

    if !AS_REGEX.is_match(identifier) {
        validate_where_column(identifier)?;
        return Ok(None);
    }

    let matched = AS_REGEX.find(identifier).unwrap();
    let orig = &identifier[..matched.start()];
    let alias = &identifier[matched.end()..];

    validate_where_column(orig)?;
    validate_where_column(alias)?;

    Ok(Some((orig, alias)))
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
}

#[cfg(test)]
mod validate_alias_identifier_tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn as_alias() {
        // may change later
        assert_eq!(
            validate_alias_identifier("arya AS arry").unwrap(),
            Some(("arya", "arry"))
        );
        assert!(validate_alias_identifier(" AS arry").is_err());
        assert!(validate_alias_identifier("arya AS arry AS cat").is_err());
        assert!(validate_alias_identifier("arya AS  AS arry").is_err());
    }
}
