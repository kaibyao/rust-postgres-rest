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

// /// Takes a `where` query string and converts it to the contents of a "WHERE" SQL clause.
// pub fn where_query_to_clause(where_query: &str) -> String {
//     String::from("")

//     // we might have to pass in the table + column types information in order to differentiate column names from strings
// }

// fn sanitize_where_clause() {}

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

// #[cfg(test)]
// mod where_query_to_clause_tests {
//     use super::*;
//     use pretty_assertions::assert_eq;

//     #[test]
//     fn empty_string() {
//         assert_eq!(where_query_to_clause(""), "");
//     }

//     #[test]
//     fn simple_equal() {
//         assert_eq!(where_query_to_clause("x=t"), "x = t");
//         assert_eq!(where_query_to_clause("x='t'"), "x = 't'");
//         assert_eq!(where_query_to_clause("x=true"), "x = true");
//         assert_eq!(where_query_to_clause("x='true'"), "x = 'true'");
//         assert_eq!(where_query_to_clause("x=1"), "x = 1");
//         assert_eq!(where_query_to_clause("x=1.05"), "x = 1.05");
//     }

//     #[test]
//     fn simple_not_equal() {
//         assert_eq!(where_query_to_clause("x!=t"), "x != t");
//         assert_eq!(where_query_to_clause("x!='t'"), "x != 't'");
//         assert_eq!(where_query_to_clause("x!=true"), "x != true");
//         assert_eq!(where_query_to_clause("x!='true'"), "x != 'true'");
//         assert_eq!(where_query_to_clause("x!=1"), "x != 1");
//         assert_eq!(where_query_to_clause("x!=1.05"), "x != 1.05");
//     }

//     #[test]
//     fn simple_greater_than() {
//         assert_eq!(where_query_to_clause("x>t"), "x > t");
//         assert_eq!(where_query_to_clause("x>'t'"), "x > 't'");
//         assert_eq!(where_query_to_clause("x>'true'"), "x > 'true'");
//         assert_eq!(where_query_to_clause("x>1"), "x > 1");
//         assert_eq!(where_query_to_clause("x>1.05"), "x > 1.05");
//     }

//     #[test]
//     fn simple_less_than() {
//         assert_eq!(where_query_to_clause("x<t"), "x < t");
//         assert_eq!(where_query_to_clause("x<'t'"), "x < 't'");
//         assert_eq!(where_query_to_clause("x<'true'"), "x < 'true'");
//         assert_eq!(where_query_to_clause("x<1"), "x < 1");
//         assert_eq!(where_query_to_clause("x<1.05"), "x < 1.05");
//     }

//     #[test]
//     fn simple_greater_than_equal() {
//         assert_eq!(where_query_to_clause("x>=t"), "x >= t");
//         assert_eq!(where_query_to_clause("x>='t'"), "x >= 't'");
//         assert_eq!(where_query_to_clause("x>=true"), "x >= true");
//         assert_eq!(where_query_to_clause("x>='true'"), "x >= 'true'");
//         assert_eq!(where_query_to_clause("x>=1"), "x >= 1");
//         assert_eq!(where_query_to_clause("x>=1.05"), "x >= 1.05");
//     }

//     #[test]
//     fn simple_less_than_equal() {
//         assert_eq!(where_query_to_clause("x<=t"), "x <= t");
//         assert_eq!(where_query_to_clause("x<='t'"), "x <= 't'");
//         assert_eq!(where_query_to_clause("x<=true"), "x <= true");
//         assert_eq!(where_query_to_clause("x<='true'"), "x <= 'true'");
//         assert_eq!(where_query_to_clause("x<=1"), "x <= 1");
//         assert_eq!(where_query_to_clause("x<=1.05"), "x <= 1.05");
//     }

//     #[test]
//     fn simple_and() {
//         assert_eq!(where_query_to_clause("xANDy"), "x AND y");
//         assert_eq!(where_query_to_clause("xANDtrue"), "x AND true");
//         assert_eq!(where_query_to_clause("xANDTRUE"), "x AND TRUE");
//         assert_eq!(where_query_to_clause("TRUEAND'FALSE'"), "TRUE AND 'FALSE'");
//         assert_eq!(where_query_to_clause("'1'AND2"), "'1' AND 2");
//     }

//     #[test]
//     fn simple_or() {
//         assert_eq!(where_query_to_clause("xORy"), "x OR y");
//         assert_eq!(where_query_to_clause("xORtrue"), "x OR true");
//         assert_eq!(where_query_to_clause("xORTRUE"), "x OR TRUE");
//         assert_eq!(where_query_to_clause("TRUEOR'FALSE'"), "TRUE OR 'FALSE'");
//         assert_eq!(where_query_to_clause("'1'OR2"), "'1' OR 2");
//     }

//     #[test]
//     fn simple_in() {
//         assert_eq!(where_query_to_clause("xIN1,2,3"), "x IN (1, 2, 3)");
//         assert_eq!(where_query_to_clause("xIN(1,2,3)"), "x IN (1, 2, 3)");
//     }

//     #[test]
//     fn simple_is_null() {
//         assert_eq!(where_query_to_clause("xISNULL"), "x IS NULL");
//     }

//     #[test]
//     fn simple_is_true() {
//         assert_eq!(where_query_to_clause("xISTRUE"), "x IS TRUE");
//     }

//     #[test]
//     fn simple_is_false() {
//         assert_eq!(where_query_to_clause("xISFALSE"), "x IS FALSE");
//     }

//     #[test]
//     fn between_and() {
//         assert_eq!(where_query_to_clause("xISTRUE"), "x IS TRUE");
//     }

//     #[test]
//     fn simple_overlaps() {}

//     #[test]
//     fn is_not_true() {}

//     #[test]
//     fn is_not_false() {}

//     #[test]
//     fn is_not_null() {}

//     #[test]
//     fn not_between() {}

//     #[test]
//     fn not_overlaps() {}

//     #[test]
//     fn parentheses() {}

//     #[test]
//     fn string_and() {}

//     #[test]
//     fn string_or() {}

//     #[test]
//     fn string_like() {}

//     #[test]
//     fn string_ilike() {}

//     #[test]
//     fn string_in() {}

//     #[test]
//     fn string_is() {}

//     #[test]
//     fn string_between() {}

//     #[test]
//     fn string_overlaps() {}

//     #[test]
//     fn string_not() {}

//     #[test]
//     fn semicolon_error() {}

//     #[test]
//     fn semicolon_string() {}
// }
