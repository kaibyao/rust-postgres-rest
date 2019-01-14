// use failure::Error;
use regex::Regex;
use std::io::{Error as StdError, ErrorKind};

/// Checks an SQL identifier (such as table or column name) and returns true if it is valid or false otherwise.
/// The identifier must start with a letter or underscore, and only contain alphanumeric or underscore characters.
/// (Sorry, I don’t have time or energy for UTF-8 shenanigans)
pub fn validate_sql_name(name: &str) -> Result<(), StdError> {
    // Using lazy_static so that VALID_REGEX is only compiled once total (versus compiling the regex every time this function is called)
    lazy_static! {
        static ref VALID_REGEX: Regex = Regex::new(r"^[a-zA-Z_]\w*$").unwrap();
    }

    if !VALID_REGEX.is_match(name) {
        return Err(
            StdError::new(
                ErrorKind::InvalidInput,
                format!("`{}` is not a valid name. Valid names must only contain alphanumeric and underscore (_) characters. The first character must also be a letter or underscore.", name)
            )
        );
    }
    Ok(())
}
