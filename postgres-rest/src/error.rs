use failure::Fail;
use serde::Serialize;

#[derive(Debug, Fail, Serialize)]
#[serde(untagged)]
/// A wrapper around all the errors we can run into.
pub enum Error {
    /// Describes errors that are generated due to user misuse.
    #[fail(
        display = "{}: {} Offender: {}.\n\nDetails:\n{}",
        code, message, offender, details
    )]
    UserError {
        code: &'static str,
        details: String,
        message: &'static str,
        offender: String,
        http_status: u16,
    },

    /// Describes errors that are generated due to system errors.
    #[fail(display = "An internal error has occurred: {}. {}", message, details)]
    InternalError {
        code: &'static str,
        details: String,
        message: &'static str,
        http_status: u16,
    },
}

impl From<actix::MailboxError> for Error {
    fn from(err: actix::MailboxError) -> Self {
        Error::InternalError {
            code: "SEND_MESSAGE_ERROR",
            details: format!("{}", err),
            message: "A message failed to send/receive to/from Actix actor.",
            http_status: 500,
        }
    }
}
impl From<chrono::format::ParseError> for Error {
    fn from(err: chrono::format::ParseError) -> Self {
        Error::UserError {
            code: "JSON_ERROR",
            details: format!("{}", err),
            message: "An error occurred when parsing JSON.",
            offender: "".to_string(),
            http_status: 400,
        }
    }
}
impl From<eui48::ParseError> for Error {
    fn from(err: eui48::ParseError) -> Self {
        Error::UserError {
            code: "MAC_ADDR_ERROR",
            details: format!("{}", err),
            message: "An error occurred when parsing a mac address.",
            offender: "".to_string(),
            http_status: 400,
        }
    }
}
impl From<rust_decimal::Error> for Error {
    fn from(err: rust_decimal::Error) -> Self {
        Error::UserError {
            code: "DECIMAL_ERROR",
            details: format!("{}", err),
            message: "An error occurred when parsing a decimal string.",
            offender: "".to_string(),
            http_status: 400,
        }
    }
}
impl From<serde_json::error::Error> for Error {
    fn from(err: serde_json::error::Error) -> Self {
        Error::UserError {
            code: "JSON_ERROR",
            details: format!("{}", err),
            message: "A message occurred when parsing JSON.",
            offender: "".to_string(),
            http_status: 400,
        }
    }
}
impl From<sqlparser::parser::ParserError> for Error {
    fn from(err: sqlparser::parser::ParserError) -> Self {
        let details = match err {
            sqlparser::parser::ParserError::ParserError(err_str) => err_str,
            sqlparser::parser::ParserError::TokenizerError(err_str) => err_str,
        };

        Error::UserError {
            code: "SQL_PARSER_ERROR",
            details,
            message: "A message occurred when parsing SQL.",
            offender: "".to_string(),
            http_status: 400,
        }
    }
}
impl<T> From<std::sync::PoisonError<T>> for Error {
    fn from(err: std::sync::PoisonError<T>) -> Self {
        Error::InternalError {
            code: "MEM_LOCK_ERROR",
            details: format!("{}", err),
            message: "A memory-locked process has failed.",
            http_status: 500,
        }
    }
}
impl From<tokio_postgres::Error> for Error {
    fn from(err: tokio_postgres::Error) -> Self {
        Error::InternalError {
            code: "DATABASE_ERROR",
            details: format!("{}", err),
            message: "A database error occurred (postgres).",
            http_status: 500,
        }
    }
}
impl From<uuid::parser::ParseError> for Error {
    fn from(err: uuid::parser::ParseError) -> Self {
        Error::UserError {
            code: "UUID_ERROR",
            details: format!("{}", err),
            message: "An error occurred when parsing a UUID string.",
            offender: "".to_string(),
            http_status: 500,
        }
    }
}

impl futures::future::Future for Error {
    type Item = ();
    type Error = Self;

    fn poll(&mut self) -> futures::Poll<(), Self::Error> {
        Ok(futures::Async::Ready(()))
    }
}

impl Error {
    /// Used to generate an Error
    pub fn generate_error(err_id: &'static str, offender: String) -> Self {
        match err_id {
            "INCORRECT_REQUEST_BODY" => Error::UserError {
                code: err_id,
                details: "".to_string(),
                http_status: 400,
                message: "The request body does not match the expected shape. Please check the documentation for the correct format.",
                offender,
            },

            "INVALID_JSON_TYPE_CONVERSION" => Error::UserError {
                code: err_id,
                details: "The type of the JSON data does not match the type of the database column.".to_string(),
                http_status: 400,
                message: "Failed conversion of data from JSON to database column.",
                offender
            },

            "INVALID_SQL_IDENTIFIER" => Error::UserError {
                code: err_id,
                details: "Valid identifiers must only contain alphanumeric and underscore (_) characters. The first character must also be a letter or underscore. Wildcards (*) are not allowed.".to_string(),
                http_status: 400,
                message: "There was an identifier (such as table or column name) that did not have valid characters.",
                offender,
            },

            "INVALID_SQL_SYNTAX" => Error::UserError {
                code: err_id,
                details: "The SQL expression could not be parsed by PostgreSQL.".to_string(),
                http_status: 400,
                message: "Check that the SQL syntax is correct.",
                offender,
            },

            "SQL_IDENTIFIER_KEYWORD" => Error::UserError {
                code: err_id,
                details: "`table` is a reserved keyword and cannot be used to name SQL identifiers".to_string(),
                http_status: 400,
                message: "There was an identifier (such as table or column name) that used a reserved keyword.",
                offender,
            },

            "TABLE_COLUMN_TYPE_NOT_FOUND" => Error::InternalError {
                code: err_id,
                details: format!("The column type for column `{}` could not be generated from the Table Stats query. Please submit a bug report, as this really shouldn’t be happening.", offender),
                http_status: 500,
                message: "The column type for a queried table column could not be determined.",
            },

            "TABLE_STATS_CACHE_NOT_ENABLED" => Error::UserError {
                code: err_id,
                details: "The Table Stats Cache has not been enabled, please turn it on by calling the config’s `cache_table_stats()`.".to_string(),
                http_status: 400,
                message: "",
                offender,
            },

            "TABLE_STATS_CACHE_NOT_INITIALIZED" => Error::UserError {
                code: err_id,
                details: "The Table Stats Cache has not yet started/finished fetching table stats.".to_string(),
                http_status: 503,
                message: "",
                offender,
            },

            "UNSUPPORTED_DATA_TYPE" => Error::UserError {
                code: err_id,
                details: "".to_string(),
                http_status: 400,
                message: "The type of the database column is not supported by the REST API.",
                offender
            },

            // If this happens, that means we forgot to implement an error handler
            _ => Error::UserError {
                code: err_id,
                details: "Generic error.".to_string(),
                http_status: 418,
                message: "An error occurred that we did not anticipate. Please let admins know.",
                offender,
            }
        }
    }
}
