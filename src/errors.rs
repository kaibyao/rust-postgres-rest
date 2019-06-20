use actix_web::{http, HttpResponse};
use failure::Fail;

#[derive(Debug, Serialize)]
/// Describes the type of notification we are sending
pub enum MessageCategory {
    Error,
    // Info,
    // Warning,
}

#[derive(Debug, Fail, Serialize)]
#[serde(untagged)]
/// A wrapper around all the errors we can run into.
pub enum ApiError {
    /// Describes errors that are generated due to user misuse.
    #[fail(
        display = "{}: {} Offender: {}.\n\nDetails:\n{}",
        code, message, offender, details
    )]
    UserError {
        category: MessageCategory,
        code: &'static str,
        details: String,
        message: &'static str,
        offender: String,
        http_status: u16,
    },

    /// Describes errors that are generated due to system errors.
    #[fail(display = "An internal error has occurred.")]
    InternalError {
        category: MessageCategory,
        code: &'static str,
        details: String,
        message: &'static str,
        http_status: u16,
    },
}

impl ApiError {
    /// Used to generate an ApiError
    pub fn generate_error(err_id: &'static str, offender: String) -> Self {
        match err_id {
            "INCORRECT_REQUEST_BODY" => ApiError::UserError {
                category: MessageCategory::Error,
                code: err_id,
                details: "".to_string(),
                http_status: 400,
                message: "The request body does not match the expected shape. Please check the documentation for the correct format.",
                offender,
            },

            "INVALID_JSON_TYPE_CONVERSION" => ApiError::UserError {
                category: MessageCategory::Error,
                code: err_id,
                details: "The type of the JSON data does not match the type of the database column.".to_string(),
                http_status: 400,
                message: "Failed conversion of data from JSON to database column.",
                offender
            },

            "INVALID_SQL_IDENTIFIER" => ApiError::UserError {
                category: MessageCategory::Error,
                code: err_id,
                details: "Valid identifiers must only contain alphanumeric and underscore (_) characters. The first character must also be a letter or underscore.".to_string(),
                http_status: 400,
                message: "There was an identifier (such as table or column name) that did not have valid characters.",
                offender,
            },

            "INVALID_SQL_SYNTAX" => ApiError::UserError {
                category: MessageCategory::Error,
                code: err_id,
                details: "The SQL expression could not be parsed by PostgreSQL.".to_string(),
                http_status: 400,
                message: "Check that the SQL syntax is correct.",
                offender,
            },

            "NO_DATABASE_CONNECTION" => ApiError::UserError {
                category: MessageCategory::Error,
                code: err_id,
                details: "A database client does not exist.".to_string(),
                http_status: 500,
                message: "Something went wrong during server startup. Message the admin.",
                offender,
            },

            "REQUIRED_PARAMETER_MISSING" => ApiError::UserError {
                category: MessageCategory::Error,
                code: err_id,
                details: "".to_string(),
                http_status: 400,
                message: "There was a parameter required by this action, but it was not found.",
                offender,
            },

            "SQL_IDENTIFIER_KEYWORD" => ApiError::UserError {
                category: MessageCategory::Error,
                code: err_id,
                details: "`table` is a reserved keyword and cannot be used to name SQL identifiers".to_string(),
                http_status: 400,
                message: "There was an identifier (such as table or column name) that used a reserved keyword.",
                offender,
            },

            "UNSUPPORTED_DATA_TYPE" => ApiError::UserError {
                category: MessageCategory::Error,
                code: err_id,
                details: "".to_string(),
                http_status: 400,
                message: "The type of the database column is not supported by the REST API.",
                offender
            },

            // If this happens, that means we forgot to implement an error handler
            _ => ApiError::UserError {
                category: MessageCategory::Error,
                code: err_id,
                details: "Generic error.".to_string(),
                http_status: 418,
                message: "An error occurred that we did not anticipate. Please let admins know.",
                offender,
            }
        }
    }
}

impl From<actix::MailboxError> for ApiError {
    fn from(err: actix::MailboxError) -> Self {
        ApiError::InternalError {
            category: MessageCategory::Error,
            code: "SEND_MESSAGE_ERROR",
            details: format!("{}", err),
            message: "A message failed to send/receive to/from Actix actor.",
            http_status: 500,
        }
    }
}
impl From<actix_http::error::Error> for ApiError {
    fn from(err: actix_http::error::Error) -> Self {
        ApiError::InternalError {
            category: MessageCategory::Error,
            code: "ACTIX_ERROR",
            details: format!("{}", err),
            message: "Error occurred with Actix.",
            http_status: 500,
        }
    }
}
impl From<bb8::RunError<ApiError>> for ApiError {
    fn from(err: bb8::RunError<ApiError>) -> Self {
        match err {
            bb8::RunError::TimedOut => {
                let details = "The database connection timed out.".to_string();
                ApiError::InternalError {
                    category: MessageCategory::Error,
                    code: "DATABASE_ERROR_TIMEOUT",
                    details,
                    message: "There was an error when making the request with the database pool.",
                    http_status: 500,
                }
            }
            bb8::RunError::User(e) => e,
        }
    }
}
impl From<bb8::RunError<tokio_postgres::Error>> for ApiError {
    fn from(err: bb8::RunError<tokio_postgres::Error>) -> Self {
        let details = match err {
            bb8::RunError::TimedOut => "The database connection timed out.".to_string(),
            bb8::RunError::User(e) => format!("{}", e),
        };

        ApiError::InternalError {
            category: MessageCategory::Error,
            code: "DATABASE_ERROR",
            details,
            message: "There was an error when making the request with the database pool.",
            http_status: 500,
        }
    }
}
impl From<actix_web::error::PayloadError> for ApiError {
    fn from(err: actix_web::error::PayloadError) -> Self {
        ApiError::InternalError {
            category: MessageCategory::Error,
            code: "PAYLOAD_ERROR",
            details: format!("{}", err),
            message: "Could not parse request payload.",
            http_status: 500,
        }
    }
}
impl From<serde_json::error::Error> for ApiError {
    fn from(err: serde_json::error::Error) -> Self {
        ApiError::UserError {
            category: MessageCategory::Error,
            code: "JSON_ERROR",
            details: format!("{}", err),
            message: "A message occurred when parsing JSON.",
            offender: "".to_string(),
            http_status: 400,
        }
    }
}
impl From<sqlparser::sqlparser::ParserError> for ApiError {
    fn from(err: sqlparser::sqlparser::ParserError) -> Self {
        let details = match err {
            sqlparser::sqlparser::ParserError::ParserError(err_str) => err_str,
            sqlparser::sqlparser::ParserError::TokenizerError(err_str) => err_str,
        };

        ApiError::UserError {
            category: MessageCategory::Error,
            code: "SQL_PARSER_ERROR",
            details,
            message: "A message occurred when parsing JSON.",
            offender: "".to_string(),
            http_status: 400,
        }
    }
}
impl From<tokio_postgres::Error> for ApiError {
    fn from(err: tokio_postgres::Error) -> Self {
        ApiError::InternalError {
            category: MessageCategory::Error,
            code: "DATABASE_ERROR",
            details: format!("{}", err),
            message: "A database error occurred (postgres).",
            http_status: 500,
        }
    }
}

impl futures::future::Future for ApiError {
    type Item = ();
    type Error = Self;

    fn poll(&mut self) -> futures::Poll<(), Self::Error> {
        Ok(futures::Async::Ready(()))
    }
}

#[derive(Debug, Serialize)]
struct DisplayUserError<'a> {
    code: &'static str,
    details: String,
    message: &'static str,
    offender: Option<&'a str>,
}

// How ApiErrors are formatted for an http response
impl actix_web::ResponseError for ApiError {
    fn render_response(&self) -> HttpResponse {
        // Used for formatting the ApiErrors that occur to display in an http response.
        #[derive(Debug, Serialize)]
        struct DisplayUserError<'a> {
            code: &'static str,
            details: String,
            message: &'static str,
            offender: Option<&'a str>,
        }

        match self {
            ApiError::UserError {
                code,
                details,
                http_status,
                message,
                offender,
                ..
            } => HttpResponse::build(http::StatusCode::from_u16(*http_status).unwrap()).json(
                DisplayUserError {
                    code,
                    details: details.to_string(),
                    message,
                    offender: Some(offender),
                },
            ),

            ApiError::InternalError {
                code,
                details,
                http_status,
                message,
                ..
            } => HttpResponse::build(http::StatusCode::from_u16(*http_status).unwrap()).json(
                DisplayUserError {
                    code,
                    details: details.to_string(),
                    message,
                    offender: None,
                },
            ),
        }
    }
}
