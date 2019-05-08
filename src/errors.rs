use actix_web::{error, http, HttpResponse};
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

            "INVALID_SQL_IDENTIFIER" => ApiError::UserError {
                category: MessageCategory::Error,
                code: err_id,
                details: "Valid identifiers must only contain alphanumeric and underscore (_) characters. The first character must also be a letter or underscore.".to_string(),
                http_status: 400,
                message: "There was an identifier (such as table or column name) that did not have valid characters.",
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

            "REQUIRED_PARAMETER_MISSING" => ApiError::UserError {
                category: MessageCategory::Error,
                code: err_id,
                details: "".to_string(),
                http_status: 400,
                message: "There was a parameter required by this action, but it was not found.",
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

impl From<r2d2::Error> for ApiError {
    fn from(err: r2d2::Error) -> Self {
        ApiError::InternalError {
            category: MessageCategory::Error,
            code: "DATABASE_ERROR_R2D2",
            details: format!("{}", err),
            message: "A database error occurred (r2d2).",
            http_status: 500,
        }
    }
}
impl From<actix_web::actix::MailboxError> for ApiError {
    fn from(err: actix_web::actix::MailboxError) -> Self {
        ApiError::InternalError {
            category: MessageCategory::Error,
            code: "SEND_MESSAGE_ERROR",
            details: format!("{}", err),
            message: "A message failed to send/receive to/from Actix actor.",
            http_status: 500,
        }
    }
}
impl From<error::PayloadError> for ApiError {
    fn from(err: error::PayloadError) -> Self {
        ApiError::InternalError {
            category: MessageCategory::Error,
            code: "PAYLOAD_ERROR",
            details: format!("{}", err),
            message: "Could not parse request payload.",
            http_status: 500,
        }
    }
}
impl From<error::Error> for ApiError {
    fn from(err: error::Error) -> Self {
        ApiError::InternalError {
            category: MessageCategory::Error,
            code: "ACTIX_ERROR",
            details: format!("{}", err),
            message: "Error occurred with Actix.",
            http_status: 500,
        }
    }
}
impl From<postgres::Error> for ApiError {
    fn from(err: postgres::Error) -> Self {
        ApiError::InternalError {
            category: MessageCategory::Error,
            code: "DATABASE_ERROR_POSTGRES",
            details: format!("{}", err),
            message: "A database error occurred (postgres).",
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

// How ApiErrors are formatted for an http response
impl error::ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse {
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
