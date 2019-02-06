use actix_web::{error, http, HttpResponse};
use failure::Fail;
// use std::collections::HashMap;

#[derive(Debug, Serialize)]
pub enum MessageCategory {
    Error,
    // Info,
    // Warning,
}

#[derive(Debug, Fail, Serialize)]
#[serde(untagged)]
pub enum ApiError {
    // covers ALL errors that can be reported to the user
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

    #[fail(display = "An internal error has occurred.")]
    InternalError {
        category: MessageCategory,
        code: &'static str,
        details: String,
        message: &'static str,
        http_status: u16,
    },
}

#[derive(Debug, Serialize)]
pub struct DisplayUserError<'a> {
    code: &'static str,
    details: String,
    message: &'static str,
    offender: Option<&'a str>,
}

impl error::ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse {
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

impl From<r2d2::Error> for ApiError {
    fn from(err: r2d2::Error) -> Self {
        ApiError::InternalError {
            category: MessageCategory::Error,
            code: "DATABASE_ERROR",
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
impl From<postgres::Error> for ApiError {
    fn from(err: postgres::Error) -> Self {
        ApiError::InternalError {
            category: MessageCategory::Error,
            code: "DATABASE_ERROR",
            details: format!("{}", err),
            message: "A database error occurred (postgres).",
            http_status: 500,
        }
    }
}

pub fn generate_error(err_id: &'static str, offender: String) -> ApiError {
    match err_id {
        "INVALID_SQL_IDENTIFIER" => ApiError::UserError {
            category: MessageCategory::Error,
            code: err_id,
            details: "Valid identifiers must only contain alphanumeric and underscore (_) characters. The first character must also be a letter or underscore.".to_string(),
            http_status: 400,
            message: "There was an identifier (such as table or column name) that did not have valid characters.",
            offender,
        },
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
