use actix_web::{http, HttpResponse};
use failure::Fail;
use postgres_rest::Error as RestError;
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

impl From<actix_web::Error> for Error {
    fn from(err: actix_web::Error) -> Self {
        Error::InternalError {
            code: "ACTIX_ERROR",
            details: format!("{}", err),
            message: "Error occurred with Actix.",
            http_status: 500,
        }
    }
}
impl From<actix_web::error::PayloadError> for Error {
    fn from(err: actix_web::error::PayloadError) -> Self {
        Error::InternalError {
            code: "PAYLOAD_ERROR",
            details: format!("{}", err),
            message: "Could not parse request payload.",
            http_status: 500,
        }
    }
}
impl From<RestError> for Error {
    fn from(err: RestError) -> Self {
        match err {
            RestError::InternalError {
                code,
                details,
                message,
                http_status,
            } => Error::InternalError {
                code,
                details,
                message,
                http_status,
            },
            RestError::UserError {
                code,
                details,
                message,
                offender,
                http_status,
            } => Error::UserError {
                code,
                details,
                message,
                offender,
                http_status,
            },
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

            "INVALID_CONTENT_TYPE" => Error::UserError {
                code: err_id,
                details: "The `Content-Type` header value is not valid for this request".to_string(),
                http_status: 400,
                message: "The `Content-Type` must be `text/plain`.",
                offender,
            },

            "REQUIRED_PARAMETER_MISSING" => Error::UserError {
                code: err_id,
                details: "".to_string(),
                http_status: 400,
                message: "There was a parameter required by this action, but it was not found.",
                offender,
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

/// Used for formatting the Errors that occur to display in an http response.
#[derive(Debug, Serialize)]
struct DisplayUserError<'a> {
    code: &'static str,
    details: String,
    message: &'static str,
    offender: Option<&'a str>,
}

// How Errors are formatted for an http response
impl actix_web::ResponseError for Error {
    fn render_response(&self) -> HttpResponse {
        match self {
            Error::UserError {
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

            Error::InternalError {
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
