use actix_web::error;
use failure::Fail;
use std::collections::HashMap;

#[derive(Debug, Serialize)]
enum MessageCategory {
    Error,
    // Info,
    // Warning,
}

#[derive(Debug, Serialize)]
struct DiagnosticMessage {
    category: MessageCategory,
    code: i16,
    details: &'static str,
    message: &'static str,
}

#[derive(Debug, Fail, Serialize)]
#[fail(display = "API_ERROR")]
pub struct ApiError {
    #[serde(flatten)]
    diagnostic_message: &'static DiagnosticMessage,
    // better name available?
    offenders_found: Option<Vec<String>>,
}

impl error::ResponseError for ApiError {}

// [#derive(Debug, Fail, Serialize)]
// [#serde(untagged)]
// pub enum QueryError {
//     [#fail(display = )]
//     InvalidSqlIdentifier {
//         code: 1000,
//         identifiers: Vec<&str>,
//         details: "Valid identifiers must only contain alphanumeric and underscore (_) characters. The first character must also be a letter or underscore.",
//         message: "There was an identifier (such as table or column name) that did not have valid characters.",
//     }
// }

pub fn generate_error(err_id: &str, offenders_found: Vec<String>) -> ApiError {
    // first generate errors once for entire runtime of app
    lazy_static! {
        // key = short error identifier
        static ref MESSAGES: HashMap<&'static str, DiagnosticMessage> = {
            let mut m = HashMap::new();

            m.insert("INVALID_SQL_IDENTIFIER", DiagnosticMessage {
                category: MessageCategory::Error,
                code: 1000,
                details: "Valid identifiers must only contain alphanumeric and underscore (_) characters. The first character must also be a letter or underscore.",
                message: "There was an identifier (such as table or column name) that did not have valid characters.",
            });

            m
        };
    }

    let diagnostic_message = MESSAGES.get(err_id).unwrap();

    ApiError {
        diagnostic_message,
        offenders_found: if offenders_found.is_empty() {
            Some(offenders_found)
        } else {
            None
        },
    }
}
