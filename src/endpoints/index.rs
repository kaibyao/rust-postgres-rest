use actix_web::{HttpRequest, HttpResponse};
use serde_json::{json, Value};

use crate::AppState;

pub fn index(_req: &HttpRequest<AppState>) -> HttpResponse {
    lazy_static! {
        static ref endpoints_json: Value = json!({
            "endpoints": {
            "/": {
                "GET": "The current endpoint. Displays REST API endpoints and available tables."
            },
            "/table": {
                "GET": "Displays list of tables.",
                "POST": "Create table (not implemented)",
                "PUT|PATCH": "Update table (not implemented)",
                "DELETE": "Delete table (not implemented)",
            },
            "/{table}": {
                "GET": {
                    "description": "Queries {table} with given parameters using SELECT. If no columns are provided, returns stats for {table}.",
                    "query_params": {
                        "columns": {
                            "default": null,
                            "description": "A list of column names for which values are retrieved.",
                            "example": "col1,col2,col_infinity",
                        },
                        "distinct": {
                            "default": null,
                            "description": "A list of column names for which rows that have duplicate values are excluded.",
                            "example": "col1,col2,col_infinity",
                        },
                        "limit": {
                            "default": 10000,
                            "description": "The maximum number of rows that can be returned.",
                        },
                        "offset": {
                            "default": 0,
                            "description": "The number of rows to exclude.",
                        },
                        "order_by": {
                            "default": null,
                            "description": "The field(s) on which to sort the resulting rows.",
                            "example": "date DESC, id ASC",
                        },
                        "where": {
                            "default": null,
                            "description": "The WHERE clause of a SELECT statement. Remember to URI-encode the final result.",
                            "example": "(field_1 >= field_2 AND id IN (1,2,3)) OR field_2 > field_1",
                        }
                    }
                }
            }}
        });
    }

    HttpResponse::Ok().json(&*endpoints_json)
}
