use actix_web::{HttpRequest, HttpResponse};
use serde_json::{json, Value};

use crate::AppState;

pub fn index(_req: &HttpRequest<AppState>) -> HttpResponse {
    lazy_static! {
        static ref ENDPOINTS_JSON: Value = json!({
            "endpoints": {
            "/": {
                "GET": "The current endpoint. Displays REST API endpoints and available tables.",
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
                            "description": "A comma-separated list of column names for which values are retrieved.",
                            "example": "col1,col2,col_infinity",
                        },
                        "distinct": {
                            "default": null,
                            "description": "A comma-separated list of column names for which rows that have duplicate values are excluded.",
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
                            "description": "Comma-separated list representing the field(s) on which to sort the resulting rows.",
                            "example": "date DESC, id ASC",
                        },
                        "where": {
                            "default": null,
                            "description": "The WHERE clause of a SELECT statement. Remember to URI-encode the final result. NOTE: $1, $2, etc. can be used in combination with `prepared_values` to create prepared statements (see https://www.postgresql.org/docs/current/sql-prepare.html).",
                            "example": "(field_1 >= field_2 AND id IN (1,2,3)) OR field_2 > field_1",
                        },
                        "prepared_values": {
                            "default": null,
                            "description": "If the WHERE clause contains ${number}, this comma-separated list of values is used to substitute the numbered parameters.",
                            "example": "col2,'Test'",
                        },
                    }
                },
            }},
            "/sql": {
                "GET|POST|PUT|PATCH|DELETE": "Runs a raw SQL statement. (not implemented)",
            },
        });
    }

    HttpResponse::Ok().json(&*ENDPOINTS_JSON)
}
