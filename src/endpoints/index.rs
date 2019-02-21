use crate::errors::ApiError;
use actix_web::{AsyncResponder, FutureResponse, HttpRequest, HttpResponse};
use futures::Future;
use serde_json::{json, Value};

use crate::queries::query_types::{Query, QueryTasks};
use crate::AppState;

pub fn index(req: &HttpRequest<AppState>) -> FutureResponse<HttpResponse, ApiError> {
    lazy_static! {
        static ref endpoints_json: Value = json!({
            "/": {
                "GET": "The current endpoint. Displays REST API endpoints and available tables."
            },
            "/{table}": {
                "GET": {
                    "description": "Queries {table} with given parameters using SELECT. If no columns are provided, returns stats for {table}.",
                    "query_params": {
                        "limit": {
                            "default": 10000,
                            "description": "The maximum number of rows that can be returned.",
                            "type": "integer"
                        },
                        "offset": {
                            "default": 0,
                            "description": "The number of rows to exclude.",
                            "type": "integer"
                        },
                        "order_by": {
                            "default": null,
                            "description": "The field(s) on which to sort the resulting rows.",
                            "example": "date DESC, id ASC",
                            "type": "string"
                        },
                        "where": {
                            "default": null,
                            "description": "The WHERE clause of a SELECT statement. Uses the language detailed below. Remember to URI-encode the final result.",
                            "example": "(field_1>=field_2ANDidIN(1,2,3))ORfield_2>field_1",
                            "type": "string",
                            "language": {
                                "=": "Equals",
                                "!=": "Not equal to",
                                ">": "Greater than",
                                "<": "Less than",
                                ">=": "Greater than or equal to",
                                "<=": "Less than or equal to",
                                "AND": "AND operator",
                                "OR": "OR operator",
                                "NOT": "NOT operator (use as prefix to other comparison operators)",
                                "LIKE": "Case-sensitive search on field value (use _ and % as wildcards)",
                                "ILIKE": "Like, but case-insensitive",
                                "IN": "IN operator (one of a list of values)",
                                "IS": "Exact equality (null, true, false)",
                                "BETWEEN": "Between two dates. Example: `some_dateBETWEENx:y`",
                                "OVERLAPS": "Overlap in date ranges. Example: `start1:end1OVERLAPSstart2:end2`"
                            }
                        }
                    }
                }
            }
        });
    }

    let query: Query = Query {
        columns: vec![],
        conditions: None,
        limit: 0,
        offset: 0,
        order_by: None,
        table: "".to_string(),
        task: QueryTasks::GetAllTables,
    };

    req.state()
        .db
        .send(query)
        .from_err()
        .and_then(|res| match res {
            Ok(tables) => Ok(HttpResponse::Ok().json(json!({
                "endpoints": &*endpoints_json,
                "tables": tables
            }))),
            Err(err) => Err(err),
        })
        .responder()
}
