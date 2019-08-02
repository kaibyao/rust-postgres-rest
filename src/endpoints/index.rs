use actix_web::HttpResponse;
use lazy_static::lazy_static;
use serde_json::{json, Value};

/// Displays a list of available endpoints and their descriptions.
pub fn index() -> HttpResponse {
    lazy_static! {
        static ref ENDPOINTS_JSON: Value = json!({
            "endpoints": {
            "/": {
                "GET": "The current endpoint. Displays REST API endpoints and available tables.",
            },
            "/table": {
                "GET": "Displays list of tables.",
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
                        "where": {
                            "default": null,
                            "description": "The WHERE clause of a SELECT statement. Remember to URI-encode the final result.",
                            "example": "(field_1 >= field_2 AND id IN (1,2,3)) OR field_2 > field_1",
                        },
                        "group_by": {
                            "default": null,
                            "description": "Comma-separated list representing the field(s) on which to group the resulting rows.",
                            "example": "name, category",
                        },
                        "order_by": {
                            "default": null,
                            "description": "Comma-separated list representing the field(s) on which to sort the resulting rows.",
                            "example": "date DESC, id ASC",
                        },
                        "limit": {
                            "default": 10000,
                            "description": "The maximum number of rows that can be returned.",
                        },
                        "offset": {
                            "default": 0,
                            "description": "The number of rows to exclude.",
                        },
                    }
                },
                "POST": {
                    "description": "Inserts new records into the table. Returns the number of rows affected.",
                    "body": {
                        "description": "An array of objects where each object represents a row and whose key-values represent column names and their values.",
                        "example": [{
                            "column_a": "a string value",
                            "column_b": 123,
                        }]
                    },
                    "query_params": {
                        "conflict_action": {
                            "default": null,
                            "options": [null, "update", "nothing"],
                            "description": "The `ON CONFLICT` action to perform (`update` or `nothing`).",
                        },
                        "conflict_target": {
                            "default": null,
                            "description": "Comma-separated list of columns that determine if a row being inserted conflicts with an existing row.",
                            "example": "id,name,field_2",
                        },
                        "returning_columns": {
                            "default": null,
                            "description": "Comma-separated list of columns to return from the INSERT operation.",
                            "example": "id,name,field_2",
                        }
                    },
                },
                "PUT": {
                    "description": "Updates table records.",
                    "body": {
                        "description": "An object whose key-values represent column names and the values to set. String values must be contained inside quotes or else they will be evaluated as expressions and not strings.",
                        "example": {
                            "column_a": "'some_string_value (notice the quotes)'",
                            "column_b": "foreign_key_example_id.foreign_key_column",
                            "column_c": 123,
                        }},
                    "query_params": {
                        "where": {
                            "default": null,
                            "description": "The WHERE clause of the UPDATE statement. Remember to URI-encode the final result.",
                            "example": "(field_1 >= field_2 AND id IN (1,2,3)) OR field_2 > field_1",
                        },
                        "returning_columns": {
                            "default": null,
                            "description": "Comma-separated list of columns to return from the UPDATE operation.",
                            "example": "id,name, field_2",
                        }
                    },
                },
                "DELETE": {
                    "description": "Deletes table records.",
                    "query_params": {
                        "where": {
                            "default": null,
                            "description": "The WHERE clause of the DELETE statement. Remember to URI-encode the final result.",
                            "example": "(field_1 >= field_2 AND id IN (1,2,3)) OR field_2 > field_1",
                        },
                        "confirm_delete": {
                            "default": null,
                            "description": "This param is required in order for DELETE operation to process.",
                        },
                        "returning_columns": {
                            "default": null,
                            "description": "Comma-separated list of columns to return from deleted rows.",
                            "example": "id,name, field_2",
                        },
                    }
                },
            }},
            "/sql": {
                "POST": "Runs a raw SQL statement. (not implemented)",
            },
        });
    }

    HttpResponse::Ok().json(&*ENDPOINTS_JSON)
}
