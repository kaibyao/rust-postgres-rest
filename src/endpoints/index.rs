use actix_web::HttpResponse;
use serde_json::{json, Value};

pub fn index() -> HttpResponse {
    lazy_static! {
        // static ref TABLE_COLUMN_STAT_HELP: Value = json!({
        //     "column_name": "column_name_string",
        //     "column_type": "valid PostgreSQL type",
        //     "default_value": "default value (use single quote for string value)",
        //     "is_nullable": "whether NULL can be a column value (default true)",
        //     "is_foreign_key": "whether this column references another table column (default false)",
        //     "foreign_key_table": "table being referenced (if is_foreign_key). Default null",
        //     "foreign_key_columns": "table column being referenced (if is_foreign_key). Default null",
        //     "char_max_length": "If data_type identifies a character or bit string type, the declared maximum length; null for all other data types or if no maximum length was declared.",
        //     "char_octet_length": "If data_type identifies a character type, the maximum possible length in octets (bytes) of a datum; null for all other data types. The maximum octet length depends on the declared character maximum length (see above) and the server encoding.",
        // });
        static ref ENDPOINTS_JSON: Value = json!({
            "endpoints": {
            "/": {
                "GET": "The current endpoint. Displays REST API endpoints and available tables.",
            },
            "/table": {
                "GET": "Displays list of tables.",
                // "POST": {
                //     "description": "Create table.",
                //     "body": {
                //         "description": "A JSON object describing the table name, columns, and constraints",
                //         "schema": {
                //             "table_name": "The table name.",
                //             "columns": [*TABLE_COLUMN_STAT_HELP],
                //         },
                //     },
                // },
                // "PUT|PATCH": "Update table (not implemented)",
                // "DELETE": "Delete table (not implemented)",
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
                            "description": "The WHERE clause of a SELECT statement. Remember to URI-encode the final result. NOTE: $1, $2, etc. can be used in combination with `prepared_values` to create prepared statements (see https://www.postgresql.org/docs/current/sql-prepare.html).",
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
                        "prepared_values": {
                            "default": null,
                            "description": "If the WHERE clause contains ${number}, this comma-separated list of values is used to substitute the numbered parameters.",
                            "example": "col2,'Test'",
                        },
                    }
                },
                "POST": {
                    "description": "Inserts new records into the table.",
                    "body": "An array of objects where each object represents a row and whose key-values represent column names and their values.",
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
                "PUT|PATCH": {
                    "description": "Updates table records (not implemented).",
                    "body": "An object whose key-values represent column names and the values to set",
                    "query_params": {
                        "where": {
                            "default": null,
                            "description": "The WHERE clause of the UPDATE statement. Remember to URI-encode the final result. NOTE: $1, $2, etc. can be used in combination with `prepared_values` to create prepared statements (see https://www.postgresql.org/docs/current/sql-prepare.html).",
                            "example": "(field_1 >= field_2 AND id IN (1,2,3)) OR field_2 > field_1",
                        },
                        "prepared_values": {
                            "default": null,
                            "description": "If the WHERE clause contains ${number}, this comma-separated list of values is used to substitute the numbered parameters.",
                            "example": "col2,'Test'",
                        },
                    },
                },
                "DELETE": {
                    "description": "Deletes table records (not implemented).",
                    "query_params": {
                        "where": {
                            "default": null,
                            "description": "The WHERE clause of the UPDATE statement. Remember to URI-encode the final result. NOTE: $1, $2, etc. can be used in combination with `prepared_values` to create prepared statements (see https://www.postgresql.org/docs/current/sql-prepare.html).",
                            "example": "(field_1 >= field_2 AND id IN (1,2,3)) OR field_2 > field_1",
                        },
                        "prepared_values": {
                            "default": null,
                            "description": "If the WHERE clause contains ${number}, this comma-separated list of values is used to substitute the numbered parameters.",
                            "example": "col2,'Test'",
                        },
                        "confirm_delete": {
                            "default": null,
                            "description": "This param is required in order for DELETE operation to process.",
                        }
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
