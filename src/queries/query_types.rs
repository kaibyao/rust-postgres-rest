use super::postgres_types::RowFields;
use super::table_stats::TableStats;
use crate::errors::ApiError;
use crate::AppState;
use actix_web::{actix::Message, HttpRequest};
use serde_json::{Map, Value};

/// Represents a single SELECT query
pub struct QueryParamsSelect {
    pub distinct: Option<Vec<String>>,
    pub columns: Vec<String>,
    pub table: String,
    pub conditions: Option<String>,
    pub group_by: Option<Vec<String>>,
    pub order_by: Option<Vec<String>>,
    pub limit: i32,
    pub offset: i32,
    pub prepared_values: Option<String>,
}

impl QueryParamsSelect {
    /// Fills the struct’s values based on the HttpRequest data.
    pub fn from_http_request(req: &HttpRequest<AppState>) -> Self {
        let default_limit = 10000;
        let default_offset = 0;

        let query_params = req.query();

        QueryParamsSelect {
            columns: match query_params.get("columns") {
                Some(columns_str) => Self::normalize_columns(columns_str),
                None => vec![],
            },
            distinct: match query_params.get("distinct") {
                Some(distinct_str) => Some(
                    Self::normalize_columns(distinct_str),
                ),
                None => None,
            },
            table: match req.match_info().query("table") {
                Ok(table_name) => table_name,
                Err(_) => unreachable!(
                    "this function should really only be called with a request that contains table"
                ),
            },
            conditions: match query_params.get("where") {
                Some(where_string) => Some(where_string.to_lowercase()),
                None => None,
            },
            group_by: match query_params.get("group_by") {
                Some(group_by_str) => Some(Self::normalize_columns(group_by_str)),
                None => None,
            },
            order_by: match query_params.get("order_by") {
                Some(order_by_str) => Some(Self::normalize_columns(order_by_str)),
                None => None,
            },
            limit: match query_params.get("limit") {
                Some(limit_string) => match limit_string.parse() {
                    Ok(limit_i32) => limit_i32,
                    Err(_) => default_limit,
                },
                None => default_limit,
            },
            offset: match query_params.get("offset") {
                Some(offset_string) => match offset_string.parse() {
                    Ok(offset_i32) => offset_i32,
                    Err(_) => default_offset,
                },
                None => default_offset,
            },
            prepared_values: match query_params.get("prepared_values") {
                Some(prepared_values) => Some(prepared_values.clone()),
                None => None,
            },
        }
    }

    fn normalize_columns(columns_str: &str) -> Vec<String> {
        columns_str
                    .split(',')
                    .map(|s| s.trim().to_lowercase())
                    .collect()
    }
}

/// Represents a single INSERT query
pub struct QueryParamsInsert {
    pub conflict_action: Option<String>,
    pub conflict_target: Option<Vec<String>>,
    pub returning_columns: Option<Vec<String>>,
    pub rows: Vec<Map<String, Value>>,
    pub table: String,
}

impl QueryParamsInsert {
    /// Fills the struct’s values based on the HttpRequest data.
    pub fn from_http_request(req: &HttpRequest<AppState>, body: Value) -> Result<Self, ApiError> {
        let table = match req.match_info().query("table") {
            Ok(table_name) => table_name,
            Err(_) => unreachable!("Not possible to reach this endpoint without a table name."),
        };

        // generate ON CONFLICT data
        let conflict_action = match req.query().get("conflict_action") {
            Some(action_str) => Some(action_str.to_string().to_lowercase()),
            None => None,
        };
        let conflict_target: Option<Vec<String>> = match req.query().get("conflict_target") {
            Some(targets_str) => Some(
                targets_str
                    .split(',')
                    .map(|target_str| target_str.to_string().to_lowercase())
                    .collect(),
            ),
            None => None,
        };
        if (conflict_action.is_some() && conflict_target.is_none())
            || (conflict_action.is_none() && conflict_target.is_some())
        {
            return Err(ApiError::generate_error("INCORRECT_REQUEST_BODY", "`conflict_action` and `conflict_target` must both be present for the `ON CONFLICT` clause to be generated correctly.".to_string()));
        }

        if let (Some(conflict_action_str), Some(conflict_target_vec)) =
            (&conflict_action, &conflict_target)
        {
            // Some validation checking of conflict_action and conflict_target
            if conflict_action_str != "nothing" && conflict_action_str != "update" {
                return Err(ApiError::generate_error(
                    "INCORRECT_REQUEST_BODY",
                    "Valid options for `conflict_action` are: `nothing`, `update`.".to_string(),
                ));
            }

            if conflict_target_vec.is_empty() {
                return Err(ApiError::generate_error(
                    "INCORRECT_REQUEST_BODY",
                    "`conflict_target` must be a comma-separated list of column names and include at least one column name.".to_string(),
                ));
            }

            if conflict_target_vec
                .iter()
                .any(|conflict_target_str| *conflict_target_str == "")
            {
                return Err(ApiError::generate_error(
                    "INCORRECT_REQUEST_BODY",
                    "<Empty string> is not a valid column name for the parameter`conflict_target`."
                        .to_string(),
                ));
            }
        }

        // generate RETURNING data
        let returning_columns = match req.query().get("returning_columns") {
            Some(columns_str) => {
                if columns_str == "" {
                    return Err(ApiError::generate_error(
                        "INCORRECT_REQUEST_BODY",
                        "`conflict_target` must be a comma-separated list of column names and include at least one column name.".to_string(),
                    ));
                }

                let returning_columns_vec = columns_str
                        .split(',')
                        .map(|column_str| -> Result<String, ApiError> {
                            if column_str == "" {
                                return Err(ApiError::generate_error(
                                    "INCORRECT_REQUEST_BODY",
                                    "`conflict_target` must be a comma-separated list of column names and include at least one column name.".to_string(),
                                ));
                            }

                            Ok(column_str.to_string())
                        })
                        .collect::<Result<Vec<String>, ApiError>>()?;

                Some(returning_columns_vec)
            }
            None => None,
        };

        let rows: Vec<Map<String, Value>> = match body.as_array() {
            Some(body_rows_to_insert) => {
                if !body_rows_to_insert
                .iter().all(Value::is_object) {
                    return Err(ApiError::generate_error("INCORRECT_REQUEST_BODY", "The body needs to be an array of objects where each object represents a row and whose key-values represent column names and their values.".to_string()));
                }

                body_rows_to_insert
                .iter().map(|json_value| {
                    if let Some(row_obj_map) = json_value.as_object() {
                        row_obj_map.clone()
                    } else {
                        unreachable!("Taken care of via above conditional.")
                    }
                })
                .collect()
            },
            None => return Err(ApiError::generate_error("INCORRECT_REQUEST_BODY", "The body needs to be an array of objects where each object represents a row and whose key-values represent column names and their values.".to_string())),
        };

        Ok(QueryParamsInsert {
            conflict_action,
            conflict_target,
            returning_columns,
            rows,
            table,
        })
    }
}

pub enum QueryParams {
    Select(QueryParamsSelect),
    Insert(QueryParamsInsert),
}

/// Represents a database task (w/ included query) to be performed by DbExecutor
pub struct Query {
    pub params: QueryParams,
    pub task: QueryTasks,
}

impl Message for Query {
    type Result = Result<QueryResult, ApiError>;
}

/// Represents the different query tasks that is performed by this library
pub enum QueryTasks {
    GetAllTables,
    InsertIntoTable,
    // UpsertIntoTable,
    // DeleteTableRows,
    // UpdateTableRows,
    QueryTable,
    QueryTableStats,
}

#[derive(Serialize)]
pub struct RowsAffectedQueryResult {
    num_rows: u64,
}

#[derive(Serialize)]
#[serde(untagged)]
/// Represents the response from sending a QueryTask to DbExecutor
pub enum QueryResult {
    GetAllTablesResult(Vec<String>),
    QueryTableResult(Vec<RowFields>),
    RowsAffected(RowsAffectedQueryResult),
    TableStats(TableStats),
}

impl QueryResult {
    pub fn from_num_rows_affected(num_rows: u64) -> Self {
        QueryResult::RowsAffected(RowsAffectedQueryResult { num_rows })
    }
}
