use actix_web::{AsyncResponder, FutureResponse, HttpRequest, HttpResponse};
use futures::Future;

use crate::errors::ApiError;
use crate::queries::query_types::{Query, QueryTasks};
use crate::AppState;

pub fn query_table(req: &HttpRequest<AppState>) -> FutureResponse<HttpResponse, ApiError> {
    let table: String = req.match_info().query("table").unwrap();

    let query_params = req.query();

    // set custom limit if available
    let default_limit = 10000;
    let limit: i32 = match query_params.get("limit") {
        Some(limit_string) => match limit_string.parse() {
            Ok(limit_i32) => limit_i32,
            Err(_) => default_limit,
        },
        None => default_limit,
    };

    // set custom offset if available
    let default_offset = 0;
    let offset: i32 = match query_params.get("offset") {
        Some(offset_string) => match offset_string.parse() {
            Ok(offset_i32) => offset_i32,
            Err(_) => default_offset,
        },
        None => default_offset,
    };

    // dbg!(query_params);
    // dbg!(limit);

    // extract columns
    let columns: Vec<String> = match &query_params.get("columns") {
        Some(columns_str) => columns_str
            .split(',')
            .map(|column_str_raw| String::from(column_str_raw.trim()))
            .collect(),
        None => vec![],
    };

    // if &columns.len() == &0 {
    //     // need to return the table's stats: number of rows, the foreign keys associated with this table, and column names + types
    // }

    // query string parameters:
    // columns
    //      requesting to `/{table}` without `columns`:
    //      - number of rows (`count(*)`)
    //      - relations (references and referenced_by)
    //      - and column names and their type
    // where ()
    // limit (default 10000)
    // offset (default 0)
    // order by

    // request headers
    // Range (alternative to limit/offset). if both limit or offset AND range headers are present, return error.

    //
    let query = Query {
        columns,
        conditions: None,
        limit,
        offset,
        order_by: None,
        table,
        task: QueryTasks::QueryTable,
    };

    req.state()
        .db
        .send(query)
        .from_err()
        .and_then(|res| match res {
            Ok(rows) => Ok(HttpResponse::Ok().json(rows)),
            // Err(_) => Ok(HttpResponse::InternalServerError().into()),
            // TODO: proper error handling
            Err(err) => {
                // let mut response = HttpResponse::InternalServerError();
                // let response2 = response.reason(err_str);
                // let response3 = response2.finish();
                // Ok(HttpResponse::InternalServerError().into())
                // Ok(HttpResponse::from_error(err))
                Err(err)
            }
        })
        .responder()
}
