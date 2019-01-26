use actix_web::{AsyncResponder, FutureResponse, HttpRequest, HttpResponse};
use failure::Error;
use futures::Future;

use crate::queries::query_types::{Query, QueryTasks};
use crate::AppState;

pub fn query_table(req: &HttpRequest<AppState>) -> FutureResponse<HttpResponse, Error> {
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

    dbg!(query_params);
    dbg!(limit);

    // query string parameters:
    // columns
    //      requesting to `/{table}` without `columns`:
    //      - number of rows (`count(*)`)
    //      - relations (references and referenced_by)
    //      - and column names and their type
    // where ()
    // limit (default 10000)
    // offset (default 0)

    // request headers
    // Range (alternative to limit/offset). if both limit or offset AND range headers are present, return error.

    //
    let query = Query {
        columns: vec![
            "test_bigint".to_string(),
            "test_bigserial".to_string(),
            "test_bit".to_string(),
            "test_bool".to_string(),
            "test_bytea".to_string(),
            "test_char".to_string(),
            "test_citext".to_string(),
            "test_date".to_string(),
            "test_float8".to_string(),
            "test_hstore".to_string(),
            "test_int".to_string(),
            "test_json".to_string(),
            "test_jsonb".to_string(),
            "test_macaddr".to_string(),
            "test_name".to_string(),
            "test_oid".to_string(),
            "test_real".to_string(),
            "test_serial".to_string(),
            "test_smallint".to_string(),
            "test_smallserial".to_string(),
            "test_text".to_string(),
            "test_time".to_string(),
            "test_timestamp".to_string(),
            "test_timestamptz".to_string(),
            "test_uuid".to_string(),
            "test_varbit".to_string(),
            "test_varchar".to_string(),
        ],
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
            Err(_) => {
                // let mut response = HttpResponse::InternalServerError();
                // let response2 = response.reason(err_str);
                // let response3 = response2.finish();
                Ok(HttpResponse::InternalServerError().into())
            }
        })
        .responder()
}
