use actix_web::{AsyncResponder, FutureResponse, HttpRequest, HttpResponse};
// use failure::Error;
use crate::errors::ApiError;
use futures::Future;

use crate::queries::query_types::{Query, QueryTasks};
use crate::AppState;

pub fn index(req: &HttpRequest<AppState>) -> FutureResponse<HttpResponse, ApiError> {
    let query = Query {
        columns: vec![],
        conditions: None,
        limit: 0,
        offset: 0,
        order_by: None,
        table: "information_schema.columns".to_string(),
        task: QueryTasks::GetAllTableColumns,
    };
    req.state()
        .db
        .send(query)
        .from_err()
        .and_then(|res| match res {
            Ok(rows) => Ok(HttpResponse::Ok().json(rows)),
            Err(_) => Ok(HttpResponse::InternalServerError().into()),
        })
        .responder()
}
