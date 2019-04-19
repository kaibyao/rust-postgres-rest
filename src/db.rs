use actix_web::actix::{Actor, Handler, SyncContext};
// use failure::Error;
use crate::errors::ApiError;
use r2d2;
use r2d2_postgres::{PostgresConnectionManager, TlsMode};

use crate::queries::{
    get_all_tables, get_table_stats, insert_into_table, query_table,
    query_types::{Query, QueryParams, QueryResult, QueryTasks},
};

/// Represents a PostgreSQL database pool
pub type Pool = r2d2::Pool<PostgresConnectionManager>;

/// Represents a single PostgreSQL database connection
pub type Connection = r2d2::PooledConnection<PostgresConnectionManager>;

/// A tuple struct that represents an actor (you could think of it as a separate service) that executes database actions/queries.
pub struct DbExecutor(pub Pool);

impl Actor for DbExecutor {
    type Context = SyncContext<Self>;
}

// We need to implement Handler in order to know what to do when data is sent to the actor via Addr::send(Queries {})
impl Handler<Query> for DbExecutor {
    type Result = Result<QueryResult, ApiError>;

    fn handle(&mut self, msg: Query, _: &mut Self::Context) -> Self::Result {
        let conn = self.0.get()?;

        match msg.task {
            QueryTasks::GetAllTables => get_all_tables(&conn),
            QueryTasks::InsertIntoTable => insert_into_table(&conn, msg),
            QueryTasks::QueryTable => query_table(&conn, msg),
            QueryTasks::QueryTableStats => {
                match msg.params {
                    QueryParams::Select(params) => get_table_stats(&conn, params.table),
                    _ => unreachable!("QueryTableStats should never be reached unless QueryParams is of the Select variant.")
                }
            },
        }
    }
}

/// Initializes the database connection pool and returns it.
pub fn init_connection_pool(db_url: &str) -> Pool {
    let manager = PostgresConnectionManager::new(db_url, TlsMode::None).unwrap();
    Pool::new(manager).unwrap()
}
