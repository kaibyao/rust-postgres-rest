
use bb8;
use bb8_postgres::PostgresConnectionManager;
use futures::Future;
use tokio_postgres::{Error, NoTls};

pub struct PgConnection;

//     // pub fn insert_into_table(&self, msg: Query) -> impl Future<Item = QueryResult, Error = ApiError> {
//     //     insert_into_table(self.client, msg)
//     // }

//     // pub fn query_table(&self, msg: Query) -> impl Future<Item = QueryResult, Error = ApiError> {
//     //     query_table(self.client, msg)
//     // }

//     // pub fn get_table_stats(&self, msg: Query) -> impl Future<Item = QueryResult, Error = ApiError> {
//     //     match msg.params {
//     //         QueryParams::Select(params) => get_table_stats(self.client, params.table),
//     //         _ => unreachable!("QueryTableStats should never be reached unless QueryParams is of the Select variant.")
//     //     }
//     // }
// }

pub type Pool = bb8::Pool<PostgresConnectionManager<NoTls>>;

impl PgConnection {
    /// Initializes the database connection pool and returns it.
    pub fn connect(db_url: &str) -> Result<Pool, bb8::RunError<Error>> {
        let pg_mgr = PostgresConnectionManager::new(db_url, NoTls);

        bb8::Pool::builder()
            .build(pg_mgr)
            .map_err(bb8::RunError::User)
            .wait()
    }
}
