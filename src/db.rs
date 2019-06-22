use bb8;
use bb8_postgres::PostgresConnectionManager;
use futures::Future;
use tokio_postgres::{Error as PgError, NoTls};

pub type Pool = bb8::Pool<PostgresConnectionManager<NoTls>>;

/// Initializes the database connection pool and returns it.
pub fn connect(db_url: &str) -> Result<Pool, bb8::RunError<PgError>> {
    let manager = PostgresConnectionManager::new(db_url, NoTls);

    bb8::Pool::builder()
        .build(manager)
        .map_err(bb8::RunError::User)
        .wait()
}
