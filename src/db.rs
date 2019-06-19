use bb8;
use bb8_postgres::PostgresConnectionManager;
use futures01::Future;
use tokio_postgres::{Error, NoTls};

pub struct PgConnection;

pub type Pool = bb8::Pool<PostgresConnectionManager<NoTls>>;

impl PgConnection {
    /// Initializes the database connection pool and returns it.
    pub fn connect(db_url: &str) -> Result<Pool, Error> {
        let pg_mgr = PostgresConnectionManager::new(db_url, NoTls);

        bb8::Pool::builder().build(pg_mgr).wait()
    }
}
