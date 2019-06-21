use futures::Future;
use l337_postgres::l337::Conn;
use l337_postgres::l337::{Config, Error as L337Error, Pool as L337PgPool};
use l337_postgres::PostgresConnectionManager;
use tokio_postgres::{Error as PgError, NoTls};

pub type Pool = L337PgPool<PostgresConnectionManager<NoTls>>;
pub type Connection = Conn<PostgresConnectionManager<NoTls>>;

/// Initializes the database connection pool and returns it.
pub fn connect(db_url: &str) -> Result<Pool, L337Error<PgError>> {
    let manager = PostgresConnectionManager::new(db_url.parse().unwrap(), NoTls);

    let config: Config = Config::default();
    dbg!();
    let pool = Pool::new(manager, config).wait();
    dbg!();
    pool
}
