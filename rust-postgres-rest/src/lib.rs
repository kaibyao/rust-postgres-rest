// nightly features
#![feature(async_await)]
// used for dev/tests
#![deny(clippy::complexity, clippy::correctness, clippy::perf, clippy::style)]
// to serialize large json (like the index)
#![recursion_limit = "128"]

mod db;
mod error;
pub mod queries;

mod stats_cache;
use stats_cache::StatsCacheMessage;

pub use db::connect;
pub use error::Error;

use actix::Addr;
use futures::future::{err, ok, Either, Future};

/// API Configuration
#[derive(Clone)]
pub struct Config {
    /// The database URL. URL must be [Postgres-formatted](https://www.postgresql.org/docs/current/libpq-connect.html#id-1.7.3.8.3.6).
    pub db_url: &'static str,
    /// Requires the `stats_cache` cargo feature to be enabled (which is enabled by default). When
    /// set to `true`, caching of table stats is enabled, significantly speeding up API endpoings
    /// that use `SELECT` and `INSERT` statements. Default: `false`.
    pub is_cache_table_stats: bool,
    /// When set to a positive integer `n`, automatically refresh the Table Stats cache every `n`
    /// seconds. Default: `0` (cache is never automatically reset).
    pub cache_reset_interval_seconds: u32,
    /// Actor address for the Table Stats Cache.
    stats_cache_addr: Option<Addr<stats_cache::StatsCache>>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            db_url: "",
            is_cache_table_stats: false,
            cache_reset_interval_seconds: 0,
            stats_cache_addr: None,
        }
    }
}
// opted to not use a Builder pattern, as the number of config options makes it unwarranted
// (complexity is low)
impl Config {
    /// Creates a new Config.
    pub fn new(db_url: &'static str) -> Self {
        let mut cfg = Self::default();
        cfg.db_url = db_url;

        cfg
    }

    /// Turns on the flag for caching table stats. Substantially increases performance. Use this in
    /// production or in systems where the DB schema is not changing.
    pub fn cache_table_stats(&mut self) -> &mut Self {
        self.is_cache_table_stats = true;
        stats_cache::initialize_stats_cache(self);
        self
    }

    /// Forces the Table Stats cache to reset/refresh new data.
    pub fn reset_cache(&self) -> impl Future<Item = (), Error = Error> {
        if !self.is_cache_table_stats {
            return Either::A(err(Error::generate_error(
                "TABLE_STATS_CACHE_NOT_ENABLED",
                "".to_string(),
            )));
        }

        match &self.stats_cache_addr {
            Some(addr) => {
                let reset_cache_future = addr
                    .send(StatsCacheMessage::ResetCache)
                    .map_err(Error::from)
                    .and_then(|response_result| match response_result {
                        Ok(_response_ok) => ok(()),
                        Err(e) => err(e),
                    });
                Either::B(reset_cache_future)
            }
            None => Either::A(err(Error::generate_error(
                "TABLE_STATS_CACHE_NOT_INITIALIZED",
                "The cache to be reset was not found.".to_string(),
            ))),
        }
    }

    // Set the interval timer to automatically reset the table stats cache. If this is not set, the
    // cache is never reset.
    pub fn set_cache_reset_timer(&mut self, seconds: u32) -> &mut Self {
        self.cache_reset_interval_seconds = seconds;
        self
    }
}
