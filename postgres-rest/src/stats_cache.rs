use crate::{
    queries::{select_all_table_stats, select_all_tables, TableStats},
    Config, Error,
};
use actix::{spawn, Actor, Addr, Context, Handler, Message, ResponseFuture};
use clokwerk::{Scheduler, TimeUnits};
use futures::{
    future::{err, ok, Either},
    Future, Stream,
};
use futures03::future::{FutureExt, TryFutureExt};
use lazy_static::lazy_static;
use std::{
    collections::HashMap,
    ops::Deref,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, Once, RwLock,
    },
    time::{Duration, Instant},
};
use tokio::timer::Interval;
use tokio_postgres::{tls::MakeTlsConnect, Client, Socket};

#[derive(Debug)]
pub(crate) enum StatsCacheMessage {
    ResetCache,
    FetchStatsForTable(String),
}

#[derive(Debug)]
pub(crate) enum StatsCacheResponse {
    OK,
    TableStat(Option<TableStats>),
}

impl Message for StatsCacheMessage {
    type Result = Result<StatsCacheResponse, Error>;
}

/// Contains the Table Stats cache.
pub(crate) struct StatsCache {
    /// Multi-threaded access to the table stats cache.
    cache: Arc<RwLock<Option<HashMap<String, TableStats>>>>,
    /// Whether the cache is currently being fetched/reset.
    is_fetching: Arc<AtomicBool>,
}

impl Actor for StatsCache {
    type Context = Context<Self>;

    /// Gets called after `.start()` finishes.
    fn started(&mut self, _ctx: &mut Context<Self>) {
        println!("Initializing Table Stats cache...");

        let init_future = self
            .reset_cache()
            .map_err(|e| {
                panic!("Could not initialize Table Stats cache: {}", e);
            })
            .map(|_| println!("Table Stats cache initialized."));

        spawn(init_future);
    }
}

impl Handler<StatsCacheMessage> for StatsCache {
    type Result = ResponseFuture<StatsCacheResponse, Error>;

    /// Lets us know how to process messages that are sent via `Actor::send()`.
    fn handle(&mut self, msg: StatsCacheMessage, _: &mut Self::Context) -> Self::Result {
        let response = match msg {
            StatsCacheMessage::ResetCache => {
                let reset_cache_future = self.reset_cache().map(|_| StatsCacheResponse::OK);
                Either::A(reset_cache_future)
            }
            StatsCacheMessage::FetchStatsForTable(table) => {
                let fetch_stats_future = match self.fetch_table_stats(table) {
                    Ok(response) => ok(response),
                    Err(e) => err(e),
                };
                Either::B(fetch_stats_future)
            }
        };

        Box::new(response)
    }
}

impl StatsCache {
    /// Creates a new instance of `StatsCache`.
    pub fn new() -> Self {
        StatsCache {
            cache: Arc::new(RwLock::new(None)),
            is_fetching: Arc::new(AtomicBool::new(false)),
        }
    }

    fn fetch_table_stats(&self, table: String) -> Result<StatsCacheResponse, Error> {
        match self.cache.read() {
            Ok(cache) => {
                if let Some(stat_hash) = cache.deref() {
                    if let Some(stat) = stat_hash.get(&table) {
                        return Ok(StatsCacheResponse::TableStat(Some(stat.clone())));
                    }
                }

                Ok(StatsCacheResponse::TableStat(None))
            }
            Err(e) => Err(Error::from(e)),
        }
    }

    fn reset_cache(&mut self) -> ResponseFuture<(), Error> {
        if !self.is_fetching.load(Ordering::SeqCst) {
            self.is_fetching.store(true, Ordering::SeqCst);

            let is_fetching_clone = self.is_fetching.clone();
            let cache_clone = self.cache.clone();

            // This is safe to do, because we're already ensuring that another call to reset_cache
            // will not get to this point.
            let mut persistent_db_conn_opt = PERSISTENT_DB_CONNECTION.lock().unwrap();
            let client = persistent_db_conn_opt.take().unwrap();
            let f = select_all_tables(client)
                .and_then(|(tables, client)| {
                    select_all_table_stats(client, tables).boxed().compat()
                })
                .and_then(move |(table_stats, client)| {
                    let mut cache = match cache_clone.write() {
                        Ok(cache) => cache,
                        Err(e) => return Err(Error::from(e)),
                    };

                    *cache = Some(table_stats);
                    persistent_db_conn_opt.replace(client);
                    is_fetching_clone.store(false, Ordering::SeqCst);
                    Ok(())
                });

            Box::new(f)
        } else {
            Box::new(ok(()))
        }
    }
}

lazy_static! {
    /// The call to initialize the stats cache.
    static ref INIT_STATS_CACHE: Once = Once::new();
    /// Used to determine whether the stats cache has been initialized.
    static ref IS_STATS_CACHE_INIT: AtomicBool = AtomicBool::new(false);
    /// Stores the actor address containing the cache data.
    static ref STATS_CACHE_MUTEX: Mutex<Option<Addr<StatsCache>>> = Mutex::new(None);
    /// Active connection to Postgres.
    static ref PERSISTENT_DB_CONNECTION: Mutex<Option<Client>> = Mutex::new(None);
}

// todo, replace config.stats_cache_addr with a static variable + function here

/// Initializes the stats cache and stores the actix actor's address into the passed `Config`
/// struct. The cache is initialized once. Any additional requests to this function (from other
/// threads, for example) will get the existing address.
pub fn initialize_stats_cache<T: MakeTlsConnect<Socket> + Clone + Send + Sync>(
    config: &mut Config<T>,
) {
    // initialize table stats cache actor once.
    if !IS_STATS_CACHE_INIT.load(Ordering::SeqCst) {
        //     let static_addr_opt = STATS_CACHE_MUTEX.lock().unwrap();
        //     config.stats_cache_addr = Some(static_addr_opt.as_ref().unwrap().clone());
        // } else {
        let cache_reset_interval_seconds = config.cache_reset_interval_seconds;
        INIT_STATS_CACHE.call_once(|| {
            spawn(
                config
                    .connect()
                    .map_err(|e| panic!("Could not connect to db to initialize stats cache: {}", e))
                    .map(move |client| {
                        let mut static_addr_opt = STATS_CACHE_MUTEX.lock().unwrap();
                        let mut persistent_db_conn_opt = PERSISTENT_DB_CONNECTION.lock().unwrap();
                        persistent_db_conn_opt.get_or_insert(client);

                        let addr = StatsCache::new().start();
                        let addr_clone = addr.clone();

                        static_addr_opt.get_or_insert(addr);

                        IS_STATS_CACHE_INIT.store(true, Ordering::SeqCst);

                        // set cache reset interval
                        if cache_reset_interval_seconds > 0 {
                            let mut scheduler = Scheduler::new();
                            scheduler.every(cache_reset_interval_seconds.seconds()).run(
                                move || {
                                    let reset_future = addr_clone
                                        .send(StatsCacheMessage::ResetCache)
                                        .then(|result| match result {
                                            Ok(response_result) => match response_result {
                                                Ok(_) => {
                                                    println!("Table Stats Cache has been reset.");
                                                    Ok(())
                                                }
                                                Err(e) => {
                                                    eprintln!("{}", e);
                                                    Err(())
                                                }
                                            },
                                            Err(e) => {
                                                eprintln!("{}", e);
                                                Err(())
                                            }
                                        });

                                    spawn(reset_future);
                                },
                            );

                            // spawn a future that loops endlessly, running any pending scheduler
                            // tasks
                            let interval = Interval::new(
                                Instant::now() + Duration::from_millis(1),
                                Duration::from_secs(1),
                            );
                            spawn(
                                interval
                                    .for_each(move |_instant| {
                                        scheduler.run_pending();
                                        ok(())
                                    })
                                    .map_err(|e| panic!(e)),
                            );
                        }
                    }),
            );
        });
    }
}

/// Returns an option containing the actix actor address of the Table Stats cache.
pub(crate) fn get_stats_cache_addr() -> Option<Addr<StatsCache>> {
    let opt = STATS_CACHE_MUTEX.lock().unwrap();
    opt.clone()
}
