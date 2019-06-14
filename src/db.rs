use actix::{Actor, Addr, Context, spawn};
use actix::prelude::*;
use actix::fut;
// use failure::Error;
use crate::errors::ApiError;
use futures::{future, stream, Future, Stream};
// use r2d2;
// use r2d2_postgres::{PostgresConnectionManager, TlsMode};
use tokio_postgres::{Client, Error, NoTls};

use crate::queries::{
    get_all_tables, //get_table_stats, insert_into_table, query_table,
};
use crate::queries::query_types::{Query, QueryParams, QueryResult, QueryTasks};

/// Represents a PostgreSQL database pool
// pub type Pool = r2d2::Pool<PostgresConnectionManager>;

/// Represents a single PostgreSQL database connection
// pub type Connection = r2d2::PooledConnection<PostgresConnectionManager>;

/// A tuple struct that represents an actor (you could think of it as a separate service) that executes database actions/queries.
// pub struct DbExecutor(pub Pool);

// impl Actor for DbExecutor {
//     type Context = SyncContext<Self>;
// }

/// Represents an actor (you could think of it as a separate service) that executes database actions/queries.
pub struct PgConnection {
    client: Option<Client>,
    // can probably add something for table stat state here
}

impl Actor for PgConnection {
    type Context = Context<Self>;
}

// We need to implement Handler in order to know what to do when data is sent to the actor via Addr::send(Queries {})
impl Handler<Query> for PgConnection {
    type Result = Box<Future<Item = QueryResult, Error = ApiError> + 'static>;

    fn handle(&mut self, msg: Query, _: &mut Self::Context) -> Self::Result {
        // let conn = match self.client.as_mut() {
        //     Some(cl) => cl,
        //     None => return Box::new(future::err::<QueryResult, ApiError>(ApiError::generate_error("NO_DATABASE_CONNECTION", "".to_string()))),
        // };

        // let f = self.client.as_mut().unwrap()
        //     .prepare("SELECT DISTINCT table_name FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name;")
        //     .and_then(|statement| self.client.as_mut().unwrap().query(&statement, &[]).collect())
        //     .map_err(|e| ApiError::from(e))
        //     .map(|rows| {
        //         QueryResult::GetAllTablesResult(rows.iter().map(|r| r.get(0)).collect())
        //     });

        match msg.task {
            QueryTasks::GetAllTables => Box::new(
                get_all_tables(self.client.as_mut().unwrap())
            ),
            // QueryTasks::InsertIntoTable => insert_into_table(&conn, msg),
            // QueryTasks::QueryTable => query_table(&conn, msg),
            // QueryTasks::QueryTableStats => {
            //     match msg.params {
            //         QueryParams::Select(params) => get_table_stats(&conn, params.table),
            //         _ => unreachable!("QueryTableStats should never be reached unless QueryParams is of the Select variant.")
            //     }
            // },
        }
    }
}

impl PgConnection {
    /// Initializes the database connection pool and returns it.
    pub fn connect(db_url: &str) -> Addr<Self> {
        let hs = tokio_postgres::connect(db_url, NoTls);

        PgConnection::create(move |ctx| {
            let actor = PgConnection {
                client: None,
            };

            hs.map_err(|_| panic!("cannot connect to postgresql"))
                .into_actor(&actor)
                .and_then(|(mut client, conn), act, ctx| {
                    actor.client = Some(client);
                    spawn(conn.map_err(|e| panic!("{}", e)));
                    fut::ok(())
                })
                .wait(ctx);

            actor
        })
    }

    // pub fn get_all_tables(&self) -> impl Future<Item = Vec<String>, Error = tokio_postgres::Error> {
    //     get_all_tables(&self.client)
    // }

    // pub fn insert_into_table(&self, msg: Query) -> impl Future<Item = QueryResult, Error = ApiError> {
    //     insert_into_table(self.client, msg)
    // }

    // pub fn query_table(&self, msg: Query) -> impl Future<Item = QueryResult, Error = ApiError> {
    //     query_table(self.client, msg)
    // }

    // pub fn get_table_stats(&self, msg: Query) -> impl Future<Item = QueryResult, Error = ApiError> {
    //     match msg.params {
    //         QueryParams::Select(params) => get_table_stats(self.client, params.table),
    //         _ => unreachable!("QueryTableStats should never be reached unless QueryParams is of the Select variant.")
    //     }
    // }
}

// non-actor version
// impl PgConnection {
//     /// Initializes the database connection pool and returns it.
//     pub fn connect(db_url: &str) -> impl Future<Item = PgConnection, Error = ()> {
//         let hs = tokio_postgres::connect(db_url, NoTls);

//         hs.map_err(|_| panic!("can not connect to postgresql"))
//             .and_then(|(mut client, conn)| {
//                 spawn(conn.map_err(|e| panic!("{}", e)));

//                 future::ok::<PgConnection, ()>(PgConnection {
//                     client
//                 })
//             })
//     }
// }
