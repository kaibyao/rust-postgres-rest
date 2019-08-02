# experiment00

Use `actix-web` to serve a REST API for your PostgreSQL database.

```rust
use actix::System;
use actix_web::{App, HttpServer};
use experiment00::{generate_rest_api_scope, AppConfig};

fn main() {
    let sys = System::new("my_app_runtime"); // create Actix runtime

    let ip_address = "127.0.0.1:3000";

    // start 1 server on each cpu thread
    HttpServer::new(move || {
        let mut config = AppConfig::new();
        config.db_url = "postgresql://postgres@0.0.0.0:5432/postgres";

        App::new().service(
            // appends an actix-web Scope under the "/api" endpoint to app.
            generate_rest_api_scope(config),
        )
    })
    .bind(ip_address)
    .expect("Can not bind to port 3000")
    .start();

    println!("Running server on {}", ip_address);
    sys.run().unwrap();
}
```

`generate_rest_api_scope()` creates the `/api/table` and `/api/{table}` endpoints, which allow for CRUD operations on table rows in your database.

## Features

### It’s fast

TBD.

### Easy foreign-key references using DOT (`.`) syntax

You can use dots (`.`) to easily walk through foreign keys and retrieve values of rows in related tables!

Assume the following schema:

```postgre
-- DB setup
CREATE TABLE public.company (
  id BIGINT CONSTRAINT company_id_key PRIMARY KEY,
  name TEXT
);

CREATE TABLE public.school (
  id BIGINT CONSTRAINT school_id_key PRIMARY KEY,
  name TEXT
);

CREATE TABLE public.adult (
  id BIGINT CONSTRAINT adult_id_key PRIMARY KEY,
  company_id BIGINT,
  name TEXT
);
ALTER TABLE public.adult ADD CONSTRAINT adult_company_id FOREIGN KEY (company_id) REFERENCES public.company(id);

CREATE TABLE public.child (
  id BIGINT CONSTRAINT child_id_key PRIMARY KEY,
  parent_id BIGINT,
  school_id BIGINT,
  name TEXT
);
ALTER TABLE public.child ADD CONSTRAINT child_parent_id FOREIGN KEY (parent_id) REFERENCES public.adult(id);
ALTER TABLE public.child ADD CONSTRAINT child_school_id FOREIGN KEY (school_id) REFERENCES public.school(id);

INSERT INTO public.company (id, name) VALUES (100, 'Stark Corporation');
INSERT INTO public.school (id, name) VALUES (10, 'Winterfell Tower');
INSERT INTO public.adult (id, company_id, name) VALUES (1, 100, 'Ned');
INSERT INTO public.child (id, name, parent_id, school_id) VALUES (1000, 'Robb', 1, 10);
```

Runing the `GET` operation:

```bash
GET "/api/child?columns=id,name,parent_id.name,parent_id.company_id.name"
#         |             ------------------------------------------------ column names
#         ^^^^^ {table} value
```

Will return the following JSON:

```json
[
  {
    "id": 1000,
    "name": "Robb",
    "parent_id.name": "Ned",
    "parent_id.company_id.name": "Stark Corporation"
  }
]
```

#### Alias (`AS`) syntax is supported too

Changing the previous API endpoint to `/api/child?columns=id,name,parent_id.name as parent_name,parent_id.company_id.name as parent_company_name` will return the aliased fields instead:

```json
[
  {
    "id": 1000,
    "name": "Robb",
    "parent_name": "Ned",
    "parent_company_name": "Stark Corporation"
  }
]
```

## Configuration

The `AppConfig` struct contains the configuration options used by this library.

### `db_url: &'static str (default: "")`

The database URL. URL must be [Postgres-formatted](https://www.postgresql.org/docs/current/libpq-connect.html#id-1.7.3.8.3.6).

### `is_cache_table_stats: bool (default: false)`

Requires the `stats_cache` cargo feature to be enabled (which is enabled by default). When set to `true`, caching of table stats is enabled, significantly speeding up API endpoings that use `SELECT` and `INSERT` statements.

### `is_cache_reset_endpoint_enabled: bool (default: false)`

Requires the `stats_cache` cargo feature to be enabled (which is enabled by default). When set to `true`, an additional API endpoint is made available at `{scope_name}/reset_table_stats_cache`, which allows for manual resetting of the Table Stats cache. This is useful if you want a persistent cache that only needs to be reset on upgrades, for example.

### `cache_reset_interval_seconds: u32 (default: 0)`

Requires the `stats_cache` cargo feature to be enabled (which is enabled by default). When set to a positive integer `n`, automatically refresh the Table Stats cache every `n` seconds. When set to `0`, the cache is never automatically reset.

### `scope_name: &'static str (default: "/api")`

The API endpoint that contains all of the other API operations available in this library.

## Endpoints

### `GET /`

Displays a list of all available endpoints and their descriptions + how to use instructions.

### `GET /{table}`

Queries {table} with given parameters using SELECT. If no columns are provided, column stats for {table} are returned. DOT (`.`) syntax can be used in `columns`, `distinct`, `where`, `group_by`, and `order_by`.

#### Query Parameters for `GET /{table}`

##### columns

A comma-separated list of column names for which values are retrieved. Example: `col1,col2,col_infinity`.

##### distinct

A comma-separated list of column names for which rows that have duplicate values are excluded. Example: `col1,col2,col_infinity`.

##### where

The WHERE clause of a SELECT statement. Remember to URI-encode the final result. Example: `(field_1 >= field_2 AND id IN (1,2,3)) OR field_2 > field_1`.

##### group_by

Comma-separated list representing the field(s) on which to group the resulting rows. Example: `name, category`.

##### order_by

Comma-separated list representing the field(s) on which to sort the resulting rows. Example: `date DESC, id ASC`.

##### limit

The maximum number of rows that can be returned. Default: `10000`.

##### offset

The number of rows to exclude. Default: `0`.

### `POST /{table}`

Inserts new records into the table. Returns the number of rows affected. Optionally, table columns of affected rows can be returned using the `returning_columns` query parameter (see below).

#### Query Parameters for `POST /{table}`

##### conflict_action

The `ON CONFLICT` action to perform (can be `update` or `nothing`).

##### conflict_target

Comma-separated list of columns that determine if a row being inserted conflicts with an existing row. Example: `id,name,field_2`.

##### returning_columns

Comma-separated list of columns to return from the INSERT operation. Example: `id,name,field_2`. Unfortunately PostgreSQL has no native foreign key functionality for `RETURNING` columns, so only columns that are on the table being inserted can be returned.

#### Body schema for `POST /{table}`

An array of objects where each object represents a row and whose key-values represent column names and their values.

#### Examples for `POST /{table}`

##### Simple insert

```plaintext
POST /api/child
{
  "id": 1001,
  "name": "Sansa",
  "parent_id": 1,
  "school_id": 10
}
```

returns `{ "num_rows": 1 }`.

##### `ON CONFLICT DO NOTHING`

Assuming the “Simple Insert” example above was run:

```plaintext
POST /api/child?conflict_action=nothing&conflict_target=id
{
  "id": 1001,
  "name": "Arya",
  "parent_id": 1,
  "school_id": 10
}
```

returns `{ "num_rows": 0 }`.

##### `ON CONFLICT DO UPDATE`

Assuming the “Simple Insert” example above was run:

```plaintext
POST /api/child?conflict_action=update&conflict_target=id
{
  "id": 1001,
  "name": "Arya",
  "parent_id": 1,
  "school_id": 10
}
```

returns `{ "num_rows": 1 }`. `name: "Sansa"` has been replaced with `name: "Arya"`.

##### `returning_columns`

```plaintext
POST /api/child?returning_columns=id,name
{
  "id": 1002,
  "name": "Arya",
  "parent_id": 1,
  "school_id": 10
}
```

returns `[{ "id": 1002, "name": "Arya" }]`.

### `PUT /{table}`

Updates existing records in `{table}`. Returns the number of rows affected. Optionally, table columns of affected rows can be returned using the `returning_columns` query parameter (see below). DOT (`.`) syntax can be used in `where`, `returning_columns`, as well as the request body (see examples).

#### Query Parameters for `PUT /{table}`

##### where (PUT)

The WHERE clause of an UPDATE statement. Remember to URI-encode the final result. Example: `(field_1 >= field_2 AND id IN (1,2,3)) OR field_2 > field_1`.

##### returning_columns (PUT)

Comma-separated list of columns to return from the UPDATE operation. Example: `id,name,field_2`.

#### Body schema for `PUT /{table}`

An object whose key-values represent column names and the values to set. String values must be contained inside quotes or else they will be evaluated as expressions and not strings.

#### Examples for `PUT /{table}`

Assume the following database schema for these examples:

```postgre
CREATE TABLE IF NOT EXISTS public.coach (
  id BIGINT CONSTRAINT coach_id_key PRIMARY KEY,
  name TEXT
);
CREATE TABLE IF NOT EXISTS public.team (
  id BIGINT CONSTRAINT team_id_key PRIMARY KEY,
  coach_id BIGINT,
  name TEXT
);
CREATE TABLE IF NOT EXISTS public.player (
  id BIGINT CONSTRAINT player_id_key PRIMARY KEY,
  team_id BIGINT,
  name TEXT
);

ALTER TABLE public.player ADD CONSTRAINT player_team_reference FOREIGN KEY (team_id) REFERENCES public.team(id);
ALTER TABLE public.team ADD CONSTRAINT team_coach_reference FOREIGN KEY (coach_id) REFERENCES public.coach(id);

INSERT INTO public.coach (id, name) VALUES
  (2, 'Doc Rivers'),
  (4, 'Bill Donovan'),
  (5, 'Mike D''Antoni');
INSERT INTO public.team (id, coach_id, name) VALUES
  (2, 2, 'LA Clippers'),
  (4, 4, 'OKC Thunder'),
  (5, 5, 'Houston Rockets');
INSERT INTO public.player
  (id, name, team_id)
  VALUES
  (3, 'Garrett Temple', 2),
  (4, 'Wilson Chandler', 2),
  (5, 'Russell Westbrook', 4);

```

##### Simple update

```plaintext
PUT /api/player?where=id%3D5
{ "team_id": 5 }

Result:
{ "num_rows": 1 }
Russell Westbrook’s team_id is now 5.
```

##### `returning_columns` (PUT)

```plaintext
PUT /api/player?where=id%3D5&returning_columns=name,team_id
{ "team_id": 5 }

Result:
[{ "name": "Russell Westbrook", "team_id": 5 }]
Russell Westbrook’s team_id is now 5.
```

##### String values

```plaintext
PUT /api/player?where=name%3D'Russell Westbrook'&returning_columns=name
                             ^-----------------^ Notice the quotes used to pass a string value
Body:
{ "name": "'Chris Paul'" }
           ^----------^ Notice the quotes used to pass a string value

Result:
[{ "name": "Chris Paul" }]
Russell Westbrook’s name has been changed to 'Chris Paul'.
```

##### Foreign keys in HTTP body, `where` and `returning_columns`

```plaintext
PUT /api/player?where=team_id.name%3D'LA Clippers'&returning_columns=id, name, team_id.name, team_id.coach_id.name
{ "name": "team_id.coach_id.name" }
          ^---------------------^ No inner quotes in the string means that the value is an expression.

Result:
[
  {
    "id": 3,
    "name": "Doc Rivers",
    "team_id.name": "LA Clippers",
    "team_id.coach_id.name": "Doc Rivers"
  },
  {
    "id": 4,
    "name": "Doc Rivers",
    "team_id.name": "LA Clippers",
    "team_id.coach_id.name": "Doc Rivers"
  }
]

Garrett Temple and Wilson Chandler have been renamed to Doc Rivers.
```

Obviously this request didn’t produce the most useful results, but it shows the possibilities of bulk updates.

## Not supported

- HStore (`rust-sqlparser` doesn't support it). Use JSON/JSONB instead.
- Bit and Varbit types (the `B'0101'` syntax in postgres is not supported by `rust-sqlparser`)
- Exclusion and Trigger constraints
- `BETWEEN` (see [Postgres wiki article](https://wiki.postgresql.org/wiki/Don%27t_Do_This#Don.27t_use_BETWEEN_.28especially_with_timestamps.29))

## To dos

1. Shortened alias syntax ("some_table a" vs "some_table AS a")
1. parallelize all iters (with Rayon + par_iter).
1. Add security, customizability, optimizations, etc.
1. GraphQL API
1. Optimization: Convert Strings to &str / statics.
1. CSV, XML for REST API (nix for now?)
1. Eventually support dot syntax in INSERT: [See this forum post](https://dba.stackexchange.com/questions/160674/insert-rows-in-two-tables-preserving-connection-to-a-third-table)

## To run tests

You will need `docker-compose` to run tests. In one terminal, run `docker-compose up` to start the postgres docker image.

In another terminal, run `cargo test`.
