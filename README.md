# experiment00

Toy project for exploring Rust. Really, I'm trying to see how easy it is to recreate ServiceNow's Table API in Rust using the PostgreSQL database, as I've found that feature to be useful enough to use in future web projects.

## What about PostgREST

Theoretically I could just use that, but I'm doubting its performance. It’s probably worth benchmarking, but there's a good chance that it wouldn’t be very fast.

1. There's no Haskell web frameworks that perform that well (see [TechEmpower Benchmarks](https://www.techempower.com/benchmarks/#section=data-r17&hw=cl&test=fortune&l=yyku67-1)). PostgREST uses Warp, which is the same web framework that yesod is based on (last place in that list).
1. I'm not interested in learning/working with Haskell.

## Not supported

- Bit, Unknown, and Varbit types
- Exclusion and Trigger constraints
- `BETWEEN` (see [Postgres wiki article](https://wiki.postgresql.org/wiki/Don%27t_Do_This#Don.27t_use_BETWEEN_.28especially_with_timestamps.29))

## To dos

1. Recreate the Table API.
1. Replace r2d2 with tokio-postgres (look at techempower benchmarks code)
1. Add security, customizability, optimizations, etc.
1. GraphQL API
1. Cache table stats every X minutes (default 2, make it configurable).
1. Add each endpoint as an individual export.
1. Optimization: Get rid of HashMap usage (convert to tuples or Serde_Json Maps)
1. Optimization: Convert Strings to &str / statics.
1. CSV, XML for REST API (nix for now?)
1. gRPC, Flatbuffers (nix for now?)

## Notes

- Need to be able to query for foreign key values (also need to account for when the user attempts to get the foreign key values for fields that aren't actually foreign keys)
- Dotwalking foreign keys (also see [Resource embedding](http://postgrest.org/en/v5.2/api.html#resource-embedding))
- there should probably be an option for users to add custom API endpoint/configuration for `add_rest_api_scope()`
- there should probably be an option to disable specific endpoints.
- Need to add a query parser for all endpoints

## Endpoints

### `GET /{table}`

Queries {table} with given parameters using SELECT. If no columns are provided, returns stats for {table}.

#### Query Parameters for `GET /{table}`

To be filled.

#### Examples for `GET /{table}`

##### Foreign keys

`GET /a_table?columns=a_foreign_key.some_text,another_foreign_key.some_str,b&where=a_foreign_key.some_id>0ANDanother_foreign_key.id>0`

Where

```plaintext
a_table.a_foreign_key references b_table.id
a_table.another_foreign_key references c_table.id
```

Translates into the query:

```postgre
SELECT
  a.b, b.some_text, c.some_str
FROM
  a_table a
  INNER JOIN b_table b ON a.a_foreign_key = b.id
  INNER JOIN c_table c ON a.another_foreign_key = c.id
WHERE (
  b.some_id > 0 AND
  c.id > 0
)
```

## Example 1: 1 level deep

```plaintext
/a_table
columns=a_foreign_key.some_text,another_foreign_key.some_str,b&where=a_foreign_key.some_id>0 AND another_foreign_key.id>0

a_foreign_key references b_table.id
another_foreign_key references c_table.id
```

### becomes =>

```postgre
SELECT
  a.b, b.some_text, c.some_str
FROM
  a_table a
  INNER JOIN b_table b ON a.a_foreign_key = b.id
  INNER JOIN c_table c ON a.another_foreign_key = c.id
WHERE (
  b.some_id > 0 AND
  c.id > 0
)
```

```rust
get_foreign_keys_from_query_columns(
  conn,
  "a_table",
  &[
    "a_foreign_key.some_text",
    "another_foreign_key.some_str",
    "b"
  ]
);

// should return

Ok(Some(vec![
  ForeignKeyReference {
    referring_column: "a_foreign_key".to_string(),
    table_referred: "b_table".to_string(),
    table_column_referred: "id".to_string(),
    nested_fks: None,
  },
  ForeignKeyReference {
    referring_column: "another_foreign_key".to_string(),
    table_referred: "c_table".to_string(),
    table_column_referred: "id".to_string(),
    nested_fks: None,
  }
]))
```

## Example 2: 2 levels deep

```plaintext
/a_table
columns=a_foreign_key.some_text,another_foreign_key.nested_fk.some_str,another_foreign_key.different_nested_fk.some_int,b&where=a_foreign_key.some_id>0 AND another_foreign_key.id>0

a_foreign_key references b_table.id
another_foreign_key references c_table.id
another_foreign_key.nested_fk references d_table.id
another_foreign_key.different_nested_fk references e_table.id
```

### Becomes =>

```postgre
SELECT
  a.b, b.some_text, d.some_str, e.some_int
FROM
  a_table a
  INNER JOIN b_table b ON a.a_foreign_key = b.id
  INNER JOIN c_table c ON a.another_foreign_key = c.id
  INNER JOIN d_table d ON c.nested_fk = d.id
  INNER JOIN e_table e ON c.different_nested_fk = e.id
WHERE (
  b.some_id > 0 AND
  c.id > 0
)
```

```rust
get_foreign_keys_from_query_columns(
  conn,
  "a_table",
  &[
    "a_foreign_key.some_text",
    "another_foreign_key.nested_fk.some_str",
    "another_foreign_key.different_nested_fk.some_int",
    "b"
  ]
);

// should return

Ok(Some(vec![
  ForeignKeyReference {
    referring_column: "a_foreign_key".to_string(),
    table_referred: "b_table".to_string(),
    table_column_referred: "id".to_string(),
    nested_fks: None
  },
  ForeignKeyReference {
    referring_column: "another_foreign_key".to_string(),
    table_referred: "b_table".to_string(),
    table_column_referred: "id".to_string(),
    nested_fks: Some(vec![
      ForeignKeyReference {
        referring_column: "nested_fk".to_string(),
        table_referred: "d_table".to_string(),
        table_column_referred: "id".to_string(),
        nested_fks: None
      },
      ForeignKeyReference {
        referring_column: "different_nested_fk".to_string(),
        table_referred: "e_table".to_string(),
        table_column_referred: "id".to_string(),
        nested_fks: None
      }
    ])
  }
]))
```
