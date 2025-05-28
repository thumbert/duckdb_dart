## TODO
- Performance benchmarks?  
- Support more data types
- Implement appender

## 2025-05-28 (0.3.9)
- Bump DuckDB to 1.3.0

## 2025-02-19 (0.3.8)
- Bump DuckDB to 1.2.0

## 2025-01-11 (0.3.7)
- Bump ffi version to 16.0.0

## 2024-10-04 (0.3.6)
- Map TIMESTAMPTZ data type to Dart.  Return the number of microseconds since epoch. 
  Would be nice to have a database that natively supports timezones, eh?


## 2024-09-15 (0.3.4)
- Introduce `fetchRows` to map the result of a DuckDB query to a Dart class.


## 2024-09-15 (0.3.3)
- Fix bug in `fetch` related to data chunks.  Only the last chunk was actually returned!


## 2024-09-15 (0.3.2)
- Upgrade to ffigen 14.0.0


## 2024-09-15 (0.3.1)
- Support decimal types


## 2024-09-10 (0.3.0)
- Update to DuckDB 1.1.0


## 2024-06-30 (0.2.3)
- Support TIMESTAMP_S, TIMESTAMP_MS, TIMESTAMP_NS datatypes


## 2024-06-14 (0.2.2)
- Make sure passing a database config works


## 2024-06-08 (0.2.1)
- Improve docs


## 2024-06-08 (0.2.0)
- Separate the configuration in own file
- Move from the duckdb_value_* functions to the chunk API.  Now, ENUMs are supported


## 2024-05-21 (0.1.1) 
- Support more DuckDb numeric types 
- Add a database `Configuration` argument to `Connection`


## 2024-05-20 (0.1.0)
- Initial version.  Basic things working.
