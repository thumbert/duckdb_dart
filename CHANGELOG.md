## TODO
- Performance benchmarks?  
- Support more data types
- Implement appender
- Tests break with ffigen 14.0.0.  Has a different enum treatment. 


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
