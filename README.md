A Dart client API to [DuckDb](https://duckdb.org) 

## Features
Sending and executing statements works.  Fetching the data back into Dart works for *most important* data types.  

All database operations are sync 😃.  

There are several features that are not yet implemented.  For example: 

 * Inserting data into DuckDb is usually done with an [appender](https://duckdb.org/docs/api/c/appender).  Right now, the recommended way to insert bulk data is to use the native DuckDb functionality `read_csv`, `read_parquet`, `read_json_auto`, see [documentation](https://duckdb.org/docs/data/overview).  

 * Complex data types (list, struct, array)  

Because DuckDb also has a [WASM](https://duckdb.org/docs/api/wasm/overview) implementation, it should be possible to run/embed it into Flutter and on the Web.  I have not explored that direction. 

## Disclaimer
As I am not an expert in databases, C, or even Dart, there should be significant room for improvement in the performance and ergonomics of this package.  PR's are welcome.  I *may* not be able to engage in the long term maintenance of this package, and it's very likely that I won't provide the level of support the community needs.  If the development of this package is not happening fast enough for you, consider becoming a contributor so you can take this project further and faster.  I created this package because DuckDB was worth exploring and there are not a lot of DB offerings for Dart on the backend.   

A huge thanks to the Dart FFI package designers.  The FFI gen just works!  It's amazing.  

## Getting started
I only have access to an Ubuntu 22.04 and a Windows 10 machine for testing.  The package has been tested with the `1.1.0` DuckDb version. 

To use the package, you need to install the `Command line` and  the `C/C++` bindings on your machine, see [installation](https://duckdb.org/docs/installation/index?version=stable).  
It is painless process.  For Linux systems, just copy the `libduckdb.so` in 
the `/usr/local/lib` folder.  For Window, make sure the `dll` is on your path.   

## Usage

```dart
final con = Connection.inMemory();
con.execute('CREATE TABLE tbl (state VARCHAR, population INTEGER);');
con.execute("INSERT INTO tbl VALUES ('CA', 39539223), ('VA', 8631393);");
print(con.fetch('SELECT * FROM tbl;'));
con.close();  // close the connection to release resources
```

You can also load an existing database from disk
```dart
final con = Connection('data.duckdb');
final res = con.fetch('SHOW TABLES;');
con.close();
```

Or read in a csv file directly
```dart
final con = Connection.inMemory();
con.execute("CREATE TABLE ontime AS SELECT * FROM 'flights.csv'");
print(con.fetch('SELECT * FROM ontime LIMIT 5;'));
```

See the `test/duckdb_test.dart` for more examples.

## Map query result to a Dart class

For convenience, you can map a row of the resulting query directly to a Dart class using `fetchRows`.  For example, 
given the table
```dart
con.execute('CREATE TABLE people (name VARCHAR, age INTEGER);');
con.execute("INSERT INTO people VALUES ('Tom', 31), ('Jenny', 29), ('Maria', 33);");
```
and the class
```dart
final class Person {
  Person({required this.name, required this.age});
  String name;
  int age;
}
```
you can map the rows of the table to a `Person` using `fetchRows` 
```dart
final result = con.fetchRows('SELECT name, age FROM people ORDER BY name;',
    (List row) => Person(name: row[0], age: row[1]));
assert(result.length == 3);
assert(result.first.name == 'Jenny');
assert(result.first.age == 29);
```


## Additional information

The documentation on the DuckDb web site is comprehensive.  For more info on Db internals, see the [presentation](https://15721.courses.cs.cmu.edu/spring2023/slides/22-duckdb.pdf). 


## Info for authors

The Dart client is based on the DuckDb C API.  The DuckDb tests for the C API are located at
`https://github.com/duckdb/duckdb/blob/main/test/api/capi`


Download the C/C++ DuckDb bindings and extract the zip file.  
 * Put the `duckdb.h` header file in the `./third_party` folder
 * Copy the `libduckdb.so` in the `/usr/local/lib` folder
 * Run `dart run ffigen --config ffigen.yaml` to generate the bindings.  
   Bindings get generated in file `./src/ffi/duckdb.g.dart`


