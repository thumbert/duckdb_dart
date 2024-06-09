A Dart client API to [DuckDb](https://duckdb.org) 

## Features
Sending and executing statements works.  Fetching the data back into Dart works for the *most important* data types.  

Everything is sync 😃.  

There are features that are not yet implemented.  For example, inserting data into DuckDb is usually done with an [appender](https://duckdb.org/docs/api/c/appender).  Right now, the recommended way to insert bulk data is to use the native DuckDb functionality `read_csv`, `read_parquet`, `read_json_auto`, see [documentation](https://duckdb.org/docs/data/overview).  

Because DuckDb also has a [WASM](https://duckdb.org/docs/api/wasm/overview) implementation, it should be possible to run/embed it into Flutter and on the Web.  I have not explored that direction. 

## Disclaimer
As I am not an expert in databases, C, or even Dart, there should be significant room for improvement in the performance and ergonomics of this package.  PR's are welcome.  I *may* not be able to engage in the long term maintenance of this package and it's very likely that I won't provide the level of support the community needs.  If the development of this package is not happening fast enough for you, please consider becoming a contributor so you can take this project further and faster.  I created this package because DuckDb was worth exploring and there are not a lot of DB offerings for Dart backend.   

A huge thanks to the Dart FFI package designers.  The FFI gen just works!  It's amazing.  

## Getting started
I only have access to an Ubuntu 22.04 and a Windows 10 machine for testing.  The package has been tested with the `1.0.0` DuckDb version. 

To use the package, you need to install the `Command line` and  the `C/C++` DuckDb bindings on your machine, see [installation](https://duckdb.org/docs/installation/index?version=stable).  

This is a pretty painless process.  For Linux systems, put the `libduckdb.so` in 
the `/usr/local/lib` folder.  For Window, make sure the `dll` is on your path.   

## Usage

```dart
final con = Connection.inMemory();
con.execute('');
var result = con.fetch('SELECT * FROM tbl;');

con.close();
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
   Bindings get generated in file `./src/ffi/duckdb_generated_bindings.dart`


