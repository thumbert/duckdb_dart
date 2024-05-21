A Dart client API to [DuckDb](https://duckdb.org) 

## Features
Sending and executing statements works.  Fetching the data back works for several data types.  
One major exception is `enums` (I haven't figured out how to do it.) 

Everything is sync 😃.  

## Getting started
Install the package

## Usage


```dart
final con = Connection.inMemory();
con.execute('');
var result = con.fetch('SELECT * FROM tbl;');

con.close();
```

## Additional information



## Info for authors

Download the C/C++ DuckDb bindings and extract the zip file.  
 * Put the `duckdb.h` header file in the `./third_party` folder
 * Copy the libduckdb.so in the `/usr/local/lib` folder
 * Run `dart run ffigen` to generate the bindings.  Bindings get generated in 
   in file `./src/ffi/duckdb_generated_bindings.dart`


