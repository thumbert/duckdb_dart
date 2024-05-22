library implementation.duckdb;

import 'dart:ffi';

import 'package:duckdb_dart/duckdb_dart.dart';
import 'package:ffi/ffi.dart';

enum AccessMode {
  readOnly,
  readWrite,
}

enum DefaultOrder {
  ascending,
  descending,
}

/// Configuration options to change the different settings of the database.
/// Many of these settings can be changed later using `PRAGMA` statements
/// as well.
class Config {
  Config({this.accessMode, this.threads, this.maxMemoryGb, this.defaultOrder});

  final AccessMode? accessMode;
  final int? threads;
  final int? maxMemoryGb;
  final DefaultOrder? defaultOrder;
}

class Connection {
  Connection(this.path, [this.config]) {
    init();
    final ptrPath = path.toNativeUtf8().cast<Char>();

    if (config != null) {
      setConfig();
      if (bindings.duckdb_open_ext(ptrPath, ptrDb, ptrConfig.value, nullptr) ==
          duckdb_state.DuckDBError) {
        throw StateError('Error opening the Db');
      }
    } else {
      if (bindings.duckdb_open(ptrPath, ptrDb) == duckdb_state.DuckDBError) {
        throw StateError('Error opening the Db');
      }
    }

    if (bindings.duckdb_connect(ptrDb.value, ptrCon) ==
        duckdb_state.DuckDBError) {
      throw StateError('Error connecting to the Db');
    }
  }

  Connection.inMemory([this.config]) {
    init();
    if (config != null) {
      setConfig();
      if (bindings.duckdb_open_ext(nullptr, ptrDb, ptrConfig.value, nullptr) ==
          duckdb_state.DuckDBError) {
        throw StateError('Error opening the Db');
      }
    } else {
      if (bindings.duckdb_open(nullptr, ptrDb) == duckdb_state.DuckDBError) {
        throw StateError('Error opening the Db');
      }
    }

    if (bindings.duckdb_connect(ptrDb.value, ptrCon) ==
        duckdb_state.DuckDBError) {
      throw StateError('Error connecting to the Db');
    }
  }

  late final String path;
  late final Pointer<duckdb_database> ptrDb;
  late final Pointer<duckdb_connection> ptrCon;
  late final Config? config;
  late final Pointer<duckdb_config> ptrConfig;

  void init() {
    ptrDb = calloc<duckdb_database>();
    ptrCon = calloc<duckdb_connection>();
  }

  void setConfig() {
    if (bindings.duckdb_create_config(ptrConfig) == duckdb_state.DuckDBError) {
      throw StateError('Error configuring the Db');
    }
    if (config!.accessMode != null) {
      bindings.duckdb_set_config(
          ptrConfig.value,
          'access_mode'.toNativeUtf8().cast<Char>(),
          switch (config!.accessMode!) {
            AccessMode.readOnly => 'READ_ONLY'.toNativeUtf8().cast<Char>(),
            AccessMode.readWrite => 'READ_WRITE'.toNativeUtf8().cast<Char>(),
          });
    }
    if (config!.threads != null) {
      bindings.duckdb_set_config(
        ptrConfig.value,
        'threads'.toNativeUtf8().cast<Char>(),
        '${config!.threads!}'.toNativeUtf8().cast<Char>()
      );
    }
    if (config!.maxMemoryGb != null) {
      bindings.duckdb_set_config(
        ptrConfig.value,
        'max_memory'.toNativeUtf8().cast<Char>(),
        '${config!.maxMemoryGb!}GB'.toNativeUtf8().cast<Char>()
      );
    }
    if (config!.defaultOrder != null) {
      bindings.duckdb_set_config(
          ptrConfig.value,
          'default_order'.toNativeUtf8().cast<Char>(),
          switch (config!.defaultOrder!) {
            DefaultOrder.ascending => 'ASC'.toNativeUtf8().cast<Char>(),
            DefaultOrder.descending => 'DESC'.toNativeUtf8().cast<Char>(),
          });
    }
  }

  /// A statement that doesn't return data back from the database.
  /// For example `DELETE FROM TABLE boo;`
  /// The return value is
  void execute(String statement) {
    var query = statement.toNativeUtf8().cast<Char>();
    var ptrResult = calloc<duckdb_result>();
    if (bindings.duckdb_query(ptrCon.value, query, ptrResult) ==
        duckdb_state.DuckDBError) {
      throw StateError(
          bindings.duckdb_result_error(ptrResult).cast<Utf8>().toDartString());
    }

    // return bindings.duckdb_query(
    //     ptrCon.value, query, nullptr.cast<duckdb_result>());
  }

  /// A query that returns some data back.
  ///
  /// TODO!  Will have to change the implementation to use chunks instead of
  /// column data.  However, at this moment, I've found the documentation of
  /// data chunks to be lacking.  https://duckdb.org/docs/api/c/data_chunk
  ///
  List<Map<String, Object?>> fetch(String query) {
    var q = query.toNativeUtf8().cast<Char>();
    var ptrResult = calloc<duckdb_result>();
    if (bindings.duckdb_query(ptrCon.value, q, ptrResult) ==
        duckdb_state.DuckDBError) {
      throw StateError(
          bindings.duckdb_result_error(ptrResult).cast<Utf8>().toDartString());
      // StateError(
      //     'Failed to $query, DuckDb error code ${duckdb_state.DuckDBError}');
    }
    var rowCount = bindings.duckdb_row_count(ptrResult);
    // print('Query returned $rowCount rows');
    var out = <Map<String, dynamic>>[];
    var colCount = bindings.duckdb_column_count(ptrResult);
    var columnNames = <String>[];
    var columnType = <int>[];
    var columnData = <Pointer>[];
    var nullMaskData = <Pointer<Bool>>[];

    for (var j = 0; j < colCount; j++) {
      var name =
          bindings.duckdb_column_name(ptrResult, j).cast<Utf8>().toDartString();
      columnNames.add(name);
      columnType.add(bindings.duckdb_column_type(ptrResult, j));
      switch (columnType[j]) {
        case 4:
          columnData
              .add(bindings.duckdb_column_data(ptrResult, j).cast<Int32>());
        case 17:
          columnData
              .add(bindings.duckdb_column_data(ptrResult, j).cast<Char>());
        default:
          columnData.add(bindings.duckdb_column_data(ptrResult, j));
      }
      nullMaskData.add(bindings.duckdb_nullmask_data(ptrResult, j));
    }
    // print('Columns: $columnNames');
    // print(
    //     'Result return type: ${bindings.duckdb_result_return_type(ptrResult.ref)}');

    // process the rows
    for (var i = 0; i < rowCount; i++) {
      var values = <dynamic>[];
      for (var j = 0; j < colCount; j++) {
        if (nullMaskData[j][i]) {
          values.add(null);
        } else {
          switch (columnType[j]) {
            case 1:
              values.add((columnData[j] as Pointer<Bool>)[i]);
            case 2:
              values.add((columnData[j] as Pointer<Int8>)[i]); // TINYINT
            case 3:
              values.add((columnData[j] as Pointer<Int16>)[i]); // SMALLINT
            case 4:
              values.add((columnData[j] as Pointer<Int32>)[i]); // INTEGER
            case 5:
              values.add((columnData[j] as Pointer<Int64>)[i]); // BIGINT
            case 6:
              values.add((columnData[j] as Pointer<Uint8>)[i]); // UTINYINT
            case 7:
              values.add((columnData[j] as Pointer<Uint16>)[i]); // USMALLINT
            case 8:
              values.add((columnData[j] as Pointer<Uint32>)[i]); // UINTEGER
            case 9:
              values.add((columnData[j] as Pointer<Uint64>)[i]); // UBIGINT
            case 10:
              values.add((columnData[j] as Pointer<Float>)[i]); // 4 bytes
            case 11:
              values.add((columnData[j] as Pointer<Double>)[i]); // 8 bytes
            case 12:
              var ts = bindings.duckdb_value_timestamp(ptrResult, j, i);
              values.add(
                  DateTime.fromMicrosecondsSinceEpoch(ts.micros, isUtc: true));
            case 13: // number of days since 1970-01-01
              var dt = bindings.duckdb_value_date(ptrResult, j, i);
              values.add(dt.days);
            case 17:
              var a = bindings
                  .duckdb_value_varchar(ptrResult, j, i)
                  .cast<Utf8>()
                  .toDartString();
              values.add(a);
            case 20:
              var ts = bindings.duckdb_value_timestamp(ptrResult, j, i);
              values.add(ts);
            case 23:
            // columnData[j]

            // bindings.duckdb_enum_internal_type(columnData[j]);

            // var value = bindings
            //     .duckdb_value_lo(ptrResult, j, i)
            //     .cast<Utf8>()
            //     .toDartString();
            // values.add(value);
            default:
              throw StateError('Unsupported type: ${columnType[j]}');
          }
        }
      }
      // print('values: $values');
      out.add(Map.fromIterables(columnNames, values));
    }
    bindings.duckdb_destroy_result(ptrResult);
    return out;
  }

  // close the db and the connection to avoid leaking resources
  void close() {
    bindings.duckdb_disconnect(ptrCon);
    if (config != null) {
      bindings.duckdb_destroy_config(ptrConfig);
    }
    bindings.duckdb_close(ptrDb);
  }
}

// A start of using chunks ...
// var chunkCount = bindings.duckdb_result_chunk_count(ptrResult.ref);
// print('Query result has $chunkCount chunks');
// for (var chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++) {
//   var data = bindings.duckdb_result_get_chunk(ptrResult.ref, chunkIndex);
//   bindings.duckdb_destroy_data_chunk(Pointer.fromAddress(data.address));
// }
