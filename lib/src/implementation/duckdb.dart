library implementation.duckdb;

import 'dart:ffi';

import 'package:duckdb_dart/duckdb_dart.dart';
import 'package:ffi/ffi.dart';

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
    ptrConfig = calloc<duckdb_config>();
    if (bindings.duckdb_create_config(ptrConfig) == duckdb_state.DuckDBError) {
      throw StateError('Error configuring the Db');
    }
    if (config!.accessMode != null) {
      bindings.duckdb_set_config(
          ptrConfig.value,
          'access_mode'.toNativeUtf8().cast<Char>(),
          config!.accessMode!.toString().toNativeUtf8().cast<Char>());
    }

    if (config!.allowUnsignedExtensions != null) {
      bindings.duckdb_set_config(
          ptrConfig.value,
          'allow_unsigned_extensions'.toNativeUtf8().cast<Char>(),
          config!.allowUnsignedExtensions!
              .toString()
              .toNativeUtf8()
              .cast<Char>());
    }

    if (config!.defaultNullOrder != null) {
      bindings.duckdb_set_config(
          ptrConfig.value,
          'default_null_order'.toNativeUtf8().cast<Char>(),
          config!.defaultNullOrder!.toString().toNativeUtf8().cast<Char>());
    }

    if (config!.defaultOrder != null) {
      bindings.duckdb_set_config(
          ptrConfig.value,
          'default_order'.toNativeUtf8().cast<Char>(),
          config!.defaultOrder!.toString().toNativeUtf8().cast<Char>());
    }

    if (config!.autoInstallKnownExtensions != null) {
      bindings.duckdb_set_config(
          ptrConfig.value,
          'autoinstall_known_extensions'.toNativeUtf8().cast<Char>(),
          config!.autoInstallKnownExtensions!
              .toString()
              .toNativeUtf8()
              .cast<Char>());
    }

    if (config!.autoLoadKnownExtensions != null) {
      bindings.duckdb_set_config(
          ptrConfig.value,
          'autoload_known_extensions'.toNativeUtf8().cast<Char>(),
          config!.autoLoadKnownExtensions!
              .toString()
              .toNativeUtf8()
              .cast<Char>());
    }

    if (config!.maxMemoryGb != null) {
      bindings.duckdb_set_config(
          ptrConfig.value,
          'max_memory'.toNativeUtf8().cast<Char>(),
          '${config!.maxMemoryGb!}GB'.toNativeUtf8().cast<Char>());
    }

    if (config!.threads != null) {
      bindings.duckdb_set_config(
          ptrConfig.value,
          'threads'.toNativeUtf8().cast<Char>(),
          '${config!.threads!}'.toNativeUtf8().cast<Char>());
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
    bindings.duckdb_destroy_result(ptrResult);
  }

  ///
  // List<Map<String, Object?>> fetch(String query) {
  //   final aux = fetchRaw(query);
  //   final names = aux.keys;
  //   final rowCount = aux[names.first]!.length;

  //   var out = <Map<String, Object?>>[];
  //   for (var i = 0; i < rowCount; i++) {
  //     out.add(Map.fromIterables(names, names.map((name) => aux[name]![i])));
  //   }
  //   return out;
  // }

  /// A query that returns some data back (using the data chunks API).
  ///
  /// Note: Can't use duckdb_row_count and duckdb_value_* functions when using
  /// data chunks.  Data chunks is more performant and allows access the
  /// complex data structures, e.g. enums, list, struct, array.
  ///
  Map<String, List<Object?>> fetch(String query) {
    var q = query.toNativeUtf8().cast<Char>();
    var resultPtr = calloc<duckdb_result>();
    if (bindings.duckdb_query(ptrCon.value, q, resultPtr) ==
        duckdb_state.DuckDBError) {
      throw StateError(
          bindings.duckdb_result_error(resultPtr).cast<Utf8>().toDartString());
    }
    var out = <String, List<Object?>>{};

    var chunkCount = bindings.duckdb_result_chunk_count(resultPtr.ref);
    // print('chunkCount=$chunkCount');
    for (var chunk = 0; chunk < chunkCount; chunk++) {
      var chunkPtr = bindings.duckdb_result_get_chunk(resultPtr.ref, chunk);
      var rowCount = bindings.duckdb_data_chunk_get_size(chunkPtr);
      // print('rowCount=$rowCount');
      final ids = List.generate(rowCount, (i) => i);

      var colCount = bindings.duckdb_data_chunk_get_column_count(chunkPtr);
      // print('colCount=$colCount');
      for (var j = 0; j < colCount; j++) {
        var vector = bindings.duckdb_data_chunk_get_vector(chunkPtr, j);
        var logicalType = bindings.duckdb_vector_get_column_type(vector);
        var typeId = bindings.duckdb_get_type_id(logicalType);
        // print('typeId=$typeId');
        var values = bindings.duckdb_vector_get_data(vector);
        var validity = bindings.duckdb_vector_get_validity(vector);
        var name = bindings
            .duckdb_column_name(resultPtr, j)
            .cast<Utf8>()
            .toDartString();
        // print('columnName=$name');

        switch (typeId) {
          case 1:
            var xs = values.cast<Bool>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              return isNull ? null : xs[i];
            }).toList();

          case 2: // TINYINT
            var xs = values.cast<Int8>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              return isNull ? null : xs[i];
            }).toList();

          case 3: // SMALLINT
            var xs = values.cast<Int16>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              return isNull ? null : xs[i];
            }).toList();

          case 4: // INTEGER
            var xs = values.cast<Int32>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              return isNull ? null : xs[i];
            }).toList();

          case 5: // BIGINT
            var xs = values.cast<Int64>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              return isNull ? null : xs[i];
            }).toList();

          case 6: // UTINYINT
            var xs = values.cast<Uint8>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              return isNull ? null : xs[i];
            }).toList();

          case 7: // USMALLINT
            var xs = values.cast<Uint16>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              return isNull ? null : xs[i];
            }).toList();

          case 8: // UINTEGER
            var xs = values.cast<Uint32>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              return isNull ? null : xs[i];
            }).toList();

          case 9: // UBIGINT
            var xs = values.cast<Uint64>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              return isNull ? null : xs[i];
            }).toList();

          case 10: // 4 bytes
            var xs = values.cast<Float>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              return isNull ? null : xs[i];
            }).toList();

          case 11: // 8 bytes
            var xs = values.cast<Double>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              return isNull ? null : xs[i];
            }).toList();

          case 12: // UTC DateTime
            var xs = values.cast<Int64>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              return isNull
                  ? null
                  : DateTime.fromMicrosecondsSinceEpoch(xs[i], isUtc: true);
            }).toList();

          case 13: // number of days since 1970-01-01
            var xs = values.cast<Int32>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              return isNull ? null : xs[i];
            }).toList();

          case 17: // VARCHAR
            // see test 'Test DataChunk varchar result fetch in C API'
            // https://github.com/duckdb/duckdb/blob/main/test/api/capi/test_capi_data_chunk.cpp#L260
            var xs = values.cast<duckdb_string_t>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              if (isNull) return null;
              final tuple = xs[i];
              if (bindings.duckdb_string_is_inlined(tuple)) {
                // The data is small enough to fit in the string_t, it does not have a separate allocation
                var array = tuple.value.inlined.inlined;
                final charCodes = <int>[];
                var i = 0;
                while (array[i] != 0) {
                  charCodes.add(array[i]);
                  i++;
                }
                return String.fromCharCodes(charCodes);
              } else {
                return tuple.value.pointer.ptr.cast<Utf8>().toDartString();
              }
            }).toList();

          // case 19: // decimal
          // /// https://github.com/Giorgi/DuckDB.NET/blob/8520bf5005d9309f762ef61d71412d60d24ca32c/DuckDB.NET.Data/Internal/Reader/DecimalVectorDataReader.cs#L43

          case 23:
            // there are several internal types for ENUMs based on the size
            // of the dictionary (uint8_t, uint16_t, uint32_t)
            // See https://github.com/duckdb/duckdb/blob/main/test/api/capi/test_capi_complex_types.cpp#L52
            var enumInternalType =
                bindings.duckdb_enum_internal_type(logicalType);
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              if (isNull) return null;
              // get the index of the enum dictionary for this row
              late int idx;
              if (enumInternalType == 6) {
                idx = (values as Pointer<Uint8>)[i];
              } else if (enumInternalType == 7) {
                idx = (values as Pointer<Uint16>)[i];
              } else if (enumInternalType == 8) {
                idx = (values as Pointer<Uint32>)[i];
              }
              return bindings
                  .duckdb_enum_dictionary_value(logicalType, idx)
                  .cast<Utf8>()
                  .toDartString();
            }).toList();

          default:
            throw StateError('TypeId $typeId has not been mapped yet');

          // bindings.duckdb_free(valuePtr);
        }

        // cleaning
        var ptr = calloc<duckdb_logical_type>();
        ptr.value = Pointer.fromAddress(logicalType.address);
        bindings.duckdb_destroy_logical_type(ptr);
      }

      // cleaning
      var ptr = calloc<duckdb_data_chunk>();
      ptr.value = Pointer.fromAddress(chunkPtr.address);
      bindings.duckdb_destroy_data_chunk(ptr);
    }

    bindings.duckdb_destroy_result(resultPtr);
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

/// This implementation uses a simple processing by row.
/// It can't deal with ENUMs or other complex types (need to use
/// the chunk api).
// List<Map<String, Object?>> _fetch(String query) {
//   var q = query.toNativeUtf8().cast<Char>();
//   var resultPtr = calloc<duckdb_result>();
//   if (bindings.duckdb_query(ptrCon.value, q, resultPtr) ==
//       duckdb_state.DuckDBError) {
//     throw StateError(
//         bindings.duckdb_result_error(resultPtr).cast<Utf8>().toDartString());
//   }
//   var rowCount = bindings.duckdb_row_count(resultPtr);
//   // print('Query returned $rowCount rows');
//   var out = <Map<String, dynamic>>[];
//   var colCount = bindings.duckdb_column_count(resultPtr);
//   var columnNames = <String>[];
//   var columnType = <int>[];
//   var columnData = <Pointer>[];
//   var nullMaskData = <Pointer<Bool>>[];

//   for (var j = 0; j < colCount; j++) {
//     var name =
//         bindings.duckdb_column_name(resultPtr, j).cast<Utf8>().toDartString();
//     // print('column j=$j has name: $name');
//     columnNames.add(name);
//     columnType.add(bindings.duckdb_column_type(resultPtr, j));
//     switch (columnType[j]) {
//       case 4:
//         columnData
//             .add(bindings.duckdb_column_data(resultPtr, j).cast<Int32>());
//       case 17:
//         columnData
//             .add(bindings.duckdb_column_data(resultPtr, j).cast<Char>());
//       default:
//         columnData.add(bindings.duckdb_column_data(resultPtr, j));
//     }
//     nullMaskData.add(bindings.duckdb_nullmask_data(resultPtr, j));
//   }
//   // print('Columns: $columnNames');
//   // print(
//   //     'Result return type: ${bindings.duckdb_result_return_type(ptrResult.ref)}');
//   // print('no error=${bindings.duckdb_result_error(resultPtr) == nullptr}');

//   // process the rows
//   for (var i = 0; i < rowCount; i++) {
//     var values = <dynamic>[];
//     for (var j = 0; j < colCount; j++) {
//       if (nullMaskData[j][i]) {
//         values.add(null);
//       } else {
//         switch (columnType[j]) {
//           case 1:
//             values.add((columnData[j] as Pointer<Bool>)[i]);
//           case 2:
//             values.add((columnData[j] as Pointer<Int8>)[i]); // TINYINT
//           case 3:
//             values.add((columnData[j] as Pointer<Int16>)[i]); // SMALLINT
//           case 4:
//             values.add((columnData[j] as Pointer<Int32>)[i]); // INTEGER
//           case 5:
//             values.add((columnData[j] as Pointer<Int64>)[i]); // BIGINT
//           case 6:
//             values.add((columnData[j] as Pointer<Uint8>)[i]); // UTINYINT
//           case 7:
//             values.add((columnData[j] as Pointer<Uint16>)[i]); // USMALLINT
//           case 8:
//             values.add((columnData[j] as Pointer<Uint32>)[i]); // UINTEGER
//           case 9:
//             values.add((columnData[j] as Pointer<Uint64>)[i]); // UBIGINT
//           case 10:
//             values.add((columnData[j] as Pointer<Float>)[i]); // 4 bytes
//           case 11:
//             values.add((columnData[j] as Pointer<Double>)[i]); // 8 bytes
//           case 12:
//             var ts = bindings.duckdb_value_timestamp(resultPtr, j, i);
//             values.add(
//                 DateTime.fromMicrosecondsSinceEpoch(ts.micros, isUtc: true));
//           case 13: // number of days since 1970-01-01
//             var dt = bindings.duckdb_value_date(resultPtr, j, i);
//             values.add(dt.days);
//           case 17:
//             var a = bindings
//                 .duckdb_value_varchar(resultPtr, j, i)
//                 .cast<Utf8>()
//                 .toDartString();
//             values.add(a);
//           case 20:
//             var ts = bindings.duckdb_value_timestamp(resultPtr, j, i);
//             values.add(ts);
//           case 23:

//             /// ENUMs are a complex type, the API above doesn't work!  See
//             /// https://github.com/duckdb/duckdb/blob/main/test/api/capi/test_capi_complex_types.cpp#L52
//             values.add(null);
//           default:
//             throw StateError('Unsupported type: ${columnType[j]}');
//         }
//       }
//     }
//     // print('values: $values');
//     out.add(Map.fromIterables(columnNames, values));
//   }
//   bindings.duckdb_destroy_result(resultPtr);
//   return out;
// }
