library implementation.duckdb;

import 'dart:ffi';

import 'package:decimal/decimal.dart';
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
          case DUCKDB_TYPE.DUCKDB_TYPE_BOOLEAN:
            var xs = values.cast<Bool>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              return isNull ? null : xs[i];
            }).toList();

          case DUCKDB_TYPE.DUCKDB_TYPE_TINYINT:
            var xs = values.cast<Int8>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              return isNull ? null : xs[i];
            }).toList();

          case DUCKDB_TYPE.DUCKDB_TYPE_SMALLINT:
            var xs = values.cast<Int16>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              return isNull ? null : xs[i];
            }).toList();

          case DUCKDB_TYPE.DUCKDB_TYPE_INTEGER:
            var xs = values.cast<Int32>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              return isNull ? null : xs[i];
            }).toList();

          case DUCKDB_TYPE.DUCKDB_TYPE_BIGINT:
            var xs = values.cast<Int64>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              return isNull ? null : xs[i];
            }).toList();

          case DUCKDB_TYPE.DUCKDB_TYPE_UTINYINT:
            var xs = values.cast<Uint8>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              return isNull ? null : xs[i];
            }).toList();

          case DUCKDB_TYPE.DUCKDB_TYPE_USMALLINT:
            var xs = values.cast<Uint16>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              return isNull ? null : xs[i];
            }).toList();

          case DUCKDB_TYPE.DUCKDB_TYPE_UINTEGER:
            var xs = values.cast<Uint32>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              return isNull ? null : xs[i];
            }).toList();

          case DUCKDB_TYPE.DUCKDB_TYPE_UBIGINT:
            var xs = values.cast<Uint64>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              return isNull ? null : xs[i];
            }).toList();

          case DUCKDB_TYPE.DUCKDB_TYPE_FLOAT: // 4 bytes
            var xs = values.cast<Float>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              return isNull ? null : xs[i];
            }).toList();

          case DUCKDB_TYPE.DUCKDB_TYPE_DOUBLE: // 8 bytes
            var xs = values.cast<Double>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              return isNull ? null : xs[i];
            }).toList();

          case DUCKDB_TYPE
                .DUCKDB_TYPE_TIMESTAMP: // UTC DateTime, microsecond precision
            var xs = values.cast<Int64>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              return isNull
                  ? null
                  : DateTime.fromMicrosecondsSinceEpoch(xs[i], isUtc: true);
            }).toList();

          case DUCKDB_TYPE.DUCKDB_TYPE_DATE: // number of days since 1970-01-01
            var xs = values.cast<Int32>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              return isNull ? null : xs[i];
            }).toList();

          case DUCKDB_TYPE.DUCKDB_TYPE_VARCHAR: // VARCHAR
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

          case DUCKDB_TYPE.DUCKDB_TYPE_DECIMAL: // decimal
            /// https://github.com/Giorgi/DuckDB.NET/blob/8520bf5005d9309f762ef61d71412d60d24ca32c/DuckDB.NET.Data/Internal/Reader/DecimalVectorDataReader.cs#L43
            var type = bindings.duckdb_decimal_internal_type(logicalType);
            var scale = bindings.duckdb_decimal_scale(logicalType);
            switch (type) {
              case DUCKDB_TYPE.DUCKDB_TYPE_SMALLINT ||
                    DUCKDB_TYPE.DUCKDB_TYPE_INTEGER:
                var xs = values.cast<Int32>();
                out[name] = ids.map((i) {
                  final isNull =
                      !bindings.duckdb_validity_row_is_valid(validity, i);
                  if (isNull) return null;
                  var res = (Decimal.fromInt(xs[i]) /
                          Decimal.ten.pow(scale).toDecimal())
                      .toDecimal();
                  // print(res);
                  return res;
                }).toList();
              case DUCKDB_TYPE.DUCKDB_TYPE_BIGINT:
                var xs = values.cast<Int64>();
                out[name] = ids.map((i) {
                  final isNull =
                      !bindings.duckdb_validity_row_is_valid(validity, i);
                  if (isNull) return null;
                  var res = (Decimal.fromInt(xs[i]) /
                          Decimal.ten.pow(scale).toDecimal())
                      .toDecimal();
                  return res;
                }).toList();
              case _:
                throw StateError('Unsupported decimal type $type');
            }

          case DUCKDB_TYPE.DUCKDB_TYPE_TIMESTAMP_S: // UTC DateTime, second precision
            var xs = values.cast<Int64>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              return isNull
                  ? null
                  : DateTime.fromMillisecondsSinceEpoch(xs[i] * 1000,
                      isUtc: true);
            }).toList();

          case DUCKDB_TYPE.DUCKDB_TYPE_TIMESTAMP_MS: // UTC DateTime, millisecond precision
            var xs = values.cast<Int64>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              return isNull
                  ? null
                  : DateTime.fromMillisecondsSinceEpoch(xs[i], isUtc: true);
            }).toList();

          case DUCKDB_TYPE.DUCKDB_TYPE_TIMESTAMP_NS: // UTC DateTime, nanosecond precision
            var xs = values.cast<Int64>();
            out[name] = ids.map((i) {
              final isNull =
                  !bindings.duckdb_validity_row_is_valid(validity, i);
              return isNull
                  ? null
                  : DateTime.fromMicrosecondsSinceEpoch(xs[i] ~/ 1000,
                      isUtc: true);
            }).toList();

          case DUCKDB_TYPE.DUCKDB_TYPE_ENUM:
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
              if (enumInternalType == DUCKDB_TYPE.DUCKDB_TYPE_UTINYINT) {
                idx = (values as Pointer<Uint8>)[i];
              } else if (enumInternalType == DUCKDB_TYPE.DUCKDB_TYPE_USMALLINT) {
                idx = (values as Pointer<Uint16>)[i];
              } else if (enumInternalType == DUCKDB_TYPE.DUCKDB_TYPE_UINTEGER) {
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
