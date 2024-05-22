import 'dart:ffi';
import 'dart:io';
import 'duckdb.g.dart';

Bindings? _duckdb;

DynamicLibrary? _dynLib;

Bindings get bindings {
  return _duckdb ??= Bindings(open());
}

DynamicLibrary open() {
  if (_duckdb == null) {
    if (Platform.isLinux) {
      _dynLib = DynamicLibrary.open('/usr/local/lib/libduckdb.so');
    } else if (Platform.isWindows) {
      // make sure it's in your path
      _dynLib = DynamicLibrary.open('duckdb.dll');
    }      else {
      throw StateError('Your platform is not supported yet');
    }
  }
  return _dynLib!;
}
