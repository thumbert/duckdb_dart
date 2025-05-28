import 'package:decimal/decimal.dart';
import 'package:duckdb_dart/duckdb_dart.dart';
import 'package:ffi/ffi.dart';
import 'package:test/test.dart';

void tests() {
  group('DuckDb tests', () {
    late Connection con;
    setUp(() => con = Connection.inMemory());
    tearDown(() => con.close());

    test('Check supported DuckDb API version', () {
      expect(bindings.duckdb_library_version().cast<Utf8>().toDartString(),
          'v1.3.0');
    });

    test('Get an error', () {
      expect(() => con.execute('CREATE TABL bools (isTrue BOOL);'),
          throwsStateError);
    });

    test('Create table with boolean column', () {
      con.execute('CREATE TABLE bools (status BOOL);');
      con.execute("INSERT INTO bools VALUES (true), (false), (NULL);");
      var result = con.fetch('SELECT * FROM bools;');
      expect(result['status'], [true, false, null]);
    });

    test('Create table with integer column', () {
      con.execute('CREATE TABLE items (i INTEGER, j INTEGER, k INTEGER);');
      con.execute("INSERT INTO items VALUES (1, NULL, 3), (2, 4, 6);");
      var result = con.fetch('SELECT * FROM items;');
      expect(result.keys.toList(), ['i', 'j', 'k']);
      expect(result['i'], [1, 2]);
      expect(result['j'], [null, 4]);
      expect(result['k'], [3, 6]);
    });

    test('Create table with big integer column', () {
      con.execute('CREATE TABLE items (i BIGINT);');
      con.execute("INSERT INTO items VALUES (1), (NULL);");
      var result = con.fetch('SELECT * FROM items;');
      expect(result['i'], [1, null]);
    });

    test('Create table with string column', () {
      con.execute('CREATE TABLE cities (state VARCHAR, city VARCHAR);');
      con.execute(
          "INSERT INTO cities VALUES ('CA', 'Los Angeles'), ('MD', NULL);");
      var result = con.fetch('SELECT * FROM cities;');
      expect(result['state'], ['CA', 'MD']);
      expect(result['city'], ['Los Angeles', null]);
    });

    test('Create table with timestamp column', () {
      // this has microsecond precision
      con.execute('CREATE TABLE ts (timestamp TIMESTAMP);');
      con.execute("INSERT INTO ts VALUES ('2024-01-15 11:30:02');");
      var result = con.fetch('SELECT * FROM ts;');
      expect(result.length, 1);
      expect(result['timestamp']!.first, DateTime.utc(2024, 1, 15, 11, 30, 02));
    });

    test('Create table with timestamp_s column', () {
      // this has second precision
      con.execute('CREATE TABLE ts (timestamp TIMESTAMP_S);');
      con.execute("INSERT INTO ts VALUES ('2024-01-15 11:30:02');");
      var result = con.fetch('SELECT * FROM ts;');
      expect(result.length, 1);
      expect(result['timestamp']!.first, DateTime.utc(2024, 1, 15, 11, 30, 2));
    });

    test('Create table with timestamp_ms column', () {
      // this has millisecond precision
      con.execute('CREATE TABLE ts (timestamp TIMESTAMP_MS);');
      con.execute("INSERT INTO ts VALUES ('2024-01-15 11:30:02.123');");
      var result = con.fetch('SELECT * FROM ts;');
      expect(result.length, 1);
      expect(result['timestamp']!.first,
          DateTime.utc(2024, 1, 15, 11, 30, 2, 123));
    });

    test('Create table with timestamp_ns column', () {
      // this has nanosecond precision, but Dart only supports microsecond precision
      con.execute('CREATE TABLE ts (timestamp TIMESTAMP_NS);');
      con.execute("INSERT INTO ts VALUES ('2024-01-15 11:30:02.123456789');");
      var result = con.fetch('SELECT * FROM ts;');
      expect(result.length, 1);
      expect(result['timestamp']!.first,
          DateTime.utc(2024, 1, 15, 11, 30, 2, 123, 456));
    });

    test('Create table with timestamptz column', () {
      con.execute('CREATE TABLE ts (timestamp TIMESTAMPTZ);');
      con.execute("INSERT INTO ts VALUES ('2024-01-15 11:30:02-05:00');");
      var result = con.fetch('SELECT * FROM ts;');
      expect(result.length, 1);
      expect(result['timestamp']!.first,
          DateTime.utc(2024, 1, 15, 16, 30, 2, 0, 0).microsecondsSinceEpoch);
    });

    test('Create table with date column', () {
      con.execute('CREATE TABLE dt (date DATE);');
      con.execute("INSERT INTO dt VALUES ('2024-05-20');");
      var result = con.fetch('SELECT * FROM dt;');
      expect(result.length, 1);
      expect(result['date']!.first, 19863);
    });

    test('Create table with decimal column', () {
      con.execute('CREATE TABLE tbl (price DECIMAL(6,2));');
      con.execute("INSERT INTO tbl VALUES (45.01);");
      con.execute("INSERT INTO tbl VALUES (55.15);");
      con.execute("INSERT INTO tbl VALUES (NULL);");
      var result = con.fetch('SELECT * FROM tbl;');
      expect(result.length, 1);
      expect(result['price']!, <Decimal?>[
        Decimal.parse('45.01'),
        Decimal.parse('55.15'),
        null,
      ]);
    });

    test('Create table with bigint decimal column', () {
      con.execute('CREATE TABLE tbl (price DECIMAL(15,2));');
      con.execute("INSERT INTO tbl VALUES (9007199254740.01);");
      con.execute("INSERT INTO tbl VALUES (NULL);");
      var result = con.fetch('SELECT * FROM tbl;');
      expect(result.length, 1);
      expect(result['price']!, <Decimal?>[
        Decimal.parse('9007199254740.01'),
        null,
      ]);
    });

    test('Create table with enum', () {
      con.execute("CREATE TABLE moods (mood ENUM ('ok', 'sad', 'happy'));");
      con.execute("INSERT INTO moods VALUES ('sad'), ('sad'), (NULL), ('ok');");
      final result = con.fetch('SELECT * FROM moods;');
      expect(result.keys.toSet(), {'mood'});
      expect(result['mood'], ['sad', 'sad', null, 'ok']);
    });

    test('Open duckdb db with config', () {
      final config = Config(defaultOrder: DefaultOrder.descending);
      final con = Connection.inMemory(config);
      var res = con.fetch('SHOW TABLES;');
      expect(res.length, 0);
      con.close();
    });

    test('fetchRows', () {
      con.execute('CREATE TABLE people (name VARCHAR, age INTEGER);');
      con.execute(
          "INSERT INTO people VALUES ('Tom', 31), ('Jenny', 29), ('Maria', 33);");
      var result = con.fetchRows('SELECT name, age FROM people ORDER BY name;',
          (List row) => Person(name: row[0], age: row[1]));
      expect(result.length, 3);
      expect(result.first.name, 'Jenny');
      expect(result.first.age, 29);
    });
  });

  /// Goal is to make this pass!
  group('test_all_types', () {
    late Connection con;
    setUp(() => con = Connection.inMemory());
    tearDown(() => con.close());

    test('all types', () {
      var res =
          con.fetch('SELECT * FROM test_all_types(use_large_enum = true);');
      expect(res.length, 3);
    });
  }, skip: true);
}

final class Person {
  Person({required this.name, required this.age});
  String name;
  int age;
}

void main() {
  tests();
}
