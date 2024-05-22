import 'package:duckdb_dart/duckdb_dart.dart';
import 'package:ffi/ffi.dart';
import 'package:test/test.dart';

void tests() {
  group('DuckDb tests', () {
    late Connection con;
    setUp(() => con = Connection.inMemory());
    tearDown(() => con.close());

    test('Check supported DuckDb API version', () {
      expect(DUCKDB_API_VERSION, 2);
      expect(bindings.duckdb_library_version().cast<Utf8>().toDartString(),
          'v0.10.2');
    });

    test('Create table with boolean column', () {
      con.execute('CREATE TABLE bools (isTrue BOOL);');
      con.execute("INSERT INTO bools VALUES (true), (false), (NULL);");
      var result = con.fetch('SELECT * FROM bools;');
      expect(result.length, 3);
      expect(result[0], {'isTrue': true});
      expect(result[1], {'isTrue': false});
      expect(result[2], {'isTrue': null});
    });

    test('Create table with integer column', () {
      con.execute('CREATE TABLE items (i INTEGER, j INTEGER, k INTEGER);');
      con.execute("INSERT INTO items VALUES (1, NULL, 3), (2, 4, 6);");
      var result = con.fetch('SELECT * FROM items;');
      expect(result.length, 2);
      expect(result.first, {'i': 1, 'j': null, 'k': 3});
    });

    test('Create table with big integer column', () {
      con.execute('CREATE TABLE items (i BIGINT);');
      con.execute("INSERT INTO items VALUES (1), (NULL);");
      var result = con.fetch('SELECT * FROM items;');
      expect(result.length, 2);
      expect(result.first, {'i': 1});
      expect(result.last, {'i': null});
    });

    test('Create table with string column', () {
      con.execute('CREATE TABLE cities (state VARCHAR, city VARCHAR);');
      con.execute(
          "INSERT INTO cities VALUES ('CA', 'Los Angeles'), ('MD', NULL);");
      var result = con.fetch('SELECT * FROM cities;');
      expect(result.length, 2);
      expect(result[0], {'state': 'CA', 'city': 'Los Angeles'});
      expect(result[1], {'state': 'MD', 'city': null});
    });

    test('Create table with timestamp column', () {
      con.execute('CREATE TABLE ts (timestamp TIMESTAMP);');
      con.execute("INSERT INTO ts VALUES ('2024-01-15 11:30:02');");
      var result = con.fetch('SELECT * FROM ts;');
      expect(result.length, 1);
      expect(result.first['timestamp'], DateTime.utc(2024, 1, 15, 11, 30, 02));
    });

    test('Create table with date column', () {
      con.execute('CREATE TABLE dt (date DATE);');
      con.execute("INSERT INTO dt VALUES ('2024-05-20');");
      var result = con.fetch('SELECT * FROM dt;');
      expect(result.length, 1);
      expect(result.first['date'], 19863);
    });

    // test('Create table with enum', () {
    //   con.execute("CREATE TABLE moods (mood ENUM ('sad', 'ok', 'happy'));");
    //   con.execute("INSERT INTO moods VALUES ('sad'), ('sad'), ('ok');");
    //   var result = con.fetch('SELECT * FROM moods;');
    //   expect(result.length, 3);
    //   expect(result.first['mood'], 'sad');
    // });

    test('Open duckdb db from file', () {
      final con = Connection(
          '/home/adrian/Downloads/Archive/IsoExpress/Capacity/HistoricalBidsOffers/MonthlyAuction/mra.duckdb');
      var res = con.fetch('SHOW TABLES;');
      expect(res.length, 1);
      expect(res.first, {'name': 'mra'});
    });
  });
}

void main() {
  tests();
}
