import 'package:duckdb/duckdb.dart';
import 'package:ffi/ffi.dart';
import 'package:test/test.dart';

void tests() {
  group('DuckDb tests', () {
    late Connection con;
    setUp(() => con = Connection.inMemory());
    tearDown(() => con.close());

    test('Create table of booleans', () {
      con.execute('CREATE TABLE bools (isTrue BOOL);');
      con.execute("INSERT INTO bools VALUES (true), (false), (NULL);");
      var result = con.fetch('SELECT * FROM bools;');
      expect(result.length, 3);
      expect(result[0], {'isTrue': true});
      expect(result[1], {'isTrue': false});
      expect(result[2], {'isTrue': null});
    });

    test('Create table of integers', () {
      con.execute('CREATE TABLE items (i INTEGER, j INTEGER, k INTEGER);');
      con.execute("INSERT INTO items VALUES (1, NULL, 3), (2, 4, 6);");
      var result = con.fetch('SELECT * FROM items;');
      expect(result.length, 2);
      expect(result.first, {'i': 1, 'j': null, 'k': 3});
    });

    test('Create table of strings', () {
      con.execute('CREATE TABLE cities (state VARCHAR, city VARCHAR);');
      con.execute(
          "INSERT INTO cities VALUES ('CA', 'Los Angeles'), ('MD', NULL);");
      var result = con.fetch('SELECT * FROM cities;');
      expect(result.length, 2);
      expect(result[0], {'state': 'CA', 'city': 'Los Angeles'});
      expect(result[1], {'state': 'MD', 'city': null});
    });

    test('Create table with timestamp', () {
      con.execute('CREATE TABLE ts (timestamp TIMESTAMP);');
      con.execute("INSERT INTO ts VALUES ('2024-01-15 11:30:02');");
      var result = con.fetch('SELECT * FROM ts;');
      expect(result.length, 1);
      expect(result.first['timestamp'], DateTime.utc(2024, 1, 15, 11, 30, 02));
    });

    test('Create table with date', () {
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


  });
}

void main() {
  print('DuckDb API version: $DUCKDB_API_VERSION');
  print(
      'DuckDb library version: ${bindings.duckdb_library_version().cast<Utf8>().toDartString()}');
  tests();
}
