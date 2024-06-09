import 'package:duckdb_dart/duckdb_dart.dart';

main() {
  final con = Connection.inMemory();
  con.execute('CREATE TABLE tbl (state VARCHAR, population INTEGER);');
  con.execute("INSERT INTO tbl VALUES ('CA', 39539223), ('VA', 8631393);");
  var result = con.fetch('SELECT * FROM tbl;');
  print(result);
  con.close(); // close the connection to release resources
}
