enum AccessMode {
  automatic('AUTOMATIC'), // default
  readOnly('READ_ONLY'),
  readWrite('READ_WRITE');

  const AccessMode(this._value);

  final String _value;

  @override
  String toString() => _value;
}

enum DefaultOrder {
  ascending('ASC'),
  descending('DESC');

  const DefaultOrder(this._value);

  final String _value;

  @override
  String toString() => _value;
}

enum DefaultNullOrder {
  nullsFirst('NULLS_FIRST'),
  nullsLast('NULLS_LAST');

  const DefaultNullOrder(this._value);

  final String _value;

  @override
  String toString() => _value;
}

/// Configuration options to change the different settings of the database.
/// Many of these settings can be changed later using `PRAGMA` statements
/// as well.  See `https://duckdb.org/docs/configuration/overview`
///
/// Below are only a few of the settings available.  You can see all global
/// settings
/// `select name, value from duckdb_settings() where scope = 'GLOBAL' order by name;`
class Config {
  Config({
    this.accessMode,
    this.allowUnsignedExtensions,
    this.autoInstallKnownExtensions,
    this.autoLoadKnownExtensions,
    this.threads,
    this.maxMemoryGb,
    this.defaultNullOrder,
    this.defaultOrder,
    this.enableExternalAccess,
    this.enableObjectCache,
    this.preserveInsertionOrder,
  });

  /// Access mode of the database ([AUTOMATIC], READ_ONLY or READ_WRITE)
  final AccessMode? accessMode;

  /// Allow to load third-party duckdb extensions.
  final bool? allowUnsignedExtensions;

  final bool? autoInstallKnownExtensions;

  final bool? autoLoadKnownExtensions;

  /// Null ordering used when none is specified ([NULLS_FIRST] or NULLS_LAST)
  final DefaultNullOrder? defaultNullOrder;

  /// The order type used when none is specified ([ASC] or DESC)
  final DefaultOrder? defaultOrder;

  /// Allow the database to access external state (through e.g. COPY TO/FROM, CSV readers, pandas replacement scans, etc)
  final bool? enableExternalAccess;

  /// Whether or not object cache is used to cache e.g. Parquet metadata
  final bool? enableObjectCache;

  /// The maximum memory of the system (e.g. 1GB)
  final int? maxMemoryGb;

  final bool? preserveInsertionOrder;

  /// The number of total threads used by the system
  final int? threads;
}
