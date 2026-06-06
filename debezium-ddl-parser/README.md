# MySQL Oracle Grammar Files

This document describes the MySQL grammar files located at:
- `src/main/antlr4/io/debezium/ddl/parser/mysql/generated/MySqlLexer.g4`
- `src/main/antlr4/io/debezium/ddl/parser/mysql/generated/MySqlParser.g4`

These ANTLR grammar files are used by Debezium to parse MySQL DDL statements from the binlog and track how table schemas evolve over time.
The grammars are based on Oracle's official MySQL grammar, which provides accurate and up-to-date parsing support for MySQL 8.0+ syntax. We maintain customized copies of these grammar files with several customizations needed for Debezium's use case.

## Source

The MySQL ANTLR grammar files in this module are taken from the official Oracle MySQL grammar in the grammars-v4 repository:

- [`MySQLLexer.g4`](https://github.com/antlr/grammars-v4/blob/master/sql/mysql/Oracle/MySQLLexer.g4)
- [`MySQLParser.g4`](https://github.com/antlr/grammars-v4/blob/master/sql/mysql/Oracle/MySQLParser.g4)

## Customizations Applied

The following customizations have been applied to the Oracle grammar files for Debezium:

### MySqlLexer.g4

**Line 1** - Grammar name changed to match Java naming conventions:
```antlr
// Upstream
lexer grammar MySQLLexer;

// Debezium
lexer grammar MySqlLexer;
```

**Line 30** - SuperClass name changed to match Java naming conventions:
```antlr
// Upstream
superClass = MySQLLexerBase;

// Debezium
superClass = MySqlLexerBase;
```

**Lines 43-45** - Header section added with Debezium-specific import:
```antlr
// Upstream
// Insert here @header for lexer.

// Debezium
@header {
    import io.debezium.antlr.mysql.MySqlLexerBase;
}
```

**Lines 3578-3581** - VECTOR_SYMBOL token added for vector type support (not available in Oracle grammar yet since is based on MySQL 8.0):
```antlr
// Added in Debezium
VECTOR_SYMBOL
    : V E C T O R
    ;
```

### MySqlParser.g4

**Line 1** - Grammar name changed to match Java naming conventions:
```antlr
// Upstream
parser grammar MySQLParser;

// Debezium
parser grammar MySqlParser;
```

**Line 30** - SuperClass name changed to match Java naming conventions:
```antlr
// Upstream
superClass = MySQLParserBase;

// Debezium
superClass = MySqlParserBase;
```

**Line 31** - TokenVocab reference updated to match renamed lexer:
```antlr
// Upstream
tokenVocab = MySQLLexer;

// Debezium
tokenVocab = MySqlLexer;
```

**Lines 34-36** - Header section added with Debezium-specific import:
```antlr
// Upstream
// Insert here @header for parser.

// Debezium
@header {
    import io.debezium.antlr.mysql.MySqlParserBase;
}
```

**Line 3779** - MySQL 9.x VECTOR type added in dataType rule (not available in Oracle grammar yet since is based on MySQL 8.0):
```antlr
// Added in Debezium
| type = VECTOR_SYMBOL fieldLength?
```

**Line 3919** - Added catch-all for engine-specific table options (TokuDB, RocksDB, Aria) that aren't defined in the upstream grammar:
```antlr
// Added in Debezium
| identifier EQUAL_OPERATOR? textOrIdentifier  // Engine-specific options
```

## Upgrading Grammar Files

To upgrade to a newer version of the Oracle MySQL grammar:

1. **Download the latest grammar files** from the grammars-v4 repository:
   ```bash
   curl -O https://raw.githubusercontent.com/antlr/grammars-v4/master/sql/mysql/Oracle/MySQLLexer.g4
   curl -O https://raw.githubusercontent.com/antlr/grammars-v4/master/sql/mysql/Oracle/MySQLParser.g4
   ```

2. **Copy the files** to the Debezium grammar directory:
   ```bash
   cp MySQLLexer.g4 debezium-ddl-parser/src/main/antlr4/io/debezium/ddl/parser/mysql/generated/MySqlLexer.g4
   cp MySQLParser.g4 debezium-ddl-parser/src/main/antlr4/io/debezium/ddl/parser/mysql/generated/MySqlParser.g4
   ```

3. **Re-apply the customizations** listed above to the copied files. Pay attention to the line numbers mentioned (they may shift slightly if upstream grammar has changed).

4. **Build and test** to verify the grammar works correctly:
   ```bash
   mvn clean install -pl debezium-ddl-parser
   mvn clean install -pl debezium-connector-binlog
   mvn clean install -pl debezium-connector-mysql
   ```

## Notes

- The grammar files must be named with `MySql` casing (not `MySQL`) to match Java naming conventions used in Debezium.
- The base classes (`MySqlLexerBase` and `MySqlParserBase`) are implemented in this module (`debezium-ddl-parser`) and provide runtime support for MySQL-specific features like charset introducers and version-specific syntax.
