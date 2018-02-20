/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.ddl.DataType;
import io.debezium.relational.ddl.DataTypeParser;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.ddl.DdlParserListener.SetVariableEvent;
import io.debezium.relational.ddl.DdlTokenizer;
import io.debezium.text.MultipleParsingExceptions;
import io.debezium.text.ParsingException;
import io.debezium.text.TokenStream;
import io.debezium.text.TokenStream.Marker;

/**
 * A parser for DDL statements.
 * <p>
 * See the <a href="http://dev.mysql.com/doc/refman/5.7/en/sql-syntax-data-definition.html">MySQL SQL Syntax documentation</a> for
 * the grammar supported by this parser.
 *
 * @author Randall Hauch
 */
@NotThreadSafe
public class MySqlDdlParser extends DdlParser {

    /**
     * The system variable name for the name of the character set that the server uses by default.
     * See http://dev.mysql.com/doc/refman/5.7/en/server-options.html#option_mysqld_character-set-server
     */
    private static final String SERVER_CHARSET_NAME = MySqlSystemVariables.CHARSET_NAME_SERVER;

    private final MySqlSystemVariables systemVariables = new MySqlSystemVariables();
    private final ConcurrentMap<String, String> charsetNameForDatabase = new ConcurrentHashMap<>();

    /**
     * Create a new DDL parser for MySQL that does not include view definitions.
     */
    public MySqlDdlParser() {
        super(";");
    }

    /**
     * Create a new DDL parser for MySQL.
     *
     * @param includeViews {@code true} if view definitions should be included, or {@code false} if they should be skipped
     */
    public MySqlDdlParser(boolean includeViews) {
        super(";", includeViews);
    }

    protected MySqlSystemVariables systemVariables() {
        return systemVariables;
    }

    @Override
    protected void initializeDataTypes(DataTypeParser dataTypes) {
        dataTypes.register(Types.BIT, "BIT[(L)]");
        // MySQL unsigned TINYINTs can be mapped to JDBC TINYINT, but signed values don't map. Therefore, per JDBC spec
        // the best mapping for all TINYINT values is JDBC's SMALLINT, which maps to a short.
        dataTypes.register(Types.SMALLINT, "TINYINT[(L)] [UNSIGNED|SIGNED] [ZEROFILL]");
        dataTypes.register(Types.SMALLINT, "SMALLINT[(L)] [UNSIGNED|SIGNED] [ZEROFILL]");
        dataTypes.register(Types.INTEGER, "MEDIUMINT[(L)] [UNSIGNED|SIGNED] [ZEROFILL]");
        dataTypes.register(Types.INTEGER, "INT[(L)] [UNSIGNED|SIGNED] [ZEROFILL]");
        dataTypes.register(Types.INTEGER, "INTEGER[(L)] [UNSIGNED|SIGNED] [ZEROFILL]");
        dataTypes.register(Types.BIGINT, "BIGINT[(L)] [UNSIGNED|SIGNED] [ZEROFILL]");
        dataTypes.register(Types.REAL, "REAL[(M[,D])] [UNSIGNED] [ZEROFILL]");
        dataTypes.register(Types.DOUBLE, "DOUBLE[(M[,D])] [UNSIGNED|SIGNED] [ZEROFILL]");
        dataTypes.register(Types.DOUBLE, "DOUBLE PRECISION[(M[,D])] [UNSIGNED|SIGNED] [ZEROFILL]");
        dataTypes.register(Types.FLOAT, "FLOAT[(M[,D])] [UNSIGNED|SIGNED] [ZEROFILL]");
        dataTypes.register(Types.DECIMAL, "DECIMAL[(M[,D])] [UNSIGNED|SIGNED] [ZEROFILL]");
        dataTypes.register(Types.DECIMAL, "FIXED[(M[,D])] [UNSIGNED|SIGNED] [ZEROFILL]");
        dataTypes.register(Types.DECIMAL, "DEC[(M[,D])] [UNSIGNED|SIGNED] [ZEROFILL]");
        dataTypes.register(Types.NUMERIC, "NUMERIC[(M[,D])] [UNSIGNED|SIGNED] [ZEROFILL]");
        dataTypes.register(Types.BOOLEAN, "BOOLEAN");
        dataTypes.register(Types.BOOLEAN, "BOOL");
        dataTypes.register(Types.DATE, "DATE");
        dataTypes.register(Types.TIME, "TIME[(L)]");
        dataTypes.register(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP[(L)]"); // includes timezone information
        dataTypes.register(Types.TIMESTAMP, "DATETIME[(L)]");
        dataTypes.register(Types.INTEGER, "YEAR[(2|4)]");
        dataTypes.register(Types.BINARY, "CHAR[(L)] BINARY");
        dataTypes.register(Types.VARBINARY, "VARCHAR(L) BINARY");
        dataTypes.register(Types.BINARY, "BINARY[(L)]");
        dataTypes.register(Types.VARCHAR, "VARCHAR(L)");
        dataTypes.register(Types.NVARCHAR, "NVARCHAR(L)");
        dataTypes.register(Types.NVARCHAR, "NATIONAL VARCHAR(L)");
        dataTypes.register(Types.NVARCHAR, "NCHAR VARCHAR(L)");
        dataTypes.register(Types.NVARCHAR, "NATIONAL CHARACTER VARYING(L)");
        dataTypes.register(Types.NVARCHAR, "NATIONAL CHAR VARYING(L)");
        dataTypes.register(Types.CHAR, "CHAR[(L)]");
        dataTypes.register(Types.NCHAR, "NCHAR[(L)]");
        dataTypes.register(Types.NCHAR, "NATIONAL CHARACTER(L)");
        dataTypes.register(Types.VARBINARY, "VARBINARY(L)");
        dataTypes.register(Types.BLOB, "TINYBLOB");
        dataTypes.register(Types.BLOB, "BLOB");
        dataTypes.register(Types.BLOB, "MEDIUMBLOB");
        dataTypes.register(Types.BLOB, "LONGBLOB");
        dataTypes.register(Types.BLOB, "TINYTEXT BINARY");
        dataTypes.register(Types.BLOB, "TEXT BINARY");
        dataTypes.register(Types.BLOB, "MEDIUMTEXT BINARY");
        dataTypes.register(Types.BLOB, "LONGTEXT BINARY");
        dataTypes.register(Types.VARCHAR, "TINYTEXT");
        dataTypes.register(Types.VARCHAR, "TEXT[(L)]");
        dataTypes.register(Types.VARCHAR, "MEDIUMTEXT");
        dataTypes.register(Types.VARCHAR, "LONGTEXT");
        dataTypes.register(Types.CHAR, "ENUM(...)");
        dataTypes.register(Types.CHAR, "SET(...)");
        dataTypes.register(Types.OTHER, "JSON");
        dataTypes.register(Types.OTHER, "GEOMETRY");
        dataTypes.register(Types.OTHER, "POINT");
        dataTypes.register(Types.OTHER, "LINESTRING");
        dataTypes.register(Types.OTHER, "POLYGON");
        dataTypes.register(Types.OTHER, "MULTIPOINT");
        dataTypes.register(Types.OTHER, "MULTILINESTRING");
        dataTypes.register(Types.OTHER, "MULTIPOLYGON");
        dataTypes.register(Types.OTHER, "GEOMETRYCOLLECTION");
    }

    @Override
    protected void initializeKeywords(TokenSet keywords) {
    }

    @Override
    protected void initializeStatementStarts(TokenSet statementStartTokens) {
        statementStartTokens.add("CREATE", "ALTER", "DROP", "GRANT", "REVOKE", "FLUSH", "TRUNCATE", "COMMIT", "USE", "SAVEPOINT", "ROLLBACK",
                // table maintenance statements: https://dev.mysql.com/doc/refman/5.7/en/table-maintenance-sql.html
                "ANALYZE", "OPTIMIZE", "REPAIR",
                // DML-related statements
                "DELETE", "INSERT"
                );
    }

    @Override
    protected void parseNextStatement(Marker marker) {
        if (tokens.matches(DdlTokenizer.COMMENT)) {
            parseComment(marker);
        } else if (tokens.matches("CREATE")) {
            parseCreate(marker);
        } else if (tokens.matches("ALTER")) {
            parseAlter(marker);
        } else if (tokens.matches("DROP")) {
            parseDrop(marker);
        } else if (tokens.matches("RENAME")) {
            parseRename(marker);
        } else if (tokens.matches("USE")) {
            parseUse(marker);
        } else if (tokens.matches("SET")) {
            parseSet(marker);
        } else if (tokens.matches("INSERT")) {
            consumeStatement();
        } else if (tokens.matches("DELETE")) {
            consumeStatement();
        } else {
            parseUnknownStatement(marker);
        }
    }

    protected void parseSet(Marker start) {
        tokens.consume("SET");
        AtomicReference<MySqlSystemVariables.Scope> scope = new AtomicReference<>();
        parseSetVariable(start, scope);
        while (tokens.canConsume(',')) {
            parseSetVariable(start, scope);
        }
        consumeRemainingStatement(start);
        debugParsed(start);
    }

    protected void parseSetVariable(Marker start, AtomicReference<MySqlSystemVariables.Scope> scope) {
        // First, use the modifier to set the scope ...
        if (tokens.canConsume("GLOBAL") || tokens.canConsume("@@GLOBAL", ".")) {
            scope.set(MySqlSystemVariables.Scope.GLOBAL);
        } else if (tokens.canConsume("SESSION") || tokens.canConsume("@@SESSION", ".")) {
            scope.set(MySqlSystemVariables.Scope.SESSION);
        } else if (tokens.canConsume("LOCAL") || tokens.canConsume("@@LOCAL", ".")) {
            scope.set(MySqlSystemVariables.Scope.LOCAL);
        }

        // Now handle the remainder of the variable assignment ...
        if (tokens.canConsume("PASSWORD")) {
            // ignore
        } else if (tokens.canConsume("TRANSACTION", "ISOLATION", "LEVEL")) {
            // ignore
        } else if (tokens.canConsume("CHARACTER", "SET") || tokens.canConsume("CHARSET")) {
            // Sets two variables plus the current character set for the current database
            // See https://dev.mysql.com/doc/refman/5.7/en/set-statement.html
            String charsetName = tokens.consume();
            if ("DEFAULT".equalsIgnoreCase(charsetName)) {
                charsetName = currentDatabaseCharset();
            }
            systemVariables.setVariable(scope.get(), "character_set_client", charsetName);
            systemVariables.setVariable(scope.get(), "character_set_results", charsetName);
            // systemVariables.setVariable(scope.get(), "collation_connection", ...);
        } else if (tokens.canConsume("NAMES")) {
            // https://dev.mysql.com/doc/refman/5.7/en/set-statement.html
            String charsetName = tokens.consume();
            if ("DEFAULT".equalsIgnoreCase(charsetName)) {
                charsetName = currentDatabaseCharset();
            }
            systemVariables.setVariable(scope.get(), "character_set_client", charsetName);
            systemVariables.setVariable(scope.get(), "character_set_results", charsetName);
            systemVariables.setVariable(scope.get(), "character_set_connection", charsetName);
            // systemVariables.setVariable(scope.get(), "collation_connection", ...);

            if (tokens.canConsume("COLLATION")) {
                tokens.consume(); // consume the collation name but do nothing with it
            }
        } else {
            // This is a global, session, or local system variable, or a user variable.
            String variableName = parseVariableName();
            tokens.canConsume(":"); // := is for user variables
            tokens.consume("=");
            String value = parseVariableValue();

            if (variableName.startsWith("@")) {
                // This is a user variable, so do nothing with it ...
            } else {
                systemVariables.setVariable(scope.get(), variableName, value);

                // If this is setting 'character_set_database', then we need to record the character set for
                // the given database ...
                if ("character_set_database".equalsIgnoreCase(variableName)) {
                    String currentDatabaseName = currentSchema();
                    if (currentDatabaseName != null) {
                        charsetNameForDatabase.put(currentDatabaseName, value);
                    }
                }

                // Signal that the variable was set ...
                signalEvent(new SetVariableEvent(variableName, value, statement(start)));
            }
        }
    }

    protected String parseVariableName() {
        String variableName = tokens.consume();
        while (tokens.canConsume("-")) {
            variableName = variableName + "-" + tokens.consume();
        }
        return variableName;
    }

    protected String parseVariableValue() {
        if (tokens.canConsumeAnyOf(",", ";")) {
            // The variable is blank ...
            return "";
        }
        Marker start = tokens.mark();
        tokens.consumeUntilEndOrOneOf(",", ";");
        String value = tokens.getContentFrom(start);
        if (value.startsWith("'") && value.endsWith("'")) {
            // Remove the single quotes around the value ...
            if (value.length() <= 2) {
                value = "";
            } else {
                value = value.substring(1, value.length() - 2);
            }
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void parseCreate(Marker marker) {
        tokens.consume("CREATE");
        if (tokens.matches("TABLE") || tokens.matches("TEMPORARY", "TABLE")) {
            parseCreateTable(marker);
        } else if (tokens.matches("VIEW")) {
            parseCreateView(marker);
        } else if (tokens.matchesAnyOf("DATABASE", "SCHEMA")) {
            parseCreateDatabase(marker);
        } else if (tokens.matchesAnyOf("EVENT")) {
            parseCreateEvent(marker);
        } else if (tokens.matchesAnyOf("FUNCTION", "PROCEDURE")) {
            parseCreateProcedure(marker);
        } else if (tokens.matchesAnyOf("UNIQUE", "FULLTEXT", "SPATIAL", "INDEX")) {
            parseCreateIndex(marker);
        } else if (tokens.matchesAnyOf("SERVER")) {
            parseCreateUnknown(marker);
        } else if (tokens.matchesAnyOf("TABLESPACE")) {
            parseCreateUnknown(marker);
        } else if (tokens.matchesAnyOf("TRIGGER")) {
            parseCreateTrigger(marker);
        } else {
            // It could be several possible things (including more elaborate forms of those matches tried above),
            sequentially(this::parseCreateView,
                         this::parseCreateProcedure,
                         this::parseCreateTrigger,
                         this::parseCreateEvent,
                         this::parseCreateUnknown);
        }
    }

    protected void parseCreateDatabase(Marker start) {
        tokens.consumeAnyOf("DATABASE", "SCHEMA");
        tokens.canConsume("IF", "NOT", "EXISTS");
        String dbName = tokens.consume();
        parseDatabaseOptions(start, dbName);
        consumeRemainingStatement(start);
        signalCreateDatabase(dbName, start);
        debugParsed(start);
    }

    protected void parseAlterDatabase(Marker start) {
        tokens.consumeAnyOf("DATABASE", "SCHEMA");
        String dbName = tokens.consume();
        parseDatabaseOptions(start, dbName);
        consumeRemainingStatement(start);
        signalAlterDatabase(dbName, null, start);
        debugParsed(start);
    }

    protected void parseDropDatabase(Marker start) {
        tokens.consumeAnyOf("DATABASE", "SCHEMA");
        tokens.canConsume("IF", "EXISTS");
        String dbName = tokens.consume();
        signalDropDatabase(dbName, start);
        debugParsed(start);
    }

    protected void parseDatabaseOptions(Marker start, String dbName) {
        // Handle the default character set and collation ...
        tokens.canConsume("DEFAULT");
        if (tokens.canConsume("CHARACTER", "SET") || tokens.canConsume("CHARSET")) {
            tokens.canConsume("=");
            String charsetName = tokens.consume();
            if ("DEFAULT".equalsIgnoreCase(charsetName)) {
                charsetName = systemVariables.getVariable(SERVER_CHARSET_NAME);
            }
            charsetNameForDatabase.put(dbName, charsetName);
        }
        if (tokens.canConsume("COLLATE")) {
            tokens.canConsume("=");
            tokens.consume(); // collation name
        }
    }

    protected void parseCreateTable(Marker start) {
        tokens.canConsume("TEMPORARY");
        tokens.consume("TABLE");
        boolean onlyIfNotExists = tokens.canConsume("IF", "NOT", "EXISTS");
        TableId tableId = parseQualifiedTableName(start);
        if (tokens.canConsume("LIKE")) {
            TableId originalId = parseQualifiedTableName(start);
            Table original = databaseTables.forTable(originalId);
            if (original != null) {
                databaseTables.overwriteTable(tableId, original.columns(), original.primaryKeyColumnNames(), original.defaultCharsetName());
            }
            consumeRemainingStatement(start);
            signalCreateTable(tableId, start);
            debugParsed(start);
            return;
        }
        if (onlyIfNotExists && databaseTables.forTable(tableId) != null) {
            // The table does exist, so we should do nothing ...
            consumeRemainingStatement(start);
            signalCreateTable(tableId, start);
            debugParsed(start);
            return;
        }
        TableEditor table = databaseTables.editOrCreateTable(tableId);

        // create_definition ...
        if (tokens.matches('(')) parseCreateDefinitionList(start, table);
        // table_options ...
        parseTableOptions(start, table);
        // partition_options ...
        if (tokens.matches("PARTITION")) {
            parsePartitionOptions(start, table);
        }
        // select_statement
        if (tokens.canConsume("AS") || tokens.canConsume("IGNORE", "AS") || tokens.canConsume("REPLACE", "AS")) {
            parseAsSelectStatement(start, table);
        }

        // Make sure that the table's character set has been set ...
        if (!table.hasDefaultCharsetName()) {
            table.setDefaultCharsetName(currentDatabaseCharset());
        }

        // Update the table definition ...
        databaseTables.overwriteTable(table.create());
        signalCreateTable(tableId, start);
        debugParsed(start);
    }

    protected void parseTableOptions(Marker start, TableEditor table) {
        while (parseTableOption(start, table)) {}
    }

    protected boolean parseTableOption(Marker start, TableEditor table) {
        if (tokens.canConsume("AUTO_INCREMENT")) {
            // Sets the auto-incremented value for the next incremented value ...
            tokens.canConsume('=');
            tokens.consume();
            return true;
        } else if (tokens.canConsumeAnyOf("CHECKSUM", "ENGINE", "AVG_ROW_LENGTH", "MAX_ROWS", "MIN_ROWS", "ROW_FORMAT",
                                          "DELAY_KEY_WRITE", "INSERT_METHOD", "KEY_BLOCK_SIZE", "PACK_KEYS",
                                          "STATS_AUTO_RECALC", "STATS_PERSISTENT", "STATS_SAMPLE_PAGES" , "PAGE_CHECKSUM" )) {
            // One option token followed by '=' by a single value
            tokens.canConsume('=');
            tokens.consume();
            return true;
        } else if (tokens.canConsume("DEFAULT", "CHARACTER", "SET") || tokens.canConsume("CHARACTER", "SET") ||
                tokens.canConsume("DEFAULT", "CHARSET") || tokens.canConsume("CHARSET")) {
            tokens.canConsume('=');
            String charsetName = tokens.consume();
            if ("DEFAULT".equalsIgnoreCase(charsetName)) {
                // The table's default character set is set to the character set of the current database ...
                charsetName = currentDatabaseCharset();
            }
            table.setDefaultCharsetName(charsetName);
            return true;
        } else if (tokens.canConsume("DEFAULT", "COLLATE") || tokens.canConsume("COLLATE")) {
            tokens.canConsume('=');
            tokens.consume();
            return true;
        } else if (tokens.canConsumeAnyOf("COMMENT", "COMPRESSION", "CONNECTION", "ENCRYPTION", "PASSWORD")) {
            tokens.canConsume('=');
            consumeQuotedString();
            return true;
        } else if (tokens.canConsume("DATA", "DIRECTORY") || tokens.canConsume("INDEX", "DIRECTORY")) {
            tokens.canConsume('=');
            consumeQuotedString();
            return true;
        } else if (tokens.canConsume("TABLESPACE")) {
            tokens.consume();
            return true;
        } else if (tokens.canConsumeAnyOf("STORAGE", "ENGINE")) {
            tokens.consume(); // storage engine name
            return true;
        } else if (tokens.canConsume("UNION")) {
            tokens.canConsume('=');
            tokens.consume('(');
            tokens.consume();
            while (tokens.canConsume(',')) {
                tokens.consume();
            }
            tokens.consume(')');
            return true;
        }
        return false;
    }

    protected void parsePartitionOptions(Marker start, TableEditor table) {
        tokens.consume("PARTITION", "BY");
        if (tokens.canConsume("LINEAR", "HASH") || tokens.canConsume("HASH")) {
            consumeExpression(start);
        } else if (tokens.canConsume("LINEAR", "KEY") || tokens.canConsume("KEY")) {
            if (tokens.canConsume("ALGORITHM")) {
                tokens.consume("=");
                tokens.consumeAnyOf("1", "2");
            }
            parseColumnNameList(start);
        } else if (tokens.canConsumeAnyOf("RANGE", "LIST")) {
            if (tokens.canConsume("COLUMNS")) {
                parseColumnNameList(start);
            } else {
                consumeExpression(start);
            }
        }

        if (tokens.canConsume("PARTITIONS")) {
            tokens.consume();
        }
        if (tokens.canConsume("SUBPARTITION", "BY")) {
            if (tokens.canConsume("LINEAR", "HASH") || tokens.canConsume("HASH")) {
                consumeExpression(start);
            } else if (tokens.canConsume("LINEAR", "KEY") || tokens.canConsume("KEY")) {
                if (tokens.canConsume("ALGORITHM")) {
                    tokens.consume("=");
                    tokens.consumeAnyOf("1", "2");
                }
                parseColumnNameList(start);
            }
            if (tokens.canConsume("SUBPARTITIONS")) {
                tokens.consume();
            }
        }
        if (tokens.canConsume('(')) {
            do {
                parsePartitionDefinition(start, table);
            } while (tokens.canConsume(','));
            tokens.consume(')');
        }
    }

    protected void parsePartitionDefinition(Marker start, TableEditor table) {
        tokens.consume("PARTITION");
        tokens.consume(); // name
        if (tokens.canConsume("VALUES")) {
            if (tokens.canConsume("LESS", "THAN")) {
                if (!tokens.canConsume("MAXVALUE")) {
                    consumeExpression(start);
                }
            } else {
                tokens.consume("IN");
                consumeValueList(start);
            }
        } else if (tokens.canConsume("STORAGE", "ENGINE") || tokens.canConsume("ENGINE")) {
            tokens.canConsume('=');
            tokens.consume();
        } else if (tokens.canConsumeAnyOf("COMMENT")) {
            tokens.canConsume('=');
            consumeQuotedString();
        } else if (tokens.canConsumeAnyOf("DATA", "INDEX") && tokens.canConsume("DIRECTORY")) {
            tokens.canConsume('=');
            consumeQuotedString();
        } else if (tokens.canConsumeAnyOf("MAX_ROWS", "MIN_ROWS", "TABLESPACE")) {
            tokens.canConsume('=');
            tokens.consume();
        } else if (tokens.canConsume('(')) {
            do {
                parseSubpartitionDefinition(start, table);
            } while (tokens.canConsume(','));
            tokens.consume(')');
        }
    }

    protected void parseSubpartitionDefinition(Marker start, TableEditor table) {
        tokens.consume("SUBPARTITION");
        tokens.consume(); // name
        if (tokens.canConsume("STORAGE", "ENGINE") || tokens.canConsume("ENGINE")) {
            tokens.canConsume('=');
            tokens.consume();
        } else if (tokens.canConsumeAnyOf("COMMENT")) {
            tokens.canConsume('=');
            consumeQuotedString();
        } else if (tokens.canConsumeAnyOf("DATA", "INDEX") && tokens.canConsume("DIRECTORY")) {
            tokens.canConsume('=');
            consumeQuotedString();
        } else if (tokens.canConsumeAnyOf("MAX_ROWS", "MIN_ROWS", "TABLESPACE")) {
            tokens.canConsume('=');
            tokens.consume();
        }
    }

    protected void parseAsSelectStatement(Marker start, TableEditor table) {
        tokens.consume("SELECT");
        consumeRemainingStatement(start);
    }

    protected void parseCreateDefinitionList(Marker start, TableEditor table) {
        tokens.consume('(');
        parseCreateDefinition(start, table, false);
        while (tokens.canConsume(',')) {
            parseCreateDefinition(start, table, false);
        }
        tokens.consume(')');
    }

    /**
     * @param isAlterStatement whether this is an ALTER TABLE statement or not (i.e. CREATE TABLE)
     */
    protected void parseCreateDefinition(Marker start, TableEditor table, boolean isAlterStatement) {
        // If the first token is a quoted identifier, then we know it is a column name ...
        Collection<ParsingException> errors = null;
        boolean quoted = isNextTokenQuotedIdentifier();
        Marker defnStart = tokens.mark();
        if (!quoted) {
            // The first token is not quoted so let's check for other expressions ...
            if (tokens.canConsume("CHECK")) {
                // Try to parse the constraints first ...
                consumeExpression(start);
                return;
            }
            if (tokens.canConsume("CONSTRAINT", TokenStream.ANY_VALUE, "PRIMARY", "KEY")
                    || tokens.canConsume("CONSTRAINT", "PRIMARY", "KEY")
                    || tokens.canConsume("PRIMARY", "KEY")
            ) {
                try {
                    if (tokens.canConsume("USING")) {
                        parseIndexType(start);
                    }
                    if (!tokens.matches('(')) {
                        tokens.consume(); // index name
                    }
                    List<String> pkColumnNames = parseIndexColumnNames(start);
                    table.setPrimaryKeyNames(pkColumnNames);
                    parseIndexOptions(start);
                    // MySQL does not allow a primary key to have nullable columns, so let's make sure we model that correctly ...
                    pkColumnNames.forEach(name -> {
                        Column c = table.columnWithName(name);
                        if (c != null && c.isOptional()) {
                            table.addColumn(c.edit().optional(false).create());
                        }
                    });
                    return;
                } catch (ParsingException e) {
                    // Invalid names, so rewind and continue
                    errors = accumulateParsingFailure(e, errors);
                    tokens.rewind(defnStart);
                } catch (MultipleParsingExceptions e) {
                    // Invalid names, so rewind and continue
                    errors = accumulateParsingFailure(e, errors);
                    tokens.rewind(defnStart);
                }
            }
            if (tokens.canConsume("CONSTRAINT", TokenStream.ANY_VALUE, "UNIQUE") || tokens.canConsume("UNIQUE")) {
                tokens.canConsumeAnyOf("KEY", "INDEX");
                try {
                    if (!tokens.matches('(')) {
                        if (!tokens.matches("USING")) {
                            tokens.consume(); // name of unique index ...
                        }
                        if (tokens.matches("USING")) {
                            parseIndexType(start);
                        }
                    }
                    List<String> uniqueKeyColumnNames = parseIndexColumnNames(start);
                    if (table.primaryKeyColumnNames().isEmpty()) {
                        table.setPrimaryKeyNames(uniqueKeyColumnNames); // this may eventually get overwritten by a real PK
                    }
                    parseIndexOptions(start);
                    return;
                } catch (ParsingException e) {
                    // Invalid names, so rewind and continue
                    errors = accumulateParsingFailure(e, errors);
                    tokens.rewind(defnStart);
                } catch (MultipleParsingExceptions e) {
                    // Invalid names, so rewind and continue
                    errors = accumulateParsingFailure(e, errors);
                    tokens.rewind(defnStart);
                }
            }
            if (tokens.canConsume("CONSTRAINT", TokenStream.ANY_VALUE, "FOREIGN", "KEY") || tokens.canConsume("FOREIGN", "KEY")) {
                try {
                    if (!tokens.matches('(')) {
                        tokens.consume(); // name of foreign key
                    }
                    parseIndexColumnNames(start);
                    if (tokens.matches("REFERENCES")) {
                        parseReferenceDefinition(start);
                    }
                    return;
                } catch (ParsingException e) {
                    // Invalid names, so rewind and continue
                    errors = accumulateParsingFailure(e, errors);
                    tokens.rewind(defnStart);
                } catch (MultipleParsingExceptions e) {
                    // Invalid names, so rewind and continue
                    errors = accumulateParsingFailure(e, errors);
                    tokens.rewind(defnStart);
                }
            }
            if (tokens.canConsumeAnyOf("INDEX", "KEY")) {
                try {
                    if (!tokens.matches('(')) {
                        if (!tokens.matches("USING")) {
                            tokens.consume(); // name of unique index ...
                        }
                        if (tokens.matches("USING")) {
                            parseIndexType(start);
                        }
                    }
                    parseIndexColumnNames(start);
                    parseIndexOptions(start);
                    return;
                } catch (ParsingException e) {
                    // Invalid names, so rewind and continue
                    errors = accumulateParsingFailure(e, errors);
                    tokens.rewind(defnStart);
                } catch (MultipleParsingExceptions e) {
                    // Invalid names, so rewind and continue
                    errors = accumulateParsingFailure(e, errors);
                    tokens.rewind(defnStart);
                }
            }
            if (tokens.canConsumeAnyOf("FULLTEXT", "SPATIAL")) {
                try {
                    tokens.canConsumeAnyOf("INDEX", "KEY");
                    if (!tokens.matches('(')) {
                        tokens.consume(); // name of unique index ...
                    }
                    parseIndexColumnNames(start);
                    parseIndexOptions(start);
                    return;
                } catch (ParsingException e) {
                    // Invalid names, so rewind and continue
                    errors = accumulateParsingFailure(e, errors);
                    tokens.rewind(defnStart);
                } catch (MultipleParsingExceptions e) {
                    // Invalid names, so rewind and continue
                    errors = accumulateParsingFailure(e, errors);
                    tokens.rewind(defnStart);
                }
            }
        }

        try {
            // It's either quoted (meaning it's a column definition)
            if (isAlterStatement && !quoted) {
                tokens.canConsume("COLUMN"); // optional for ALTER TABLE
            }

            String columnName = parseColumnName();
            parseCreateColumn(start, table, columnName, null);
        } catch (ParsingException e) {
            if (errors != null) {
                errors = accumulateParsingFailure(e, errors);
                throw new MultipleParsingExceptions(errors);
            }
            throw e;
        } catch (MultipleParsingExceptions e) {
            if (errors != null) {
                errors = accumulateParsingFailure(e, errors);
                throw new MultipleParsingExceptions(errors);
            }
            throw e;
        }
    }

    protected Column parseCreateColumn(Marker start, TableEditor table, String columnName, String newColumnName) {
        // Obtain the column editor ...
        Column existingColumn = table.columnWithName(columnName);
        ColumnEditor column = existingColumn != null ? existingColumn.edit() : Column.editor().name(columnName);
        AtomicBoolean isPrimaryKey = new AtomicBoolean(false);

        parseColumnDefinition(start, columnName, tokens, table, column, isPrimaryKey);

        // Update the table ...
        Column newColumnDefn = column.create();
        table.addColumns(newColumnDefn);
        if (isPrimaryKey.get()) {
            table.setPrimaryKeyNames(newColumnDefn.name());
        }

        if (newColumnName != null && !newColumnName.equalsIgnoreCase(columnName)) {
            table.renameColumn(columnName, newColumnName);
            columnName = newColumnName;
        }

        // ALTER TABLE allows reordering the columns after the definition ...
        if (tokens.canConsume("FIRST")) {
            table.reorderColumn(columnName, null);
        } else if (tokens.canConsume("AFTER")) {
            table.reorderColumn(columnName, tokens.consume());
        }
        return table.columnWithName(newColumnDefn.name());
    }

    /**
     * Parse the {@code ENUM} or {@code SET} data type expression to extract the character options, where the index(es) appearing
     * in the {@code ENUM} or {@code SET} values can be used to identify the acceptable characters.
     *
     * @param typeExpression the data type expression
     * @return the string containing the character options allowed by the {@code ENUM} or {@code SET}; never null
     */
    public static List<String> parseSetAndEnumOptions(String typeExpression) {
        List<String> options = new ArrayList<>();
        TokenStream tokens = new TokenStream(typeExpression, TokenStream.basicTokenizer(false), false);
        tokens.start();
        if (tokens.canConsumeAnyOf("ENUM", "SET") && tokens.canConsume('(')) {
            // The literals should be quoted values ...
            if (tokens.matchesAnyOf(DdlTokenizer.DOUBLE_QUOTED_STRING, DdlTokenizer.SINGLE_QUOTED_STRING)) {
                options.add(withoutQuotes(tokens.consume()));
                while (tokens.canConsume(',')) {
                    if (tokens.matchesAnyOf(DdlTokenizer.DOUBLE_QUOTED_STRING, DdlTokenizer.SINGLE_QUOTED_STRING)) {
                        options.add(withoutQuotes(tokens.consume()));
                    }
                }
            }
            tokens.consume(')');
        }
        return options;
    }

    protected static String withoutQuotes(String possiblyQuoted) {
        if (possiblyQuoted.length() < 2) {
            // Too short to be quoted ...
            return possiblyQuoted;
        }
        if (possiblyQuoted.startsWith("'") && possiblyQuoted.endsWith("'")) {
            return possiblyQuoted.substring(1, possiblyQuoted.length() - 1);
        }
        if (possiblyQuoted.startsWith("\"") && possiblyQuoted.endsWith("\"")) {
            return possiblyQuoted.substring(1, possiblyQuoted.length() - 1);
        }
        return possiblyQuoted;
    }

    protected void parseColumnDefinition(Marker start, String columnName, TokenStream tokens, TableEditor table, ColumnEditor column,
                                         AtomicBoolean isPrimaryKey) {
        // Parse the data type, which must be at this location ...
        List<ParsingException> errors = new ArrayList<>();
        Marker dataTypeStart = tokens.mark();
        DataType dataType = dataTypeParser.parse(tokens, errors::addAll);
        if (dataType == null) {
            String dataTypeName = parseDomainName(start);
            if (dataTypeName != null) dataType = DataType.userDefinedType(dataTypeName);
        }
        if (dataType == null) {
            // No data type was found
            parsingFailed(dataTypeStart.position(), errors, "Unable to read the data type");
            return;
        }
        column.jdbcType(dataType.jdbcType());
        column.type(dataType.name(), dataType.expression());
        if ("ENUM".equals(dataType.name())) {
            column.length(1);
        } else if ("SET".equals(dataType.name())) {
            List<String> options = parseSetAndEnumOptions(dataType.expression());
            // After DBZ-132, it will always be comma seperated
            column.length(Math.max(0, options.size() * 2 - 1)); // number of options + number of commas
        } else {
            if (dataType.length() > -1) column.length((int) dataType.length());
            if (dataType.scale() > -1) column.scale(dataType.scale());
        }

        if (Types.NCHAR == dataType.jdbcType() || Types.NVARCHAR == dataType.jdbcType()) {
            // NCHAR and NVARCHAR columns always uses utf8 as charset
            column.charsetName("utf8");
        }

        if (Types.DECIMAL == dataType.jdbcType()) {
            if (dataType.length() == -1) {
                column.length(10);
            }
            if (dataType.scale() == -1) {
                column.scale(0);
            }
        }

        if (tokens.canConsume("CHARSET") || tokens.canConsume("CHARACTER", "SET")) {
            String charsetName = tokens.consume();
            if (!"DEFAULT".equalsIgnoreCase(charsetName)) {
                // Only record it if not inheriting the character set from the table
                column.charsetName(charsetName);
            }
        }
        if (tokens.canConsume("COLLATE")) {
            tokens.consume(); // name of collation
        }

        if (tokens.canConsume("AS") || tokens.canConsume("GENERATED", "ALWAYS", "AS")) {
            consumeExpression(start);
            tokens.canConsumeAnyOf("VIRTUAL", "STORED");
            if (tokens.canConsume("UNIQUE")) {
                tokens.canConsume("KEY");
            }
            if (tokens.canConsume("COMMENT")) {
                consumeQuotedString();
            }
            tokens.canConsume("NOT", "NULL");
            tokens.canConsume("NULL");
            tokens.canConsume("PRIMARY", "KEY");
            tokens.canConsume("KEY");
        } else {
            while (tokens.matchesAnyOf("NOT", "NULL", "DEFAULT", "AUTO_INCREMENT", "UNIQUE", "PRIMARY", "KEY", "COMMENT",
                                       "REFERENCES", "COLUMN_FORMAT", "ON", "COLLATE")) {
                // Nullability ...
                if (tokens.canConsume("NOT", "NULL")) {
                    column.optional(false);
                } else if (tokens.canConsume("NULL")) {
                    column.optional(true);
                }
                // Default value ...
                if (tokens.matches("DEFAULT")) {
                    parseDefaultClause(start);
                }
                if (tokens.matches("ON", "UPDATE") || tokens.matches("ON", "DELETE")) {
                    parseOnUpdateOrDelete(tokens.mark());
                    column.autoIncremented(true);
                }
                // Other options ...
                if (tokens.canConsume("AUTO_INCREMENT")) {
                    column.autoIncremented(true);
                    column.generated(true);
                }
                if (tokens.canConsume("UNIQUE", "KEY") || tokens.canConsume("UNIQUE")) {
                    if (table.primaryKeyColumnNames().isEmpty() && !column.isOptional()) {
                        // The table has no primary key (yet) but this is a non-null column and therefore will have all unique
                        // values (MySQL allows UNIQUE indexes with some nullable columns, but in that case allows duplicate
                        // rows),
                        // so go ahead and set it to this column as it's a unique key
                        isPrimaryKey.set(true);
                    }
                }
                if (tokens.canConsume("PRIMARY", "KEY") || tokens.canConsume("KEY")) {
                    // Always set this column as the primary key
                    column.optional(false); // MySQL primary key columns may not be null
                    isPrimaryKey.set(true);
                }
                if (tokens.canConsume("COMMENT")) {
                    consumeQuotedString();
                }
                if (tokens.canConsume("COLUMN_FORMAT")) {
                    tokens.consumeAnyOf("FIXED", "DYNAMIC", "DEFAULT");
                }
                if (tokens.matches("REFERENCES")) {
                    parseReferenceDefinition(start);
                }
                if (tokens.canConsume("COLLATE")) {
                    tokens.consume(); // name of collation
                }
            }
        }
    }

    protected String parseDomainName(Marker start) {
        return parseSchemaQualifiedName(start);
    }

    protected List<String> parseIndexColumnNames(Marker start) {
        List<String> names = new ArrayList<>();
        tokens.consume('(');
        parseIndexColumnName(names::add);
        while (tokens.canConsume(',')) {
            parseIndexColumnName(names::add);
        }
        tokens.consume(')');
        return names;
    }

    private void parseIndexColumnName(Consumer<String> name) {
        name.accept(parseColumnName());
        if (tokens.canConsume('(')) {
            tokens.consume(); // length
            tokens.consume(')');
        }
        tokens.canConsumeAnyOf("ASC", "DESC");
    }

    protected void parseIndexType(Marker start) {
        tokens.consume("USING");
        tokens.consumeAnyOf("BTREE", "HASH");
    }

    protected void parseIndexOptions(Marker start) {
        while (true) {
            if (tokens.matches("USING")) {
                parseIndexType(start);
            } else if (tokens.canConsume("COMMENT")) {
                consumeQuotedString();
            } else if (tokens.canConsume("KEY_BLOCK_SIZE")) {
                tokens.consume("=");
                tokens.consume();
            } else if (tokens.canConsume("WITH", "PARSER")) {
                tokens.consume();
            } else {
                break;
            }
        }
    }

    protected void parseReferenceDefinition(Marker start) {
        tokens.consume("REFERENCES");
        parseSchemaQualifiedName(start); // table name
        if (tokens.matches('(')) {
            parseColumnNameList(start);
        }
        if (tokens.canConsume("MATCH")) {
            tokens.consumeAnyOf("FULL", "PARTIAL", "SIMPLE");
        }
        if (tokens.canConsume("ON", "DELETE")) {
            parseReferenceOption(start);
        }
        if (tokens.canConsume("ON", "UPDATE")) {
            parseReferenceOption(start);
        }
        if (tokens.canConsume("ON", "DELETE")) { // in case ON UPDATE is first
            parseReferenceOption(start);
        }
    }

    protected void parseReferenceOption(Marker start) {
        if (tokens.canConsume("RESTRICT")) {} else if (tokens.canConsume("CASCADE")) {} else if (tokens.canConsume("SET", "NULL")) {} else {
            tokens.canConsume("NO", "ACTION");
        }
    }

    protected void parseCreateView(Marker start) {
        tokens.canConsume("OR", "REPLACE");
        if (tokens.canConsume("ALGORITHM")) {
            tokens.consume('=');
            tokens.consumeAnyOf("UNDEFINED", "MERGE", "TEMPTABLE");
        }
        parseDefiner(tokens.mark());
        if (tokens.canConsume("SQL", "SECURITY")) {
            tokens.consumeAnyOf("DEFINER", "INVOKER");
        }
        tokens.consume("VIEW");
        TableId tableId = parseQualifiedTableName(start);
        if (skipViews) {
            // We don't care about the rest ...
            consumeRemainingStatement(start);
            signalCreateView(tableId, start);
            debugSkipped(start);
            return;
        }

        TableEditor table = databaseTables.editOrCreateTable(tableId);
        if (tokens.matches('(')) {
            List<String> columnNames = parseColumnNameList(start);
            // We know nothing other than the names ...
            columnNames.forEach(name -> {
                table.addColumn(Column.editor().name(name).create());
            });
        }
        tokens.canConsume("AS");

        // We should try to discover the types of the columns by looking at this select
        if (tokens.canConsume("SELECT")) {
            // If the SELECT clause is selecting qualified column names or columns names from a single table, then
            // we can look up the columns and use those to set the type and nullability of the view's columns ...
            Map<String, Column> selectedColumnsByAlias = parseColumnsInSelectClause(start);
            if (table.columns().isEmpty()) {
                selectedColumnsByAlias.forEach((columnName, fromTableColumn) -> {
                    if (fromTableColumn != null && columnName != null) table.addColumn(fromTableColumn.edit().name(columnName).create());
                });
            } else {
                List<Column> changedColumns = new ArrayList<>();
                table.columns().forEach(column -> {
                    // Find the column from the SELECT statement defining the view ...
                    Column selectedColumn = selectedColumnsByAlias.get(column.name());
                    if (selectedColumn != null) {
                        changedColumns.add(column.edit()
                                                 .jdbcType(selectedColumn.jdbcType())
                                                 .type(selectedColumn.typeName(), selectedColumn.typeExpression())
                                                 .length(selectedColumn.length())
                                                 .scale(selectedColumn.scale())
                                                 .autoIncremented(selectedColumn.isAutoIncremented())
                                                 .generated(selectedColumn.isGenerated())
                                                 .optional(selectedColumn.isOptional()).create());
                    }
                });
                changedColumns.forEach(table::addColumn);
            }

            // Parse the FROM clause to see if the view is only referencing a single table, and if so then update the view
            // with an equivalent primary key ...
            Map<String, Table> fromTables = parseSelectFromClause(start);
            if (fromTables.size() == 1) {
                Table fromTable = fromTables.values().stream().findFirst().get();
                List<String> fromTablePkColumnNames = fromTable.columnNames();
                List<String> viewPkColumnNames = new ArrayList<>();
                selectedColumnsByAlias.forEach((viewColumnName, fromTableColumn) -> {
                    if (fromTablePkColumnNames.contains(fromTableColumn.name())) {
                        viewPkColumnNames.add(viewColumnName);
                    }
                });
                if (viewPkColumnNames.size() == fromTablePkColumnNames.size()) {
                    table.setPrimaryKeyNames(viewPkColumnNames);
                }
            }
        }

        // We don't care about the rest ...
        consumeRemainingStatement(start);

        // Update the table definition ...
        databaseTables.overwriteTable(table.create());

        signalCreateView(tableId, start);
        debugParsed(start);
    }

    protected void parseCreateIndex(Marker start) {
        boolean unique = tokens.canConsume("UNIQUE");
        tokens.canConsumeAnyOf("FULLTEXT", "SPATIAL");
        tokens.consume("INDEX");
        String indexName = tokens.consume(); // index name
        if (tokens.matches("USING")) {
            parseIndexType(start);
        }
        TableId tableId = null;
        if (tokens.canConsume("ON")) {
            // Usually this is required, but in some cases ON is not required
            tableId = parseQualifiedTableName(start);
        }

        if (unique && tableId != null) {
            // This is a unique index, and we can mark the index's columns as the primary key iff there is not already
            // a primary key on the table. (Should a PK be created later via an alter, then it will overwrite this.)
            TableEditor table = databaseTables.editTable(tableId);
            if (table != null && !table.hasPrimaryKey()) {
                List<String> names = parseIndexColumnNames(start);
                if (table.columns().stream().allMatch(Column::isRequired)) {
                    databaseTables.overwriteTable(table.setPrimaryKeyNames(names).create());
                }
            }
        }

        // We don't care about any other statements or the rest of this statement ...
        consumeRemainingStatement(start);
        signalCreateIndex(indexName, tableId, start);
        debugParsed(start);
    }

    protected void parseDefiner(Marker start) {
        if (tokens.canConsume("DEFINER")) {
            tokens.consume('=');
            tokens.consume(); // user or CURRENT_USER
            if (tokens.canConsume("@")) {
                tokens.consume(); // host
            } else {
                String next = tokens.peek();
                if (next.startsWith("@")) { // e.g., @`localhost`
                    tokens.consume();
                }
            }
        }
    }

    protected void parseCreateProcedure(Marker start) {
        parseDefiner(tokens.mark());
        tokens.consumeAnyOf("FUNCTION", "PROCEDURE");
        tokens.consume(); // name
        consumeRemainingStatement(start);
    }

    protected void parseCreateTrigger(Marker start) {
        parseDefiner(tokens.mark());
        tokens.consume("TRIGGER");
        tokens.consume(); // name
        consumeRemainingStatement(start);
    }

    protected void parseCreateEvent(Marker start) {
        parseDefiner(tokens.mark());
        tokens.consume("EVENT");
        tokens.consume(); // name
        consumeRemainingStatement(start);
    }

    protected void parseCreateUnknown(Marker start) {
        consumeRemainingStatement(start);
    }

    @Override
    protected void parseAlter(Marker marker) {
        tokens.consume("ALTER");
        if (tokens.matches("TABLE") || tokens.matches("IGNORE", "TABLE")) {
            parseAlterTable(marker);
            debugParsed(marker);
        } else if (tokens.matchesAnyOf("DATABASE", "SCHEMA")) {
            parseAlterDatabase(marker);
        } else {
            parseAlterUnknown(marker);
        }
    }

    protected void parseAlterTable(Marker start) {
        tokens.canConsume("IGNORE");
        tokens.consume("TABLE");
        TableId tableId = parseQualifiedTableName(start);
        TableEditor table = databaseTables.editTable(tableId);
        TableId oldTableId = null;
        if (table != null) {
            AtomicReference<TableId> newTableName = new AtomicReference<>(null);
            if (!tokens.matches(terminator()) && !tokens.matches("PARTITION")) {
                parseAlterSpecificationList(start, table, newTableName::set);
            }
            if (tokens.matches("PARTITION")) {
                parsePartitionOptions(start, table);
            }
            databaseTables.overwriteTable(table.create());
            if (newTableName.get() != null) {
                // the table was renamed ...
                Table renamed = databaseTables.renameTable(tableId, newTableName.get());
                if (renamed != null) {
                    oldTableId = tableId;
                    tableId = renamed.id();
                }
            }
        } else {
            Marker marker = tokens.mark();
            try {
                // We don't know about this table but we still have to parse the statement ...
                table = TableEditor.noOp(tableId);
                if (!tokens.matches(terminator()) && !tokens.matches("PARTITION")) {
                    parseAlterSpecificationList(start, table, str -> {
                    });
                }
                if (tokens.matches("PARTITION")) {
                    parsePartitionOptions(start, table);
                }
                parseTableOptions(start, table);
                // do nothing with this
            } catch (ParsingException e) {
                tokens.rewind(marker);
                consumeRemainingStatement(start);
            }
        }
        signalAlterTable(tableId, oldTableId, start);
    }

    protected void parseAlterSpecificationList(Marker start, TableEditor table, Consumer<TableId> newTableName) {
        parseAlterSpecification(start, table, newTableName);
        while (tokens.canConsume(',')) {
            parseAlterSpecification(start, table, newTableName);
        }
    }

    protected void parseAlterSpecification(Marker start, TableEditor table, Consumer<TableId> newTableName) {
        parseTableOptions(start, table);
        if (tokens.canConsume("ADD")) {
            if (tokens.matches("COLUMN", "(") || tokens.matches('(')) {
                tokens.canConsume("COLUMN");
                parseCreateDefinitionList(start, table);
            } else if (tokens.canConsume("PARTITION", "(")) {
                parsePartitionDefinition(start, table);
                tokens.consume(')');
            } else {
                parseCreateDefinition(start, table, true);
            }
        } else if (tokens.canConsume("DROP")) {
            if (tokens.canConsume("PRIMARY", "KEY")) {
                table.setPrimaryKeyNames();
            } else if (tokens.canConsume("FOREIGN", "KEY")) {
                tokens.consume(); // foreign key symbol
            } else if (tokens.canConsumeAnyOf("INDEX", "KEY")) {
                tokens.consume(); // index name
            } else if (tokens.canConsume("PARTITION")) {
                parsePartitionNames(start);
            } else {
                if(!isNextTokenQuotedIdentifier()) {
                    tokens.canConsume("COLUMN");
                }
                String columnName = parseColumnName();
                table.removeColumn(columnName);
                tokens.canConsume("RESTRICT");
            }
        } else if (tokens.canConsume("ALTER")) {
            if (!isNextTokenQuotedIdentifier()) {
                tokens.canConsume("COLUMN");
            }
            tokens.consume(); // column name
            if (!tokens.canConsume("DROP", "DEFAULT")) {
                tokens.consume("SET");
                parseDefaultClause(start);
            }
        } else if (tokens.canConsume("CHANGE")) {
            if (!isNextTokenQuotedIdentifier()) {
                tokens.canConsume("COLUMN");
            }
            String oldName = parseColumnName();
            String newName = parseColumnName();
            parseCreateColumn(start, table, oldName, newName);
        } else if (tokens.canConsume("MODIFY")) {
            if (!isNextTokenQuotedIdentifier()) {
                tokens.canConsume("COLUMN");
            }
            String columnName = parseColumnName();
            parseCreateColumn(start, table, columnName, null);
        } else if (tokens.canConsumeAnyOf("ALGORITHM", "LOCK")) {
            tokens.canConsume('=');
            tokens.consume();
        } else if (tokens.canConsume("DISABLE", "KEYS")
                || tokens.canConsume("ENABLE", "KEYS")) {} else if (tokens.canConsume("RENAME", "INDEX")
                        || tokens.canConsume("RENAME", "KEY")) {
            tokens.consume(); // old
            tokens.consume("TO");
            tokens.consume(); // new
        } else if (tokens.canConsume("RENAME")) {
            tokens.canConsumeAnyOf("AS", "TO");
            TableId newTableId = parseQualifiedTableName(start);
            newTableName.accept(newTableId);
        } else if (tokens.canConsume("ORDER", "BY")) {
            consumeCommaSeparatedValueList(start); // this should not affect the order of the columns in the table
        } else if (tokens.canConsume("CONVERT", "TO", "CHARACTER", "SET")
                || tokens.canConsume("CONVERT", "TO", "CHARSET")) {
            tokens.consume(); // charset name
            if (tokens.canConsume("COLLATE")) {
                tokens.consume(); // collation name
            }
        } else if (tokens.canConsume("CHARACTER", "SET")
                || tokens.canConsume("CHARSET")
                || tokens.canConsume("DEFAULT", "CHARACTER", "SET")
                || tokens.canConsume("DEFAULT", "CHARSET")) {
            tokens.canConsume('=');
            String charsetName = tokens.consume(); // charset name
            table.setDefaultCharsetName(charsetName);
            if (tokens.canConsume("COLLATE")) {
                tokens.canConsume('=');
                tokens.consume(); // collation name (ignored)
            }
        } else if (tokens.canConsume("DISCARD", "TABLESPACE") || tokens.canConsume("IMPORT", "TABLESPACE")) {
            // nothing
        } else if (tokens.canConsume("FORCE")) {
            // nothing
        } else if (tokens.canConsume("WITH", "VALIDATION") || tokens.canConsume("WITHOUT", "VALIDATION")) {
            // nothing
        } else if (tokens.canConsume("DISCARD", "PARTITION") || tokens.canConsume("IMPORT", "PARTITION")) {
            if (!tokens.canConsume("ALL")) {
                tokens.consume(); // partition name
            }
            tokens.consume("TABLESPACE");
        } else if (tokens.canConsume("COALLESCE", "PARTITION")) {
            tokens.consume(); // number
        } else if (tokens.canConsume("REORGANIZE", "PARTITION")) {
            parsePartitionNames(start);
            tokens.consume("INTO", "(");
            do {
                parsePartitionDefinition(start, table);
            } while (tokens.canConsume(','));
            tokens.consume(')');
        } else if (tokens.canConsume("EXCHANGE", "PARTITION")) {
            tokens.consume(); // partition name
            tokens.consume("WITH", "TABLE");
            parseSchemaQualifiedName(start); // table name
            if (tokens.canConsumeAnyOf("WITH", "WITHOUT")) {
                tokens.consume("VALIDATION");
            }
        } else if (tokens.matches(TokenStream.ANY_VALUE, "PARTITION")) {
            tokens.consumeAnyOf("TRUNCATE", "CHECK", "ANALYZE", "OPTIMIZE", "REBUILD", "REPAIR");
            tokens.consume("PARTITION");
            if (!tokens.canConsume("ALL")) {
                parsePartitionNames(start);
            }
        } else if (tokens.canConsume("REMOVE", "PARTITIONING")) {
            // nothing
        } else if (tokens.canConsume("UPGRADE", "PARTITIONING")) {
            // nothing
        }
    }

    protected void parseAlterUnknown(Marker start) {
        consumeRemainingStatement(start);
        debugSkipped(start);
    }

    @Override
    protected void parseDrop(Marker marker) {
        tokens.consume("DROP");
        if (tokens.matches("TABLE") || tokens.matches("TEMPORARY", "TABLE")) {
            parseDropTable(marker);
        } else if (tokens.matches("VIEW")) {
            parseDropView(marker);
        } else if (tokens.matches("INDEX")) {
            parseDropIndex(marker);
        } else if (tokens.matchesAnyOf("DATABASE", "SCHEMA")) {
            parseDropDatabase(marker);
        } else {
            parseDropUnknown(marker);
        }
    }

    protected void parseDropTable(Marker start) {
        tokens.canConsume("TEMPORARY");
        tokens.consume("TABLE");
        tokens.canConsume("IF", "EXISTS");
        String statementPrefix = statement(start);
        List<TableId> ids = parseQualifiedTableNames(start);
        boolean restrict = tokens.canConsume("RESTRICT");
        boolean cascade = tokens.canConsume("CASCADE");
        ids.forEach(tableId -> {
            databaseTables.removeTable(tableId);
            signalDropTable(tableId, statementPrefix + tableId + (restrict ? " RESTRICT" : cascade ? " CASCADE" : ""));
        });
        debugParsed(start);
    }

    protected void parseDropView(Marker start) {
        if (skipViews) {
            consumeRemainingStatement(start);
            debugSkipped(start);
            return;
        }
        tokens.consume("VIEW");
        tokens.canConsume("IF", "EXISTS");
        String statementPrefix = statement(start);
        List<TableId> ids = parseQualifiedTableNames(start);
        boolean restrict = tokens.canConsume("RESTRICT");
        boolean cascade = tokens.canConsume("CASCADE");
        ids.forEach(tableId -> {
            databaseTables.removeTable(tableId);
            signalDropView(tableId, statementPrefix + tableId + (restrict ? " RESTRICT" : cascade ? " CASCADE" : ""));
        });
        debugParsed(start);
    }

    protected void parseDropIndex(Marker start) {
        tokens.consume("INDEX");
        String indexName = tokens.consume(); // index name
        tokens.consume("ON");
        TableId tableId = parseQualifiedTableName(start);
        consumeRemainingStatement(start);
        signalDropIndex(indexName, tableId, start);
        debugParsed(start);
    }

    protected void parseDropUnknown(Marker start) {
        consumeRemainingStatement(start);
        debugSkipped(start);
    }

    protected void parseRename(Marker start) {
        tokens.consume("RENAME");
        if (tokens.canConsume("TABLE")) {
            parseRenameTable(start);
            while (tokens.canConsume(',')) {
                parseRenameTable(start);
            }
        } else if (tokens.canConsumeAnyOf("DATABASE", "SCHEMA", "USER")) {
            // See https://dev.mysql.com/doc/refman/5.1/en/rename-database.html
            consumeRemainingStatement(start);
        }
    }

    protected void parseRenameTable(Marker start) {
        TableId from = parseQualifiedTableName(start);
        tokens.consume("TO");
        TableId to = parseQualifiedTableName(start);
        databaseTables.renameTable(from, to);
        // Signal a separate statement for this table rename action, even though multiple renames might be
        // performed by a single DDL statement on the token stream ...
        signalAlterTable(from, to, "RENAME TABLE " + from + " TO " + to);
    }

    protected void parseUse(Marker marker) {
        tokens.consume("USE");
        String dbName = tokens.consume();
        setCurrentSchema(dbName);

        // Every time MySQL switches to a different database, it sets the "character_set_database" and "collation_database"
        // system variables. We replicate that behavior here (or the variable we care about) so that these variables are always
        // right for the current database.
        String charsetForDb = charsetNameForDatabase.get(dbName);
        systemVariables.setVariable(MySqlSystemVariables.Scope.GLOBAL, "character_set_database", charsetForDb);
    }

    /**
     * Get the name of the character set for the current database, via the "character_set_database" system property.
     *
     * @return the name of the character set for the current database, or null if not known ...
     */
    protected String currentDatabaseCharset() {
        String charsetName = systemVariables.getVariable("character_set_database");
        if (charsetName == null || "DEFAULT".equalsIgnoreCase(charsetName)) {
            charsetName = systemVariables.getVariable(SERVER_CHARSET_NAME);
        }
        return charsetName;
    }

    protected List<String> parseColumnNameList(Marker start) {
        List<String> names = new ArrayList<>();
        tokens.consume('(');
        names.add(parseColumnName());
        while (tokens.canConsume(',')) {
            names.add(parseColumnName());
        }
        tokens.consume(')');
        return names;
    }

    protected String parseColumnName() {
        boolean quoted = isNextTokenQuotedIdentifier();
        String name = tokens.consume();
        if (!quoted) {
            // Unquoted names may not consist entirely of digits
            if (name.matches("[0-9]+")) {
                parsingFailed(tokens.previousPosition(), "Unquoted column names may not contain only digits");
                return null;
            }
        }
        return name;
    }

    protected void parsePartitionNames(Marker start) {
        consumeCommaSeparatedValueList(start);
    }

    protected void consumeCommaSeparatedValueList(Marker start) {
        tokens.consume();
        while (tokens.canConsume(',')) {
            tokens.consume();
        }
    }

    protected void consumeValueList(Marker start) {
        tokens.consume('(');
        consumeCommaSeparatedValueList(start);
        tokens.consume(')');
    }

    /**
     * Consume an expression surrounded by parentheses.
     *
     * @param start the start of the statement
     */
    protected void consumeExpression(Marker start) {
        tokens.consume("(");
        tokens.consumeThrough(')', '(');
    }

    /**
     * Consume the entire {@code BEGIN...END} block that appears next in the token stream. This handles nested
     * <a href="https://dev.mysql.com/doc/refman/5.7/en/begin-end.html"><code>BEGIN...END</code> blocks</a>,
     * <a href="https://dev.mysql.com/doc/refman/5.7/en/statement-labels.html">labeled statements</a>,
     * and control blocks.
     *
     * @param start the marker at which the statement was begun
     */
    @Override
    protected void consumeBeginStatement(Marker start) {
        tokens.consume("BEGIN");
        // Look for a label that preceded the BEGIN ...
        LinkedList<String> labels = new LinkedList<>();
        labels.addFirst(getPrecedingBlockLabel());

        int expectedPlainEnds = 0;

        // Now look for the "END", ignoring intermediate control blocks that also use "END" ...
        while (tokens.hasNext()) {
            if (tokens.matchesWord("BEGIN")) {
                consumeBeginStatement(tokens.mark());
            }
            if (tokens.canConsumeWords("IF", "EXISTS")) {
                // Ignore any IF EXISTS phrases ...
            } else if (tokens.canConsumeWords("CASE", "WHEN")) {
                // This block can end with END without suffix
                expectedPlainEnds++;
            } else if (tokens.matchesAnyWordOf("REPEAT", "LOOP", "WHILE")) {
                // This block can contain label
                String label = getPrecedingBlockLabel();
                tokens.consume();
                labels.addFirst(label); // may be null
            } else if (tokens.canConsumeWord("END")) {
                if (tokens.matchesAnyOf("REPEAT", "LOOP", "WHILE")) {
                    // Read block label if set
                    tokens.consume();
                    String label = labels.remove();
                    if (label != null) tokens.canConsume(label);
                } else if (tokens.matchesAnyWordOf("IF", "CASE")) {
                    tokens.consume();
                } else if (expectedPlainEnds > 0) {
                    // There was a statement that will be ended with plain END
                    expectedPlainEnds--;
                } else {
                    break;
                }
            } else {
                tokens.consume();
            }
        }

        // We've consumed the corresponding END of the BEGIN, but consume the label if one was used ...
        assert labels.size() == 1;
        String label = labels.remove();
        if (label != null) tokens.canConsume(label);
    }

    /**
     * Get the label that appears with a colon character just prior to the current position. Some MySQL DDL statements can be
     * <a href="https://dev.mysql.com/doc/refman/5.7/en/statement-labels.html">labeled</a>, and this label can then appear at the
     * end of a block.
     *
     * @return the label for the block starting at the current position; null if there is no such label
     * @throws NoSuchElementException if there is no previous token
     */
    protected String getPrecedingBlockLabel() {
        if (tokens.previousToken(1).matches(':')) {
            // A label preceded the beginning of the control block ...
            return tokens.previousToken(2).value();
        }
        return null;
    }

    /**
     * Try calling the supplied functions in sequence, stopping as soon as one of them succeeds.
     *
     * @param functions the functions
     */
    @SuppressWarnings("unchecked")
    protected void sequentially(Consumer<Marker>... functions) {
        if (functions == null || functions.length == 0) return;
        Collection<ParsingException> errors = new ArrayList<>();
        Marker marker = tokens.mark();
        for (Consumer<Marker> function : functions) {
            try {
                function.accept(marker);
                return;
            } catch (ParsingException e) {
                errors.add(e);
                tokens.rewind(marker);
            }
        }
        parsingFailed(marker.position(), errors, "One or more errors trying to parse statement");
    }

    /**
     * Parse and consume the {@code DEFAULT} clause. Currently, this method does not capture the default in any way,
     * since that will likely require parsing the default clause into a useful value (e.g., dealing with hexadecimals,
     * bit-set literals, date-time literals, etc.).
     *
     * @param start the marker at the beginning of the clause
     */
    protected void parseDefaultClause(Marker start) {
        tokens.consume("DEFAULT");
        if (isNextTokenQuotedIdentifier()) {
            // We know that it is a quoted literal ...
            parseLiteral(start);
        } else {
            if (tokens.matchesAnyOf("CURRENT_TIMESTAMP", "NOW")) {
                parseCurrentTimestampOrNow();
                parseOnUpdateOrDelete(tokens.mark());
            } else if (tokens.canConsume("NULL")) {
                // do nothing ...
            } else {
                parseLiteral(start);
            }
        }
    }

    protected void parseOnUpdateOrDelete(Marker start) {
        if (tokens.canConsume("ON") && tokens.canConsumeAnyOf("UPDATE", "DELETE")) {
            parseCurrentTimestampOrNow();
        }
    }

    private void parseCurrentTimestampOrNow() {
        tokens.consumeAnyOf("CURRENT_TIMESTAMP", "NOW");
        if (tokens.canConsume('(')) {
            if (!tokens.canConsume(')')) {
                tokens.consumeInteger();
                tokens.consume(')');
            }
        }
    }
}
