/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
     * Pattern to grab the list of single-quoted options within a SET or ENUM type definition.
     */
    private static final Pattern ENUM_AND_SET_LITERALS = Pattern.compile("(ENUM|SET)\\s*[(]([^)]*)[)].*");

    /**
     * Pattern to extract the option characters from the comma-separated list of single-quoted options.
     */
    private static final Pattern ENUM_AND_SET_OPTIONS = Pattern.compile("'([^']*)'");

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
        dataTypes.register(Types.FLOAT, "FLOAT[(M[,D])] [UNSIGNED|SIGNED] [ZEROFILL]");
        dataTypes.register(Types.DECIMAL, "DECIMAL[(M[,D])] [UNSIGNED|SIGNED] [ZEROFILL]");
        dataTypes.register(Types.NUMERIC, "NUMERIC[(M[,D])] [UNSIGNED|SIGNED] [ZEROFILL]");
        dataTypes.register(Types.BOOLEAN, "BOOLEAN");
        dataTypes.register(Types.BOOLEAN, "BOOL");
        dataTypes.register(Types.DATE, "DATE");
        dataTypes.register(Types.TIME, "TIME[(L)]");
        dataTypes.register(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP[(L)]"); // includes timezone information
        dataTypes.register(Types.TIMESTAMP, "DATETIME[(L)]");
        dataTypes.register(Types.INTEGER, "YEAR[(2|4)]");
        dataTypes.register(Types.BLOB, "CHAR[(L)] BINARY");
        dataTypes.register(Types.BLOB, "VARCHAR(L) BINARY");
        dataTypes.register(Types.CHAR, "CHAR[(L)]");
        dataTypes.register(Types.VARCHAR, "VARCHAR(L)");
        dataTypes.register(Types.CHAR, "BINARY[(L)]");
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
        dataTypes.register(Types.VARCHAR, "TEXT");
        dataTypes.register(Types.VARCHAR, "MEDIUMTEXT");
        dataTypes.register(Types.VARCHAR, "LONGTEXT");
        dataTypes.register(Types.CHAR, "ENUM(...)");
        dataTypes.register(Types.CHAR, "SET(...)");
        dataTypes.register(Types.OTHER, "JSON");
    }

    @Override
    protected void initializeKeywords(TokenSet keywords) {
    }

    @Override
    protected void initializeStatementStarts(TokenSet statementStartTokens) {
        statementStartTokens.add("CREATE", "ALTER", "DROP", "INSERT", "GRANT", "REVOKE", "FLUSH", "TRUNCATE", "COMMIT", "USE");
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
                signalEvent(new SetVariableEvent(variableName,value,statement(start)));
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
        if ( tokens.canConsumeAnyOf(",",";")) {
            // The variable is blank ...
            return "";
        }
        Marker start = tokens.mark();
        tokens.consumeUntilEndOrOneOf(",", ";");
        String value = tokens.getContentFrom(start);
        if ( value.startsWith("'") && value.endsWith("'")) {
            // Remove the single quotes around the value ...
            if ( value.length() <= 2 ) {
                value = "";
            } else {
                value = value.substring(1, value.length()-2);
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
            parseCreateUnknown(marker);
        } else if (tokens.matchesAnyOf("FUNCTION", "PROCEDURE")) {
            parseCreateUnknown(marker);
        } else if (tokens.matchesAnyOf("UNIQUE", "FULLTEXT", "SPATIAL", "INDEX")) {
            parseCreateIndex(marker);
        } else if (tokens.matchesAnyOf("SERVER")) {
            parseCreateUnknown(marker);
        } else if (tokens.matchesAnyOf("TABLESPACE")) {
            parseCreateUnknown(marker);
        } else if (tokens.matchesAnyOf("TRIGGER")) {
            parseCreateUnknown(marker);
        } else {
            // It could be several possible things (including more elaborate forms of those matches tried above),
            sequentially(this::parseCreateView,
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
                                          "STATS_AUTO_RECALC", "STATS_PERSISTENT", "STATS_SAMPLE_PAGES")) {
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
            tokens.consume();
            while (tokens.canConsume(',')) {
                tokens.consume();
            }
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
        parseCreateDefinition(start, table);
        while (tokens.canConsume(',')) {
            parseCreateDefinition(start, table);
        }
        tokens.consume(')');
    }

    protected void parseCreateDefinition(Marker start, TableEditor table) {
        // If the first token is a quoted identifier, then we know it is a column name ...
        boolean quoted = isNextTokenQuotedIdentifier();

        // Try to parse the constraints first ...
        if (!quoted && tokens.canConsume("CHECK")) {
            consumeExpression(start);
        } else if (!quoted && tokens.canConsume("CONSTRAINT", TokenStream.ANY_VALUE, "PRIMARY", "KEY")
                || tokens.canConsume("PRIMARY", "KEY")) {
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
                if (c.isOptional()) {
                    table.addColumn(c.edit().optional(false).create());
                }
            });
        } else if (!quoted && tokens.canConsume("CONSTRAINT", TokenStream.ANY_VALUE, "UNIQUE") || tokens.canConsume("UNIQUE")) {
            tokens.canConsumeAnyOf("KEY", "INDEX");
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
        } else if (!quoted && tokens.canConsume("CONSTRAINT", TokenStream.ANY_VALUE, "FOREIGN", "KEY")
                || tokens.canConsume("FOREIGN", "KEY")) {
            if (!tokens.matches('(')) {
                tokens.consume(); // name of foreign key
            }
            parseIndexColumnNames(start);
            if (tokens.matches("REFERENCES")) {
                parseReferenceDefinition(start);
            }
        } else if (!quoted && tokens.canConsumeAnyOf("INDEX", "KEY")) {
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
        } else if (!quoted && tokens.canConsume("FULLTEXT", "SPATIAL")) {
            tokens.canConsumeAnyOf("INDEX", "KEY");
            if (!tokens.matches('(')) {
                tokens.consume(); // name of unique index ...
            }
            parseIndexColumnNames(start);
            parseIndexOptions(start);
        } else {
            tokens.canConsume("COLUMN"); // optional

            // Obtain the column editor ...
            String columnName = tokens.consume();
            parseCreateColumn(start, table, columnName);

            // ALTER TABLE allows reordering the columns after the definition ...
            if (tokens.canConsume("FIRST")) {
                table.reorderColumn(columnName, null);
            } else if (tokens.canConsume("AFTER")) {
                table.reorderColumn(columnName, tokens.consume());
            }
        }
    }

    protected Column parseCreateColumn(Marker start, TableEditor table, String columnName) {
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
        Matcher matcher = ENUM_AND_SET_LITERALS.matcher(typeExpression);
        List<String> options = new ArrayList<>();
        if (matcher.matches()) {
            String literals = matcher.group(2);
            Matcher optionMatcher = ENUM_AND_SET_OPTIONS.matcher(literals);
            while (optionMatcher.find()) {
                String option = optionMatcher.group(1);
                if (option != null && option.length() > 0) {
                    options.add(option);
                }
            }
        }
        return options;
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
            //After DBZ-132, it will always be comma seperated
            column.length(Math.max(0, options.size() * 2 - 1)); // number of options + number of commas
        } else {
            if (dataType.length() > -1) column.length((int) dataType.length());
            if (dataType.scale() > -1) column.scale(dataType.scale());
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
                                       "REFERENCES", "COLUMN_FORMAT", "ON")) {
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
                if (tokens.canConsume("ON")) {
                    if (tokens.canConsumeAnyOf("UPDATE", "DELETE")) {
                        tokens.consume(); // e.g., "ON UPATE CURRENT_TIMESTAMP"
                    }
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
        name.accept(tokens.consume());
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
        if (tokens.canConsume("DEFINER")) {
            tokens.consume('=');
            tokens.consume(); // user or CURRENT_USER
        }
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
                    if (fromTablePkColumnNames.contains(fromTableColumn)) {
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
            // We don't know about this table ...
            consumeRemainingStatement(start);
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
                parseCreateDefinition(start, table);
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
                tokens.canConsume("COLUMN");
                String columnName = tokens.consume();
                table.removeColumn(columnName);
            }
        } else if (tokens.canConsume("ALTER")) {
            tokens.canConsume("COLUMN");
            tokens.consume(); // column name
            if (!tokens.canConsume("DROP", "DEFAULT")) {
                tokens.consume("SET", "DEFAULT");
                parseDefaultClause(start);
            }
        } else if (tokens.canConsume("CHANGE")) {
            tokens.canConsume("COLUMN");
            String oldName = tokens.consume();
            String newName = tokens.consume();
            parseCreateColumn(start, table, oldName); // replaces the old definition but keeps old name
            table.renameColumn(oldName, newName);
            if (tokens.canConsume("FIRST")) {
                table.reorderColumn(newName, null);
            } else if (tokens.canConsume("AFTER")) {
                table.reorderColumn(newName, tokens.consume());
            }
        } else if (tokens.canConsume("MODIFY")) {
            tokens.canConsume("COLUMN");
            String columnName = tokens.consume();
            parseCreateColumn(start, table, columnName);
            if (tokens.canConsume("FIRST")) {
                table.reorderColumn(columnName, null);
            } else if (tokens.canConsume("AFTER")) {
                table.reorderColumn(columnName, tokens.consume());
            }
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
            parsePartitionDefinition(start, table);
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
        } else if (tokens.canConsumeAnyOf("DATABASE", "SCHEMA")) {
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
        names.add(tokens.consume());
        while (tokens.canConsume(',')) {
            names.add(tokens.consume());
        }
        tokens.consume(')');
        return names;
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
        parsingFailed(marker.position(), errors, "Unable to parse statement");
    }

    /**
     * Parse and consume the {@code DEFAULT} clause. Currently, this method does not capture the default in any way,
     * since that will likely require parsing the default clause into a useful value (e.g., dealing with hexadecimals,
     * bit-set literals, date-time literals, etc.).

     * @param start the marker at the beginning of the clause
     */
    protected void parseDefaultClause(Marker start) {
        tokens.consume("DEFAULT");
        if (isNextTokenQuotedIdentifier()) {
            // We know that it is a quoted literal ...
            parseLiteral(start);
        } else {
            if (tokens.canConsume("CURRENT_TIMESTAMP")) {
                if (tokens.canConsume('(')) {
                    tokens.consumeInteger();
                    tokens.consume(')');
                }
                tokens.canConsume("ON", "UPDATE", "CURRENT_TIMESTAMP");
                if (tokens.canConsume('(')) {
                    tokens.consumeInteger();
                    tokens.consume(')');
                }
            } else if (tokens.canConsume("NULL")) {
                // do nothing ...
            } else {
                parseLiteral(start);
            }
        }
    }
}
