/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.antlr;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;

import io.debezium.annotation.VisibleForTesting;
import io.debezium.antlr.AntlrDdlParser;
import io.debezium.antlr.AntlrDdlParserListener;
import io.debezium.antlr.DataTypeResolver;
import io.debezium.connector.binlog.charset.BinlogCharsetRegistry;
import io.debezium.connector.binlog.jdbc.BinlogSystemVariables;
import io.debezium.connector.mariadb.antlr.listener.MariaDbAntlrDdlParserListener;
import io.debezium.ddl.parser.mariadb.generated.MariaDBLexer;
import io.debezium.ddl.parser.mariadb.generated.MariaDBParser;
import io.debezium.ddl.parser.mariadb.generated.MariaDBParser.CharsetNameContext;
import io.debezium.ddl.parser.mariadb.generated.MariaDBParser.CollationNameContext;
import io.debezium.ddl.parser.mariadb.generated.MariaDBParser.RenameTableClauseContext;
import io.debezium.ddl.parser.mariadb.generated.MariaDBParser.RenameTableContext;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.SystemVariables;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

/**
 * An ANTLR based parser for MariaDB DDL statements.
 *
 * @author Chris Cranford
 */
public class MariaDbAntlrDdlParser extends AntlrDdlParser<MariaDBLexer, MariaDBParser> {

    private final ConcurrentHashMap<String, String> charsetNameForDatabase = new ConcurrentHashMap<>();
    private final Tables.TableFilter tableFilter;
    private final BinlogCharsetRegistry charsetRegistry;

    @VisibleForTesting
    public MariaDbAntlrDdlParser() {
        this(Tables.TableFilter.includeAll());
    }

    @VisibleForTesting
    public MariaDbAntlrDdlParser(Tables.TableFilter tableFilter) {
        this(true, false, true, tableFilter, null);
    }

    public MariaDbAntlrDdlParser(boolean throwWerrorsFromTreeWalk, boolean includeViews, boolean includeComments,
                                 Tables.TableFilter tableFilter, BinlogCharsetRegistry charsetRegistry) {
        super(throwWerrorsFromTreeWalk, includeViews, includeComments);
        systemVariables = new BinlogSystemVariables();
        this.tableFilter = tableFilter;
        this.charsetRegistry = charsetRegistry;
    }

    @Override
    protected ParseTree parseTree(MariaDBParser parser) {
        return parser.root();
    }

    @Override
    protected AntlrDdlParserListener createParseTreeWalkerListener() {
        return new MariaDbAntlrDdlParserListener(this);
    }

    @Override
    protected MariaDBLexer createNewLexerInstance(CharStream charStreams) {
        return new MariaDBLexer(charStreams);
    }

    @Override
    protected MariaDBParser createNewParserInstance(CommonTokenStream commonTokenStream) {
        return new MariaDBParser(commonTokenStream);
    }

    @Override
    protected SystemVariables createNewSystemVariablesInstance() {
        return new BinlogSystemVariables();
    }

    @Override
    protected boolean isGrammarInUpperCase() {
        return true;
    }

    @Override
    protected DataTypeResolver initializeDataTypeResolver() {
        DataTypeResolver.Builder dataTypeResolverBuilder = new DataTypeResolver.Builder();
        dataTypeResolverBuilder.registerDataTypes(MariaDBParser.StringDataTypeContext.class.getCanonicalName(), Arrays.asList(
                new DataTypeResolver.DataTypeEntry(Types.CHAR, MariaDBParser.CHAR),
                new DataTypeResolver.DataTypeEntry(Types.VARCHAR, MariaDBParser.CHAR, MariaDBParser.VARYING),
                new DataTypeResolver.DataTypeEntry(Types.VARCHAR, MariaDBParser.VARCHAR),
                new DataTypeResolver.DataTypeEntry(Types.VARCHAR, MariaDBParser.TINYTEXT),
                new DataTypeResolver.DataTypeEntry(Types.VARCHAR, MariaDBParser.TEXT),
                new DataTypeResolver.DataTypeEntry(Types.VARCHAR, MariaDBParser.MEDIUMTEXT),
                new DataTypeResolver.DataTypeEntry(Types.VARCHAR, MariaDBParser.LONGTEXT),
                new DataTypeResolver.DataTypeEntry(Types.VARCHAR, MariaDBParser.LONG),
                new DataTypeResolver.DataTypeEntry(Types.NCHAR, MariaDBParser.NCHAR),
                new DataTypeResolver.DataTypeEntry(Types.NVARCHAR, MariaDBParser.NCHAR, MariaDBParser.VARYING),
                new DataTypeResolver.DataTypeEntry(Types.NVARCHAR, MariaDBParser.NVARCHAR),
                new DataTypeResolver.DataTypeEntry(Types.CHAR, MariaDBParser.CHAR, MariaDBParser.BINARY),
                new DataTypeResolver.DataTypeEntry(Types.VARCHAR, MariaDBParser.VARCHAR, MariaDBParser.BINARY),
                new DataTypeResolver.DataTypeEntry(Types.VARCHAR, MariaDBParser.TINYTEXT, MariaDBParser.BINARY),
                new DataTypeResolver.DataTypeEntry(Types.VARCHAR, MariaDBParser.TEXT, MariaDBParser.BINARY),
                new DataTypeResolver.DataTypeEntry(Types.VARCHAR, MariaDBParser.MEDIUMTEXT, MariaDBParser.BINARY),
                new DataTypeResolver.DataTypeEntry(Types.VARCHAR, MariaDBParser.LONGTEXT, MariaDBParser.BINARY),
                new DataTypeResolver.DataTypeEntry(Types.NCHAR, MariaDBParser.NCHAR, MariaDBParser.BINARY),
                new DataTypeResolver.DataTypeEntry(Types.NVARCHAR, MariaDBParser.NVARCHAR, MariaDBParser.BINARY),
                new DataTypeResolver.DataTypeEntry(Types.CHAR, MariaDBParser.CHARACTER),
                new DataTypeResolver.DataTypeEntry(Types.VARCHAR, MariaDBParser.CHARACTER, MariaDBParser.VARYING)));

        dataTypeResolverBuilder.registerDataTypes(MariaDBParser.NationalStringDataTypeContext.class.getCanonicalName(), Arrays.asList(
                new DataTypeResolver.DataTypeEntry(Types.NVARCHAR, MariaDBParser.NATIONAL, MariaDBParser.VARCHAR).setSuffixTokens(MariaDBParser.BINARY),
                new DataTypeResolver.DataTypeEntry(Types.NCHAR, MariaDBParser.NATIONAL, MariaDBParser.CHARACTER).setSuffixTokens(MariaDBParser.BINARY),
                new DataTypeResolver.DataTypeEntry(Types.NCHAR, MariaDBParser.NATIONAL, MariaDBParser.CHAR).setSuffixTokens(MariaDBParser.BINARY),
                new DataTypeResolver.DataTypeEntry(Types.NVARCHAR, MariaDBParser.NCHAR, MariaDBParser.VARCHAR).setSuffixTokens(MariaDBParser.BINARY)));

        dataTypeResolverBuilder.registerDataTypes(MariaDBParser.NationalVaryingStringDataTypeContext.class.getCanonicalName(), Arrays.asList(
                new DataTypeResolver.DataTypeEntry(Types.NVARCHAR, MariaDBParser.NATIONAL, MariaDBParser.CHAR, MariaDBParser.VARYING),
                new DataTypeResolver.DataTypeEntry(Types.NVARCHAR, MariaDBParser.NATIONAL, MariaDBParser.CHARACTER, MariaDBParser.VARYING)));

        dataTypeResolverBuilder.registerDataTypes(MariaDBParser.DimensionDataTypeContext.class.getCanonicalName(), Arrays.asList(
                new DataTypeResolver.DataTypeEntry(Types.SMALLINT, MariaDBParser.TINYINT)
                        .setSuffixTokens(MariaDBParser.SIGNED, MariaDBParser.UNSIGNED, MariaDBParser.ZEROFILL),
                new DataTypeResolver.DataTypeEntry(Types.SMALLINT, MariaDBParser.INT1)
                        .setSuffixTokens(MariaDBParser.SIGNED, MariaDBParser.UNSIGNED, MariaDBParser.ZEROFILL),
                new DataTypeResolver.DataTypeEntry(Types.SMALLINT, MariaDBParser.SMALLINT)
                        .setSuffixTokens(MariaDBParser.SIGNED, MariaDBParser.UNSIGNED, MariaDBParser.ZEROFILL),
                new DataTypeResolver.DataTypeEntry(Types.SMALLINT, MariaDBParser.INT2)
                        .setSuffixTokens(MariaDBParser.SIGNED, MariaDBParser.UNSIGNED, MariaDBParser.ZEROFILL),
                new DataTypeResolver.DataTypeEntry(Types.INTEGER, MariaDBParser.MEDIUMINT)
                        .setSuffixTokens(MariaDBParser.SIGNED, MariaDBParser.UNSIGNED, MariaDBParser.ZEROFILL),
                new DataTypeResolver.DataTypeEntry(Types.INTEGER, MariaDBParser.INT3)
                        .setSuffixTokens(MariaDBParser.SIGNED, MariaDBParser.UNSIGNED, MariaDBParser.ZEROFILL),
                new DataTypeResolver.DataTypeEntry(Types.INTEGER, MariaDBParser.MIDDLEINT)
                        .setSuffixTokens(MariaDBParser.SIGNED, MariaDBParser.UNSIGNED, MariaDBParser.ZEROFILL),
                new DataTypeResolver.DataTypeEntry(Types.INTEGER, MariaDBParser.INT)
                        .setSuffixTokens(MariaDBParser.SIGNED, MariaDBParser.UNSIGNED, MariaDBParser.ZEROFILL),
                new DataTypeResolver.DataTypeEntry(Types.INTEGER, MariaDBParser.INTEGER)
                        .setSuffixTokens(MariaDBParser.SIGNED, MariaDBParser.UNSIGNED, MariaDBParser.ZEROFILL),
                new DataTypeResolver.DataTypeEntry(Types.INTEGER, MariaDBParser.INT4)
                        .setSuffixTokens(MariaDBParser.SIGNED, MariaDBParser.UNSIGNED, MariaDBParser.ZEROFILL),
                new DataTypeResolver.DataTypeEntry(Types.BIGINT, MariaDBParser.BIGINT)
                        .setSuffixTokens(MariaDBParser.SIGNED, MariaDBParser.UNSIGNED, MariaDBParser.ZEROFILL),
                new DataTypeResolver.DataTypeEntry(Types.BIGINT, MariaDBParser.INT8)
                        .setSuffixTokens(MariaDBParser.SIGNED, MariaDBParser.UNSIGNED, MariaDBParser.ZEROFILL),
                new DataTypeResolver.DataTypeEntry(Types.REAL, MariaDBParser.REAL)
                        .setSuffixTokens(MariaDBParser.SIGNED, MariaDBParser.UNSIGNED, MariaDBParser.ZEROFILL),
                new DataTypeResolver.DataTypeEntry(Types.DOUBLE, MariaDBParser.DOUBLE)
                        .setSuffixTokens(MariaDBParser.PRECISION, MariaDBParser.SIGNED, MariaDBParser.UNSIGNED, MariaDBParser.ZEROFILL),
                new DataTypeResolver.DataTypeEntry(Types.DOUBLE, MariaDBParser.FLOAT8)
                        .setSuffixTokens(MariaDBParser.PRECISION, MariaDBParser.SIGNED, MariaDBParser.UNSIGNED, MariaDBParser.ZEROFILL),
                new DataTypeResolver.DataTypeEntry(Types.FLOAT, MariaDBParser.FLOAT)
                        .setSuffixTokens(MariaDBParser.SIGNED, MariaDBParser.UNSIGNED, MariaDBParser.ZEROFILL),
                new DataTypeResolver.DataTypeEntry(Types.FLOAT, MariaDBParser.FLOAT4)
                        .setSuffixTokens(MariaDBParser.SIGNED, MariaDBParser.UNSIGNED, MariaDBParser.ZEROFILL),
                new DataTypeResolver.DataTypeEntry(Types.DECIMAL, MariaDBParser.DECIMAL)
                        .setSuffixTokens(MariaDBParser.SIGNED, MariaDBParser.UNSIGNED, MariaDBParser.ZEROFILL)
                        .setDefaultLengthScaleDimension(10, 0),
                new DataTypeResolver.DataTypeEntry(Types.DECIMAL, MariaDBParser.DEC)
                        .setSuffixTokens(MariaDBParser.SIGNED, MariaDBParser.UNSIGNED, MariaDBParser.ZEROFILL)
                        .setDefaultLengthScaleDimension(10, 0),
                new DataTypeResolver.DataTypeEntry(Types.DECIMAL, MariaDBParser.FIXED)
                        .setSuffixTokens(MariaDBParser.SIGNED, MariaDBParser.UNSIGNED, MariaDBParser.ZEROFILL)
                        .setDefaultLengthScaleDimension(10, 0),
                new DataTypeResolver.DataTypeEntry(Types.NUMERIC, MariaDBParser.NUMERIC)
                        .setSuffixTokens(MariaDBParser.SIGNED, MariaDBParser.UNSIGNED, MariaDBParser.ZEROFILL)
                        .setDefaultLengthScaleDimension(10, 0),
                new DataTypeResolver.DataTypeEntry(Types.BIT, MariaDBParser.BIT)
                        .setDefaultLengthDimension(1),
                new DataTypeResolver.DataTypeEntry(Types.TIME, MariaDBParser.TIME),
                new DataTypeResolver.DataTypeEntry(Types.TIMESTAMP_WITH_TIMEZONE, MariaDBParser.TIMESTAMP),
                new DataTypeResolver.DataTypeEntry(Types.TIMESTAMP, MariaDBParser.DATETIME),
                new DataTypeResolver.DataTypeEntry(Types.BINARY, MariaDBParser.BINARY),
                new DataTypeResolver.DataTypeEntry(Types.VARBINARY, MariaDBParser.VARBINARY),
                new DataTypeResolver.DataTypeEntry(Types.BLOB, MariaDBParser.BLOB),
                new DataTypeResolver.DataTypeEntry(Types.INTEGER, MariaDBParser.YEAR)));

        dataTypeResolverBuilder.registerDataTypes(MariaDBParser.SimpleDataTypeContext.class.getCanonicalName(), Arrays.asList(
                new DataTypeResolver.DataTypeEntry(Types.DATE, MariaDBParser.DATE),
                new DataTypeResolver.DataTypeEntry(Types.BLOB, MariaDBParser.TINYBLOB),
                new DataTypeResolver.DataTypeEntry(Types.BLOB, MariaDBParser.MEDIUMBLOB),
                new DataTypeResolver.DataTypeEntry(Types.BLOB, MariaDBParser.LONGBLOB),
                new DataTypeResolver.DataTypeEntry(Types.BOOLEAN, MariaDBParser.BOOL),
                new DataTypeResolver.DataTypeEntry(Types.BOOLEAN, MariaDBParser.BOOLEAN),
                new DataTypeResolver.DataTypeEntry(Types.BIGINT, MariaDBParser.SERIAL)));

        dataTypeResolverBuilder.registerDataTypes(MariaDBParser.CollectionDataTypeContext.class.getCanonicalName(), Arrays.asList(
                new DataTypeResolver.DataTypeEntry(Types.CHAR, MariaDBParser.ENUM).setSuffixTokens(MariaDBParser.BINARY),
                new DataTypeResolver.DataTypeEntry(Types.CHAR, MariaDBParser.SET).setSuffixTokens(MariaDBParser.BINARY)));

        dataTypeResolverBuilder.registerDataTypes(MariaDBParser.SpatialDataTypeContext.class.getCanonicalName(), Arrays.asList(
                new DataTypeResolver.DataTypeEntry(Types.OTHER, MariaDBParser.GEOMETRYCOLLECTION),
                new DataTypeResolver.DataTypeEntry(Types.OTHER, MariaDBParser.GEOMCOLLECTION),
                new DataTypeResolver.DataTypeEntry(Types.OTHER, MariaDBParser.LINESTRING),
                new DataTypeResolver.DataTypeEntry(Types.OTHER, MariaDBParser.MULTILINESTRING),
                new DataTypeResolver.DataTypeEntry(Types.OTHER, MariaDBParser.MULTIPOINT),
                new DataTypeResolver.DataTypeEntry(Types.OTHER, MariaDBParser.MULTIPOLYGON),
                new DataTypeResolver.DataTypeEntry(Types.OTHER, MariaDBParser.POINT),
                new DataTypeResolver.DataTypeEntry(Types.OTHER, MariaDBParser.POLYGON),
                new DataTypeResolver.DataTypeEntry(Types.OTHER, MariaDBParser.JSON),
                new DataTypeResolver.DataTypeEntry(Types.OTHER, MariaDBParser.GEOMETRY)));

        dataTypeResolverBuilder.registerDataTypes(MariaDBParser.LongVarbinaryDataTypeContext.class.getCanonicalName(), Arrays.asList(
                new DataTypeResolver.DataTypeEntry(Types.BLOB, MariaDBParser.LONG)
                        .setSuffixTokens(MariaDBParser.VARBINARY)));

        dataTypeResolverBuilder.registerDataTypes(MariaDBParser.LongVarcharDataTypeContext.class.getCanonicalName(), Arrays.asList(
                new DataTypeResolver.DataTypeEntry(Types.VARCHAR, MariaDBParser.LONG)
                        .setSuffixTokens(MariaDBParser.VARCHAR)));

        dataTypeResolverBuilder.registerDataTypes(MariaDBParser.UuidDataTypeContext.class.getCanonicalName(), Arrays.asList(
                new DataTypeResolver.DataTypeEntry(Types.OTHER, MariaDBParser.UUID)));

        return dataTypeResolverBuilder.build();
    }

    /**
     * Get the character set registry.
     *
     * @return the character set registry
     */
    public BinlogCharsetRegistry getCharsetRegistry() {
        return charsetRegistry;
    }

    /**
     * Provides a map of default character sets by database/schema name.
     *
     * @return map of default character sets.
     */
    public ConcurrentMap<String, String> charsetNameForDatabase() {
        return charsetNameForDatabase;
    }

    /**
     * Parse a name from {@link MariaDBParser.UidContext}.
     *
     * @param uidContext uid context
     * @return name without quotes.
     */
    public String parseName(MariaDBParser.UidContext uidContext) {
        return withoutQuotes(uidContext);
    }

    /**
     * Parse qualified table identification from {@link MariaDBParser.FullIdContext}.
     * {@link MariaDbAntlrDdlParser#currentSchema()} will be used if definition of schema name is not part of the context.
     *
     * @param fullIdContext full id context.
     * @return qualified {@link TableId}.
     */
    public TableId parseQualifiedTableId(MariaDBParser.FullIdContext fullIdContext) {
        final char[] fullTableName = fullIdContext.getText().toCharArray();
        StringBuilder component = new StringBuilder();
        String dbName = null;
        String tableName = null;
        final char EMPTY = '\0';
        char lastQuote = EMPTY;
        for (int i = 0; i < fullTableName.length; i++) {
            char c = fullTableName[i];
            if (isQuote(c)) {
                // Opening quote
                if (lastQuote == EMPTY) {
                    lastQuote = c;
                }
                // Closing quote
                else if (lastQuote == c) {
                    // escape of quote by doubling
                    if (i < fullTableName.length - 1 && fullTableName[i + 1] == c) {
                        component.append(c);
                        i++;
                    }
                    else {
                        lastQuote = EMPTY;
                    }
                }
                // Quote that is part of name
                else {
                    component.append(c);
                }
            }
            // dot that is not in quotes, so name separator
            else if (c == '.' && lastQuote == EMPTY) {
                dbName = component.toString();
                component = new StringBuilder();
            }
            // Any char is part of name including quoted dot
            else {
                component.append(c);
            }
        }
        tableName = component.toString();
        return resolveTableId(dbName != null ? dbName : currentSchema(), tableName);
    }

    /**
     * Parse column names for primary index from {@link MariaDBParser.IndexColumnNamesContext}. This method will update
     * column to be not optional and set primary key column names to table.
     *
     * @param indexColumnNamesContext primary key index column names context.
     * @param tableEditor editor for table where primary key index is parsed.
     */
    public void parsePrimaryIndexColumnNames(MariaDBParser.IndexColumnNamesContext indexColumnNamesContext, TableEditor tableEditor) {
        List<String> pkColumnNames = indexColumnNamesContext.indexColumnName().stream()
                .map(indexColumnNameContext -> {
                    // MariaDB does not allow a primary key to have nullable columns, so let's make sure we model that correctly ...
                    String columnName;
                    if (indexColumnNameContext.uid() != null) {
                        columnName = parseName(indexColumnNameContext.uid());
                    }
                    else if (indexColumnNameContext.STRING_LITERAL() != null) {
                        columnName = withoutQuotes(indexColumnNameContext.STRING_LITERAL().getText());
                    }
                    else {
                        columnName = indexColumnNameContext.expression().getText();
                    }
                    Column column = tableEditor.columnWithName(columnName);
                    if (column != null && column.isOptional()) {
                        final ColumnEditor ce = column.edit().optional(false);
                        if (ce.hasDefaultValue() && !ce.defaultValueExpression().isPresent()) {
                            ce.unsetDefaultValueExpression();
                        }
                        tableEditor.addColumn(ce.create());
                    }
                    return column != null ? column.name() : columnName;
                })
                .collect(Collectors.toList());

        tableEditor.setPrimaryKeyNames(pkColumnNames);
    }

    /**
     * Parse column names for unique index from {@link MariaDBParser.IndexColumnNamesContext}. This method will set
     * unique key column names to table if there are no optional.
     *
     * @param indexColumnNamesContext unique key index column names context.
     * @param tableEditor editor for table where primary key index is parsed.
     */
    public void parseUniqueIndexColumnNames(MariaDBParser.IndexColumnNamesContext indexColumnNamesContext, TableEditor tableEditor) {
        List<Column> indexColumns = getIndexColumns(indexColumnNamesContext, tableEditor);
        if (indexColumns.stream().filter(col -> Objects.isNull(col) || col.isOptional()).count() > 0) {
            logger.warn("Skip to set unique index columns {} to primary key which including optional columns", indexColumns);
        }
        else {
            tableEditor.setPrimaryKeyNames(indexColumns.stream().map(Column::name).collect(Collectors.toList()));
        }
    }

    /**
     * Determine if a table's unique index should be included when parsing relative unique index statement.
     *
     * @param indexColumnNamesContext unique index column names context.
     * @param tableEditor editor for table where unique index is parsed.
     * @return true if the index is to be included; false otherwise.
     */
    public boolean isTableUniqueIndexIncluded(MariaDBParser.IndexColumnNamesContext indexColumnNamesContext, TableEditor tableEditor) {
        return getIndexColumns(indexColumnNamesContext, tableEditor).stream().filter(Objects::isNull).count() == 0;
    }

    private List<Column> getIndexColumns(MariaDBParser.IndexColumnNamesContext indexColumnNamesContext, TableEditor tableEditor) {
        return indexColumnNamesContext.indexColumnName().stream()
                .map(indexColumnNameContext -> {
                    String columnName;
                    if (indexColumnNameContext.uid() != null) {
                        columnName = parseName(indexColumnNameContext.uid());
                    }
                    else if (indexColumnNameContext.STRING_LITERAL() != null) {
                        columnName = withoutQuotes(indexColumnNameContext.STRING_LITERAL().getText());
                    }
                    else {
                        columnName = indexColumnNameContext.expression().getText();
                    }
                    return tableEditor.columnWithName(columnName);
                })
                .collect(Collectors.toList());
    }

    /**
     * Get the name of the character set for the current database, via the "character_set_database" system property.
     *
     * @return the name of the character set for the current database, or null if not known ...
     */
    public String currentDatabaseCharset() {
        String charsetName = systemVariables.getVariable(BinlogSystemVariables.CHARSET_NAME_DATABASE);
        if (charsetName == null || "DEFAULT".equalsIgnoreCase(charsetName)) {
            charsetName = systemVariables.getVariable(BinlogSystemVariables.CHARSET_NAME_SERVER);
        }
        return charsetName;
    }

    /**
     * Get the name of the character set for the give table name.
     *
     * @return the name of the character set for the given table, or null if not known ...
     */
    public String charsetForTable(TableId tableId) {
        final String defaultDatabaseCharset = tableId.catalog() != null ? charsetNameForDatabase().get(tableId.catalog()) : null;
        return defaultDatabaseCharset != null ? defaultDatabaseCharset : currentDatabaseCharset();
    }

    /**
     * Runs a function if all given object are not null.
     *
     * @param function function to run; may not be null
     * @param nullableObjects object to be tested, if they are null.
     */
    public void runIfNotNull(Runnable function, Object... nullableObjects) {
        for (Object nullableObject : nullableObjects) {
            if (nullableObject == null) {
                return;
            }
        }
        function.run();
    }

    /**
     * Extracts the enumeration values properly parsed and escaped.
     *
     * @param enumValues the raw enumeration values from the parsed column definition
     * @return the list of options allowed for the {@code ENUM} or {@code SET}; never null.
     */
    public static List<String> extractEnumAndSetOptions(List<String> enumValues) {
        return enumValues.stream()
                .map(MariaDbAntlrDdlParser::withoutQuotes)
                .map(MariaDbAntlrDdlParser::escapeOption)
                .collect(Collectors.toList());
    }

    public static String escapeOption(String option) {
        // Replace comma to backslash followed by comma (this escape sequence implies comma is part of the option)
        // Replace backlash+single-quote to a single-quote.
        // Replace double single-quote to a single-quote.
        return option.replaceAll(",", "\\\\,").replaceAll("\\\\'", "'").replace("''", "'");
    }

    public Tables.TableFilter getTableFilter() {
        return tableFilter;
    }

    /**
     * Obtains the charset name either form charset if present or from collation.
     *
     * @param charsetNode
     * @param collationNode
     * @return character set
     */
    public String extractCharset(CharsetNameContext charsetNode, CollationNameContext collationNode) {
        String charsetName = null;
        if (charsetNode != null && charsetNode.getText() != null) {
            charsetName = withoutQuotes(charsetNode.getText());
            // System.out.println("[charSetNode]: charSetName => " + charsetName + " (" + charsetNode.getText() + ")");
        }
        else if (collationNode != null && collationNode.getText() != null) {
            final String collationName = withoutQuotes(collationNode.getText()).toLowerCase();
            for (int index = 0; index < charsetRegistry.getCharsetMapSize(); index++) {
                if (collationName.equals(charsetRegistry.getCollationNameForCollationIndex(index))) {
                    charsetName = charsetRegistry.getCharsetNameForCollationIndex(index);
                    break;
                }
            }
            // System.out.println("[collationNode]: charSetName => " + charsetName + " (" + collationNode.getText() + ")");
        }

        return charsetName;
    }

    /**
     * Signal an alter table event to ddl changes listener.
     *
     * @param id         the table identifier; may not be null
     * @param previousId the previous name of the view if it was renamed, or null if it was not renamed
     * @param ctx        the start of the statement; may not be null
     */
    public void signalAlterTable(TableId id, TableId previousId, RenameTableClauseContext ctx) {
        final RenameTableContext parent = (RenameTableContext) ctx.getParent();
        Interval interval = new Interval(ctx.getParent().start.getStartIndex(),
                parent.renameTableClause().get(0).start.getStartIndex() - 1);
        String prefix = ctx.getParent().start.getInputStream().getText(interval);
        signalAlterTable(id, previousId, prefix + getText(ctx));
    }
}
