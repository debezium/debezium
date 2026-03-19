/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;

import io.debezium.annotation.VisibleForTesting;
import io.debezium.antlr.AntlrDdlParser;
import io.debezium.antlr.AntlrDdlParserListener;
import io.debezium.antlr.DataTypeResolver;
import io.debezium.antlr.DataTypeResolver.DataTypeEntry;
import io.debezium.connector.binlog.charset.BinlogCharsetRegistry;
import io.debezium.connector.binlog.jdbc.BinlogSystemVariables;
import io.debezium.connector.mysql.antlr.listener.MySqlAntlrDdlParserListener;
import io.debezium.ddl.parser.mysql.generated.MySqlLexer;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser.CharsetNameContext;
import io.debezium.ddl.parser.mysql.generated.MySqlParser.CollationNameContext;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.SystemVariables;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.ddl.DdlChanges;

/**
 * An ANTLR based parser for MySQL DDL statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class MySqlAntlrDdlParser extends AntlrDdlParser<MySqlLexer, MySqlParser> {

    /**
     * Valid MySQL charset introducers for string literals (e.g., _utf8mb3'text').
     * The Oracle MySQL ANTLR grammar requires the lexer's charSets Set to be populated
     * to distinguish charset introducers from regular identifiers starting with underscore.
     *
     * This list matches the CHARSET_NAME fragment from the Positive Technologies grammar
     * that was used before migrating to the Oracle grammar.
     *
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/charset-introducer.html">MySQL Character Set Introducers</a>
     */
    private static final List<String> MYSQL_CHARSET_INTRODUCERS = Arrays.asList(
            "_armscii8", "_ascii", "_big5", "_binary",
            "_cp1250", "_cp1251", "_cp1256", "_cp1257",
            "_cp850", "_cp852", "_cp866", "_cp932",
            "_dec8", "_eucjpms", "_euckr",
            "_gb2312", "_gbk", "_gb18030", "_geostd8",
            "_greek", "_hebrew", "_hp8",
            "_keybcs2", "_koi8r", "_koi8u",
            "_latin1", "_latin2", "_latin5", "_latin7",
            "_macce", "_macroman",
            "_sjis", "_swe7",
            "_tis620", "_ucs2", "_ujis",
            "_utf16", "_utf16le", "_utf32",
            "_utf8", "_utf8mb3", "_utf8mb4");

    private final ConcurrentMap<String, String> charsetNameForDatabase = new ConcurrentHashMap<>();
    private final TableFilter tableFilter;
    private final BinlogCharsetRegistry charsetRegistry;
    private final DataTypeResolver dataTypeResolver = initializeDataTypeResolver();

    @VisibleForTesting
    public MySqlAntlrDdlParser() {
        this(TableFilter.includeAll());
    }

    @VisibleForTesting
    public MySqlAntlrDdlParser(TableFilter tableFilter) {
        this(true, false, false, tableFilter, null);
    }

    public MySqlAntlrDdlParser(boolean throwErrorsFromTreeWalk, boolean includeViews, boolean includeComments,
                               TableFilter tableFilter, BinlogCharsetRegistry charsetRegistry) {
        super(throwErrorsFromTreeWalk, includeViews, includeComments);
        systemVariables = new BinlogSystemVariables();
        this.tableFilter = tableFilter;
        this.charsetRegistry = charsetRegistry;
    }

    @Override
    protected ParseTree parseTree(MySqlParser parser) {
        return parser.queries();
    }

    @Override
    protected AntlrDdlParserListener createParseTreeWalkerListener() {
        return new MySqlAntlrDdlParserListener(this);
    }

    @Override
    protected MySqlLexer createNewLexerInstance(CharStream charStreams) {
        MySqlLexer lexer = new MySqlLexer(charStreams);

        // Populate the lexer's charSets Set with valid MySQL charset introducers.
        // The Oracle MySQL grammar uses runtime validation via MySqlLexerBase.checkCharset()
        // to distinguish charset introducers (e.g., _utf8mb3'text') from regular identifiers.
        // Oracle grammar requires runtime initialization (PT grammar hard-coded these in lexer).
        lexer.charSets.addAll(MYSQL_CHARSET_INTRODUCERS);

        return lexer;
    }

    @Override
    protected MySqlParser createNewParserInstance(CommonTokenStream commonTokenStream) {
        return new MySqlParser(commonTokenStream);
    }

    @Override
    public DdlChanges parse(String ddlContent, Tables databaseTables) {
        // Normalize double-quoted strings to support MySQL default mode in ANSI_QUOTES parser
        String normalizedDdl = DdlNormalizer.normalize(ddlContent);
        return super.parse(normalizedDdl, databaseTables);
    }

    @Override
    protected SystemVariables createNewSystemVariablesInstance() {
        return new BinlogSystemVariables();
    }

    @Override
    protected boolean isGrammarInUpperCase() {
        return false;
    }

    @Override
    public DataTypeResolver dataTypeResolver() {
        return dataTypeResolver;
    }

    private DataTypeResolver initializeDataTypeResolver() {
        DataTypeResolver.Builder dataTypeResolverBuilder = new DataTypeResolver.Builder();

        // In the new Oracle grammar, all data types are unified in DataTypeContext
        // The type is identified by the 'type' label token
        dataTypeResolverBuilder.registerDataTypes(MySqlParser.DataTypeContext.class.getCanonicalName(), Arrays.asList(
                // String types
                new DataTypeEntry(Types.CHAR, MySqlParser.CHAR_SYMBOL),
                new DataTypeEntry(Types.VARCHAR, MySqlParser.CHAR_SYMBOL, MySqlParser.VARYING_SYMBOL),
                new DataTypeEntry(Types.VARCHAR, MySqlParser.VARCHAR_SYMBOL),
                new DataTypeEntry(Types.VARCHAR, MySqlParser.TINYTEXT_SYMBOL),
                new DataTypeEntry(Types.VARCHAR, MySqlParser.TEXT_SYMBOL),
                new DataTypeEntry(Types.VARCHAR, MySqlParser.MEDIUMTEXT_SYMBOL),
                new DataTypeEntry(Types.VARCHAR, MySqlParser.LONGTEXT_SYMBOL),
                new DataTypeEntry(Types.VARCHAR, MySqlParser.LONG_SYMBOL),
                new DataTypeEntry(Types.NCHAR, MySqlParser.NCHAR_SYMBOL),
                new DataTypeEntry(Types.NVARCHAR, MySqlParser.NCHAR_SYMBOL, MySqlParser.VARYING_SYMBOL),
                new DataTypeEntry(Types.NVARCHAR, MySqlParser.NVARCHAR_SYMBOL),
                new DataTypeEntry(Types.CHAR, MySqlParser.CHAR_SYMBOL, MySqlParser.BINARY_SYMBOL),
                new DataTypeEntry(Types.VARCHAR, MySqlParser.VARCHAR_SYMBOL, MySqlParser.BINARY_SYMBOL),
                new DataTypeEntry(Types.VARCHAR, MySqlParser.TINYTEXT_SYMBOL, MySqlParser.BINARY_SYMBOL),
                new DataTypeEntry(Types.VARCHAR, MySqlParser.TEXT_SYMBOL, MySqlParser.BINARY_SYMBOL),
                new DataTypeEntry(Types.VARCHAR, MySqlParser.MEDIUMTEXT_SYMBOL, MySqlParser.BINARY_SYMBOL),
                new DataTypeEntry(Types.VARCHAR, MySqlParser.LONGTEXT_SYMBOL, MySqlParser.BINARY_SYMBOL),
                new DataTypeEntry(Types.NCHAR, MySqlParser.NCHAR_SYMBOL, MySqlParser.BINARY_SYMBOL),
                new DataTypeEntry(Types.NVARCHAR, MySqlParser.NVARCHAR_SYMBOL, MySqlParser.BINARY_SYMBOL),
                // National variants
                new DataTypeEntry(Types.NVARCHAR, MySqlParser.NATIONAL_SYMBOL, MySqlParser.VARCHAR_SYMBOL).setSuffixTokens(MySqlParser.BINARY_SYMBOL),
                new DataTypeEntry(Types.NCHAR, MySqlParser.NATIONAL_SYMBOL, MySqlParser.CHAR_SYMBOL).setSuffixTokens(MySqlParser.BINARY_SYMBOL),
                new DataTypeEntry(Types.NVARCHAR, MySqlParser.NATIONAL_SYMBOL, MySqlParser.CHAR_SYMBOL, MySqlParser.VARYING_SYMBOL),
                new DataTypeEntry(Types.NVARCHAR, MySqlParser.NCHAR_SYMBOL, MySqlParser.VARCHAR_SYMBOL).setSuffixTokens(MySqlParser.BINARY_SYMBOL),
                new DataTypeEntry(Types.NVARCHAR, MySqlParser.NCHAR_SYMBOL, MySqlParser.VARYING_SYMBOL),
                // Integer types
                new DataTypeEntry(Types.SMALLINT, MySqlParser.TINYINT_SYMBOL)
                        .setSuffixTokens(MySqlParser.SIGNED_SYMBOL, MySqlParser.UNSIGNED_SYMBOL, MySqlParser.ZEROFILL_SYMBOL),
                new DataTypeEntry(Types.SMALLINT, MySqlParser.SMALLINT_SYMBOL)
                        .setSuffixTokens(MySqlParser.SIGNED_SYMBOL, MySqlParser.UNSIGNED_SYMBOL, MySqlParser.ZEROFILL_SYMBOL),
                new DataTypeEntry(Types.INTEGER, MySqlParser.MEDIUMINT_SYMBOL)
                        .setSuffixTokens(MySqlParser.SIGNED_SYMBOL, MySqlParser.UNSIGNED_SYMBOL, MySqlParser.ZEROFILL_SYMBOL),
                new DataTypeEntry(Types.INTEGER, MySqlParser.INT_SYMBOL)
                        .setSuffixTokens(MySqlParser.SIGNED_SYMBOL, MySqlParser.UNSIGNED_SYMBOL, MySqlParser.ZEROFILL_SYMBOL),
                new DataTypeEntry(Types.BIGINT, MySqlParser.BIGINT_SYMBOL)
                        .setSuffixTokens(MySqlParser.SIGNED_SYMBOL, MySqlParser.UNSIGNED_SYMBOL, MySqlParser.ZEROFILL_SYMBOL),
                // Floating point types
                new DataTypeEntry(Types.REAL, MySqlParser.REAL_SYMBOL)
                        .setSuffixTokens(MySqlParser.SIGNED_SYMBOL, MySqlParser.UNSIGNED_SYMBOL, MySqlParser.ZEROFILL_SYMBOL),
                new DataTypeEntry(Types.DOUBLE, MySqlParser.DOUBLE_SYMBOL)
                        .setSuffixTokens(MySqlParser.PRECISION_SYMBOL, MySqlParser.SIGNED_SYMBOL, MySqlParser.UNSIGNED_SYMBOL, MySqlParser.ZEROFILL_SYMBOL),
                new DataTypeEntry(Types.FLOAT, MySqlParser.FLOAT_SYMBOL)
                        .setSuffixTokens(MySqlParser.SIGNED_SYMBOL, MySqlParser.UNSIGNED_SYMBOL, MySqlParser.ZEROFILL_SYMBOL),
                new DataTypeEntry(Types.DECIMAL, MySqlParser.DECIMAL_SYMBOL)
                        .setSuffixTokens(MySqlParser.SIGNED_SYMBOL, MySqlParser.UNSIGNED_SYMBOL, MySqlParser.ZEROFILL_SYMBOL)
                        .setDefaultLengthScaleDimension(10, 0),
                new DataTypeEntry(Types.NUMERIC, MySqlParser.NUMERIC_SYMBOL)
                        .setSuffixTokens(MySqlParser.SIGNED_SYMBOL, MySqlParser.UNSIGNED_SYMBOL, MySqlParser.ZEROFILL_SYMBOL)
                        .setDefaultLengthScaleDimension(10, 0),
                new DataTypeEntry(Types.DECIMAL, MySqlParser.FIXED_SYMBOL)
                        .setSuffixTokens(MySqlParser.SIGNED_SYMBOL, MySqlParser.UNSIGNED_SYMBOL, MySqlParser.ZEROFILL_SYMBOL)
                        .setDefaultLengthScaleDimension(10, 0),
                // Bit and temporal types
                new DataTypeEntry(Types.BIT, MySqlParser.BIT_SYMBOL)
                        .setDefaultLengthDimension(1),
                new DataTypeEntry(Types.TIME, MySqlParser.TIME_SYMBOL),
                new DataTypeEntry(Types.TIMESTAMP_WITH_TIMEZONE, MySqlParser.TIMESTAMP_SYMBOL),
                new DataTypeEntry(Types.TIMESTAMP, MySqlParser.DATETIME_SYMBOL),
                new DataTypeEntry(Types.DATE, MySqlParser.DATE_SYMBOL),
                new DataTypeEntry(Types.INTEGER, MySqlParser.YEAR_SYMBOL),
                // Binary types
                new DataTypeEntry(Types.BINARY, MySqlParser.BINARY_SYMBOL),
                new DataTypeEntry(Types.VARBINARY, MySqlParser.VARBINARY_SYMBOL),
                new DataTypeEntry(Types.BLOB, MySqlParser.BLOB_SYMBOL),
                new DataTypeEntry(Types.BLOB, MySqlParser.TINYBLOB_SYMBOL),
                new DataTypeEntry(Types.BLOB, MySqlParser.MEDIUMBLOB_SYMBOL),
                new DataTypeEntry(Types.BLOB, MySqlParser.LONGBLOB_SYMBOL),
                new DataTypeEntry(Types.BLOB, MySqlParser.LONG_SYMBOL, MySqlParser.VARBINARY_SYMBOL),
                // LONG VARCHAR variants
                new DataTypeEntry(Types.VARCHAR, MySqlParser.LONG_SYMBOL, MySqlParser.CHAR_SYMBOL, MySqlParser.VARYING_SYMBOL),
                new DataTypeEntry(Types.VARCHAR, MySqlParser.LONG_SYMBOL, MySqlParser.VARCHAR_SYMBOL),
                // Boolean types
                new DataTypeEntry(Types.BOOLEAN, MySqlParser.BOOL_SYMBOL),
                new DataTypeEntry(Types.BOOLEAN, MySqlParser.BOOLEAN_SYMBOL),
                new DataTypeEntry(Types.BIGINT, MySqlParser.SERIAL_SYMBOL),
                // Collection types
                new DataTypeEntry(Types.CHAR, MySqlParser.ENUM_SYMBOL).setSuffixTokens(MySqlParser.BINARY_SYMBOL),
                new DataTypeEntry(Types.CHAR, MySqlParser.SET_SYMBOL).setSuffixTokens(MySqlParser.BINARY_SYMBOL),
                // Spatial types
                new DataTypeEntry(Types.OTHER, MySqlParser.GEOMETRYCOLLECTION_SYMBOL),
                new DataTypeEntry(Types.OTHER, MySqlParser.LINESTRING_SYMBOL),
                new DataTypeEntry(Types.OTHER, MySqlParser.MULTILINESTRING_SYMBOL),
                new DataTypeEntry(Types.OTHER, MySqlParser.MULTIPOINT_SYMBOL),
                new DataTypeEntry(Types.OTHER, MySqlParser.MULTIPOLYGON_SYMBOL),
                new DataTypeEntry(Types.OTHER, MySqlParser.POINT_SYMBOL),
                new DataTypeEntry(Types.OTHER, MySqlParser.POLYGON_SYMBOL),
                new DataTypeEntry(Types.OTHER, MySqlParser.JSON_SYMBOL),
                new DataTypeEntry(Types.OTHER, MySqlParser.VECTOR_SYMBOL),
                new DataTypeEntry(Types.OTHER, MySqlParser.GEOMETRY_SYMBOL)));

        return dataTypeResolverBuilder.build();
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
     * Parse a name from {@link MySqlParser.IdentifierContext}.
     *
     * @param identifierContext identifier context
     * @return name without quotes.
     */
    public String parseName(MySqlParser.IdentifierContext identifierContext) {
        return withoutQuotes(identifierContext);
    }

    /**
     * Parse qualified table identification from {@link MySqlParser.TableRefContext} or {@link MySqlParser.TableNameContext}.
     * {@link MySqlAntlrDdlParser#currentSchema()} will be used if definition of schema name is not part of the context.
     *
     * @param tableContext table context (TableRefContext or TableNameContext).
     * @return qualified {@link TableId}.
     */
    public TableId parseQualifiedTableId(ParserRuleContext tableContext) {
        final char[] fullTableName = tableContext.getText().toCharArray();
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
     * Parse column names for primary index from {@link MySqlParser.KeyListContext}. This method will update
     * column to be not optional and set primary key column names to table.
     *
     * @param keyListContext primary key index column names context.
     * @param tableEditor editor for table where primary key index is parsed.
     */
    public void parsePrimaryIndexColumnNames(MySqlParser.KeyListContext keyListContext, TableEditor tableEditor) {
        List<String> pkColumnNames = keyListContext.keyPart().stream()
                .map(keyPartContext -> {
                    // MySQL does not allow a primary key to have nullable columns, so let's make sure we model that correctly ...
                    String columnName = removeRepeatedBacktick(parseName(keyPartContext.identifier()));
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
     * Parse column names for unique index from {@link MySqlParser.KeyListContext}. This method will set
     * unique key column names to table if there are no optional.
     *
     * @param keyListContext unique key index column names context.
     * @param tableEditor editor for table where primary key index is parsed.
     */
    public void parseUniqueIndexColumnNames(MySqlParser.KeyListContext keyListContext, TableEditor tableEditor) {
        List<Column> indexColumns = getIndexColumns(keyListContext, tableEditor);
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
     * @param keyListContext unique index column names context.
     * @param tableEditor editor for table where unique index is parsed.
     * @return true if the index is to be included; false otherwise.
     */
    public boolean isTableUniqueIndexIncluded(MySqlParser.KeyListContext keyListContext, TableEditor tableEditor) {
        return getIndexColumns(keyListContext, tableEditor).stream().filter(Objects::isNull).count() == 0;
    }

    private List<Column> getIndexColumns(MySqlParser.KeyListContext keyListContext, TableEditor tableEditor) {
        return keyListContext.keyPart().stream()
                .map(keyPartContext -> {
                    String columnName = removeRepeatedBacktick(parseName(keyPartContext.identifier()));
                    return tableEditor.columnWithName(columnName);
                })
                .collect(Collectors.toList());
    }

    // Overloaded methods for KeyListWithExpressionContext

    public void parsePrimaryIndexColumnNames(MySqlParser.KeyListWithExpressionContext keyListContext, TableEditor tableEditor) {
        List<String> pkColumnNames = keyListContext.keyPartOrExpression().stream()
                .filter(keyPartOrExpr -> keyPartOrExpr.keyPart() != null) // Skip expressions, only take column names
                .map(keyPartOrExpr -> {
                    String columnName = removeRepeatedBacktick(parseName(keyPartOrExpr.keyPart().identifier()));
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

    public void parseUniqueIndexColumnNames(MySqlParser.KeyListWithExpressionContext keyListContext, TableEditor tableEditor) {
        List<Column> indexColumns = getIndexColumns(keyListContext, tableEditor);
        if (indexColumns.stream().filter(col -> Objects.isNull(col) || col.isOptional()).count() > 0) {
            logger.warn("Skip to set unique index columns {} to primary key which including optional columns", indexColumns);
        }
        else {
            tableEditor.setPrimaryKeyNames(indexColumns.stream().map(Column::name).collect(Collectors.toList()));
        }
    }

    public boolean isTableUniqueIndexIncluded(MySqlParser.KeyListWithExpressionContext keyListContext, TableEditor tableEditor) {
        List<Column> indexColumns = getIndexColumns(keyListContext, tableEditor);

        // Check 1: All columns must exist in the table (no null columns)
        boolean allColumnsExist = indexColumns.stream().filter(Objects::isNull).count() == 0;

        // Check 2: No expressions in the index (column count must match key part count)
        // If expressions were filtered out, the sizes won't match
        boolean noExpressions = indexColumns.size() == keyListContext.keyPartOrExpression().size();

        return allColumnsExist && noExpressions;
    }

    private List<Column> getIndexColumns(MySqlParser.KeyListWithExpressionContext keyListContext, TableEditor tableEditor) {
        return keyListContext.keyPartOrExpression().stream()
                .filter(keyPartOrExpr -> keyPartOrExpr.keyPart() != null) // Skip expressions
                .map(keyPartOrExpr -> {
                    String columnName = removeRepeatedBacktick(parseName(keyPartOrExpr.keyPart().identifier()));
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
                .map(MySqlAntlrDdlParser::withoutQuotes)
                .map(MySqlAntlrDdlParser::escapeOption)
                .collect(Collectors.toList());
    }

    public static String escapeOption(String option) {
        // Replace comma to backslash followed by comma (this escape sequence implies comma is part of the option)
        // Replace backlash+single-quote to a single-quote.
        // Replace double single-quote to a single-quote.
        return option.replaceAll(",", "\\\\,").replaceAll("\\\\'", "'").replace("''", "'");
    }

    public TableFilter getTableFilter() {
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
        }
        else if (collationNode != null && collationNode.getText() != null) {
            final String collationName = withoutQuotes(collationNode.getText()).toLowerCase();
            for (int index = 0; index < charsetRegistry.getCharsetMapSize(); index++) {
                if (collationName.equals(charsetRegistry.getCollationNameForCollationIndex(index))) {
                    charsetName = charsetRegistry.getCharsetNameForCollationIndex(index);
                    break;
                }
            }
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
    public void signalAlterTable(TableId id, TableId previousId, MySqlParser.RenamePairContext ctx) {
        final MySqlParser.RenameTableStatementContext parent = (MySqlParser.RenameTableStatementContext) ctx.getParent();
        Interval interval = new Interval(ctx.getParent().start.getStartIndex(),
                parent.renamePair().get(0).start.getStartIndex() - 1);
        String prefix = ctx.getParent().start.getInputStream().getText(interval);
        signalAlterTable(id, previousId, prefix + getText(ctx));
    }
}
