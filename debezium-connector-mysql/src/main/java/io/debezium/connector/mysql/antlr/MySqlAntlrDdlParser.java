/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr;

import io.debezium.antlr.AntlrDdlParser;
import io.debezium.antlr.AntlrDdlParserListener;
import io.debezium.antlr.DataTypeResolver;
import io.debezium.antlr.DataTypeResolver.DataTypeEntry;
import io.debezium.connector.mysql.MySqlSystemVariables;
import io.debezium.connector.mysql.antlr.listener.MySqlAntlrDdlParserListener;
import io.debezium.ddl.parser.mysql.generated.MySqlLexer;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.relational.Column;
import io.debezium.relational.SystemVariables;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A ANTLR based parser for MySQL DDL statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class MySqlAntlrDdlParser extends AntlrDdlParser<MySqlLexer, MySqlParser> {

    private final ConcurrentMap<String, String> charsetNameForDatabase = new ConcurrentHashMap<>();

    public MySqlAntlrDdlParser() {
        this(true);
    }

    public MySqlAntlrDdlParser(boolean throwErrorsFromTreeWalk) {
        this(throwErrorsFromTreeWalk, false);
    }

    public MySqlAntlrDdlParser(boolean throwErrorsFromTreeWalk, boolean includeViews) {
        super(throwErrorsFromTreeWalk, includeViews);
        systemVariables = new MySqlSystemVariables();
    }

    @Override
    protected ParseTree parseTree(MySqlParser parser) {
        return parser.root();
    }

    @Override
    protected AntlrDdlParserListener createParseTreeWalkerListener() {
        return new MySqlAntlrDdlParserListener(this);
    }

    @Override
    protected MySqlLexer createNewLexerInstance(CharStream charStreams) {
        return new MySqlLexer(charStreams);
    }

    @Override
    protected MySqlParser createNewParserInstance(CommonTokenStream commonTokenStream) {
        return new MySqlParser(commonTokenStream);
    }

    @Override
    protected SystemVariables createNewSystemVariablesInstance() {
        return new MySqlSystemVariables();
    }

    @Override
    protected boolean isGrammarInUpperCase() {
        return true;
    }

    @Override
    protected void initDataTypes(DataTypeResolver dataTypeResolver) {
        dataTypeResolver.registerDataTypes(MySqlParser.StringDataTypeContext.class.getCanonicalName(), Arrays.asList(
                new DataTypeEntry(Types.CHAR, MySqlParser.CHAR),
                new DataTypeEntry(Types.VARCHAR, MySqlParser.VARCHAR),
                new DataTypeEntry(Types.VARCHAR, MySqlParser.TINYTEXT),
                new DataTypeEntry(Types.VARCHAR, MySqlParser.TEXT),
                new DataTypeEntry(Types.VARCHAR, MySqlParser.MEDIUMTEXT),
                new DataTypeEntry(Types.VARCHAR, MySqlParser.LONGTEXT),
                new DataTypeEntry(Types.NCHAR, MySqlParser.NCHAR),
                new DataTypeEntry(Types.NVARCHAR, MySqlParser.NVARCHAR),
                new DataTypeEntry(Types.BINARY, MySqlParser.CHAR, MySqlParser.BINARY),
                new DataTypeEntry(Types.BINARY, MySqlParser.VARCHAR, MySqlParser.BINARY),
                new DataTypeEntry(Types.BINARY, MySqlParser.TINYTEXT, MySqlParser.BINARY),
                new DataTypeEntry(Types.BINARY, MySqlParser.TEXT, MySqlParser.BINARY),
                new DataTypeEntry(Types.BINARY, MySqlParser.MEDIUMTEXT, MySqlParser.BINARY),
                new DataTypeEntry(Types.BINARY, MySqlParser.LONGTEXT, MySqlParser.BINARY),
                new DataTypeEntry(Types.BINARY, MySqlParser.NCHAR, MySqlParser.BINARY),
                new DataTypeEntry(Types.BINARY, MySqlParser.NVARCHAR, MySqlParser.BINARY)
        ));
        dataTypeResolver.registerDataTypes(MySqlParser.NationalStringDataTypeContext.class.getCanonicalName(), Arrays.asList(
                new DataTypeEntry(Types.NVARCHAR, MySqlParser.NATIONAL, MySqlParser.VARCHAR).setSuffixTokens(MySqlParser.BINARY),
                new DataTypeEntry(Types.NCHAR, MySqlParser.NATIONAL, MySqlParser.CHARACTER).setSuffixTokens(MySqlParser.BINARY),
                new DataTypeEntry(Types.NVARCHAR, MySqlParser.NCHAR, MySqlParser.VARCHAR).setSuffixTokens(MySqlParser.BINARY)
        ));
        dataTypeResolver.registerDataTypes(MySqlParser.NationalVaryingStringDataTypeContext.class.getCanonicalName(), Arrays.asList(
                new DataTypeEntry(Types.NVARCHAR, MySqlParser.NATIONAL, MySqlParser.CHAR, MySqlParser.VARYING),
                new DataTypeEntry(Types.NVARCHAR, MySqlParser.NATIONAL, MySqlParser.CHARACTER, MySqlParser.VARYING)
        ));
        dataTypeResolver.registerDataTypes(MySqlParser.DimensionDataTypeContext.class.getCanonicalName(), Arrays.asList(
                new DataTypeEntry(Types.SMALLINT, MySqlParser.TINYINT)
                        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL),
                new DataTypeEntry(Types.SMALLINT, MySqlParser.SMALLINT)
                        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL),
                new DataTypeEntry(Types.INTEGER, MySqlParser.MEDIUMINT)
                        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL),
                new DataTypeEntry(Types.INTEGER, MySqlParser.INT)
                        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL),
                new DataTypeEntry(Types.INTEGER, MySqlParser.INTEGER)
                        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL),
                new DataTypeEntry(Types.BIGINT, MySqlParser.BIGINT)
                        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL),
                new DataTypeEntry(Types.REAL, MySqlParser.REAL)
                        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL),
                new DataTypeEntry(Types.DOUBLE, MySqlParser.DOUBLE)
                        .setSuffixTokens(MySqlParser.PRECISION, MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL),
                new DataTypeEntry(Types.FLOAT, MySqlParser.FLOAT)
                        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL),
                new DataTypeEntry(Types.DECIMAL, MySqlParser.DECIMAL)
                        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL)
                        .setDefaultLengthScaleDimension(10, 0),
                new DataTypeEntry(Types.DECIMAL, MySqlParser.DEC)
                        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL)
                        .setDefaultLengthScaleDimension(10, 0),
                new DataTypeEntry(Types.DECIMAL, MySqlParser.FIXED)
                        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL)
                        .setDefaultLengthScaleDimension(10, 0),
                new DataTypeEntry(Types.NUMERIC, MySqlParser.NUMERIC)
                        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL),
                new DataTypeEntry(Types.BIT, MySqlParser.BIT),
                new DataTypeEntry(Types.TIME, MySqlParser.TIME),
                new DataTypeEntry(Types.TIMESTAMP_WITH_TIMEZONE, MySqlParser.TIMESTAMP),
                new DataTypeEntry(Types.TIMESTAMP, MySqlParser.DATETIME),
                new DataTypeEntry(Types.BINARY, MySqlParser.BINARY),
                new DataTypeEntry(Types.VARBINARY, MySqlParser.VARBINARY),
                new DataTypeEntry(Types.INTEGER, MySqlParser.YEAR)
        ));
        dataTypeResolver.registerDataTypes(MySqlParser.SimpleDataTypeContext.class.getCanonicalName(), Arrays.asList(
                new DataTypeEntry(Types.DATE, MySqlParser.DATE),
                new DataTypeEntry(Types.BLOB, MySqlParser.TINYBLOB),
                new DataTypeEntry(Types.BLOB, MySqlParser.BLOB),
                new DataTypeEntry(Types.BLOB, MySqlParser.MEDIUMBLOB),
                new DataTypeEntry(Types.BLOB, MySqlParser.LONGBLOB),
                new DataTypeEntry(Types.BOOLEAN, MySqlParser.BOOL),
                new DataTypeEntry(Types.BOOLEAN, MySqlParser.BOOLEAN)
        ));
        dataTypeResolver.registerDataTypes(MySqlParser.CollectionDataTypeContext.class.getCanonicalName(), Arrays.asList(
                new DataTypeEntry(Types.CHAR, MySqlParser.ENUM).setSuffixTokens(MySqlParser.BINARY),
                new DataTypeEntry(Types.CHAR, MySqlParser.SET).setSuffixTokens(MySqlParser.BINARY)
        ));
        dataTypeResolver.registerDataTypes(MySqlParser.SpatialDataTypeContext.class.getCanonicalName(), Arrays.asList(
                new DataTypeEntry(Types.OTHER, MySqlParser.GEOMETRYCOLLECTION),
                new DataTypeEntry(Types.OTHER, MySqlParser.LINESTRING),
                new DataTypeEntry(Types.OTHER, MySqlParser.MULTILINESTRING),
                new DataTypeEntry(Types.OTHER, MySqlParser.MULTIPOINT),
                new DataTypeEntry(Types.OTHER, MySqlParser.MULTIPOLYGON),
                new DataTypeEntry(Types.OTHER, MySqlParser.POINT),
                new DataTypeEntry(Types.OTHER, MySqlParser.POLYGON),
                new DataTypeEntry(Types.OTHER, MySqlParser.JSON),
                new DataTypeEntry(Types.OTHER, MySqlParser.GEOMETRY)
        ));
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
     * Parse a name from {@link MySqlParser.UidContext}.
     *
     * @param uidContext uid context
     * @return name without quotes.
     */
    public String parseName(MySqlParser.UidContext uidContext) {
        return withoutQuotes(uidContext);
    }

    /**
     * Parse qualified table identification from {@link MySqlParser.FullIdContext}.
     * {@link MySqlAntlrDdlParser#currentSchema()} will be used if definition of schema name is not part of the context.
     *
     * @param fullIdContext full id context.
     * @return qualified {@link TableId}.
     */
    public TableId parseQualifiedTableId(MySqlParser.FullIdContext fullIdContext) {
        String fullTableName = fullIdContext.getText();
        int dotIndex;
        if ((dotIndex = fullTableName.indexOf(".")) > 0) {
            return resolveTableId(withoutQuotes(fullTableName.substring(0, dotIndex)),
                    withoutQuotes(fullTableName.substring(dotIndex + 1, fullTableName.length())));
        }
        else {
            return resolveTableId(currentSchema(), withoutQuotes(fullTableName));
        }
    }

    /**
     * Parse column names for primary index from {@link MySqlParser.IndexColumnNamesContext}. This method will updates
     * column to be not optional and set primary key column names to table.
     *
     * @param indexColumnNamesContext primary key index column names context.
     * @param tableEditor editor for table where primary key index is parsed.
     */
    public void parsePrimaryIndexColumnNames(MySqlParser.IndexColumnNamesContext indexColumnNamesContext, TableEditor tableEditor) {
        List<String> pkColumnNames = indexColumnNamesContext.indexColumnName().stream()
                .map(indexColumnNameContext -> {
                    // MySQL does not allow a primary key to have nullable columns, so let's make sure we model that correctly ...
                    String columnName;
                    if (indexColumnNameContext.uid() != null) {
                        columnName = parseName(indexColumnNameContext.uid());
                    }
                    else {
                        columnName = withoutQuotes(indexColumnNameContext.STRING_LITERAL().getText());
                    }
                    Column column = tableEditor.columnWithName(columnName);
                    if (column != null && column.isOptional()) {
                        tableEditor.addColumn(column.edit().optional(false).create());
                    }
                    return columnName;
                })
                .collect(Collectors.toList());

        tableEditor.setPrimaryKeyNames(pkColumnNames);
    }

    /**
     * Get the name of the character set for the current database, via the "character_set_database" system property.
     *
     * @return the name of the character set for the current database, or null if not known ...
     */
    public String currentDatabaseCharset() {
        String charsetName = systemVariables.getVariable(MySqlSystemVariables.CHARSET_NAME_DATABASE);
        if (charsetName == null || "DEFAULT".equalsIgnoreCase(charsetName)) {
            charsetName = systemVariables.getVariable(MySqlSystemVariables.CHARSET_NAME_SERVER);
        }
        return charsetName;
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
     * Parse the {@code ENUM} or {@code SET} data type expression to extract the character options, where the index(es) appearing
     * in the {@code ENUM} or {@code SET} values can be used to identify the acceptable characters.
     *
     * @param typeExpression the data type expression
     * @return the string containing the character options allowed by the {@code ENUM} or {@code SET}; never null
     */
    public static List<String> parseSetAndEnumOptions(String typeExpression) {
        List<String> options = new ArrayList<>();
        if (typeExpression.startsWith("ENUM") || typeExpression.startsWith("SET")) {
            Pattern pattern = Pattern.compile("['\"][a-zA-Z0-9-!$%^&*()_+|~=`{}\\[\\]:\";'<>?\\/\\\\ ]*['\"]");
            Matcher matcher = pattern.matcher(typeExpression);
            while (matcher.find()) {
                options.add(withoutQuotes(matcher.group()));
            }
        }
        return options;
    }

}
