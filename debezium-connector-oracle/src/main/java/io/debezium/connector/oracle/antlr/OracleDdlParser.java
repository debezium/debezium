/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.antlr;

import java.sql.Types;
import java.util.Arrays;
import java.util.Locale;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import io.debezium.antlr.AntlrDdlParser;
import io.debezium.antlr.AntlrDdlParserListener;
import io.debezium.antlr.DataTypeResolver;
import io.debezium.antlr.DataTypeResolver.DataTypeEntry;
import io.debezium.connector.oracle.antlr.listener.OracleDdlParserListener;
import io.debezium.ddl.parser.oracle.generated.PlSqlLexer;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.relational.SystemVariables;
import io.debezium.relational.Tables;

import oracle.jdbc.OracleTypes;

/**
 * This is the main Oracle Antlr DDL parser
 */
public class OracleDdlParser extends AntlrDdlParser<PlSqlLexer, PlSqlParser> {

    private String catalogName;
    private String schemaName;

    public OracleDdlParser() {
        super(true);
    }

    public OracleDdlParser(boolean throwErrorsFromTreeWalk, final String catalogName, final String schemaName) {
        super(throwErrorsFromTreeWalk);
        this.catalogName = catalogName;
        this.schemaName = schemaName;
    }

    @Override
    public void parse(String ddlContent, Tables databaseTables) {
        if (!ddlContent.endsWith(";")) {
            ddlContent = ddlContent + ";";
        }
        super.parse(toUpperCase(ddlContent), databaseTables);
    }

    @Override
    public ParseTree parseTree(PlSqlParser parser) {
        return parser.unit_statement();
    }

    @Override
    protected AntlrDdlParserListener createParseTreeWalkerListener() {
        return new OracleDdlParserListener(catalogName, schemaName, this);
    }

    @Override
    protected PlSqlLexer createNewLexerInstance(CharStream charStreams) {
        return new PlSqlLexer(charStreams);
    }

    @Override
    protected PlSqlParser createNewParserInstance(CommonTokenStream commonTokenStream) {
        return new PlSqlParser(commonTokenStream);
    }

    @Override
    protected boolean isGrammarInUpperCase() {
        return true;
    }

    @Override
    protected DataTypeResolver initializeDataTypeResolver() {
        // todo, register all and use in ColumnDefinitionParserListener
        DataTypeResolver.Builder dataTypeResolverBuilder = new DataTypeResolver.Builder();

        dataTypeResolverBuilder.registerDataTypes(
                PlSqlParser.Native_datatype_elementContext.class.getCanonicalName(), Arrays.asList(
                        new DataTypeEntry(Types.NUMERIC, PlSqlParser.INT),
                        new DataTypeEntry(Types.NUMERIC, PlSqlParser.INTEGER),
                        new DataTypeEntry(Types.NUMERIC, PlSqlParser.SMALLINT),
                        new DataTypeEntry(Types.NUMERIC, PlSqlParser.NUMERIC),
                        new DataTypeEntry(Types.NUMERIC, PlSqlParser.DECIMAL),
                        new DataTypeEntry(Types.NUMERIC, PlSqlParser.NUMBER),

                        new DataTypeEntry(Types.TIMESTAMP, PlSqlParser.DATE),
                        new DataTypeEntry(OracleTypes.TIMESTAMPLTZ, PlSqlParser.TIMESTAMP),
                        new DataTypeEntry(OracleTypes.TIMESTAMPTZ, PlSqlParser.TIMESTAMP),
                        new DataTypeEntry(Types.TIMESTAMP, PlSqlParser.TIMESTAMP),

                        new DataTypeEntry(Types.VARCHAR, PlSqlParser.VARCHAR2),
                        new DataTypeEntry(Types.VARCHAR, PlSqlParser.VARCHAR),
                        new DataTypeEntry(Types.NVARCHAR, PlSqlParser.NVARCHAR2),
                        new DataTypeEntry(Types.CHAR, PlSqlParser.CHAR),
                        new DataTypeEntry(Types.NCHAR, PlSqlParser.NCHAR),

                        new DataTypeEntry(OracleTypes.BINARY_FLOAT, PlSqlParser.BINARY_FLOAT),
                        new DataTypeEntry(OracleTypes.BINARY_DOUBLE, PlSqlParser.BINARY_DOUBLE),
                        new DataTypeEntry(Types.FLOAT, PlSqlParser.FLOAT),
                        new DataTypeEntry(Types.FLOAT, PlSqlParser.REAL),
                        new DataTypeEntry(Types.BLOB, PlSqlParser.BLOB),
                        new DataTypeEntry(Types.CLOB, PlSqlParser.CLOB)));
        return dataTypeResolverBuilder.build();
    }

    @Override
    protected SystemVariables createNewSystemVariablesInstance() {
        // todo implement
        return null;
    }

    @Override
    public void setCurrentDatabase(String databaseName) {
        this.catalogName = databaseName;
    }

    @Override
    public void setCurrentSchema(String schemaName) {
        this.schemaName = schemaName;
    }

    @Override
    public SystemVariables systemVariables() {
        throw new UnsupportedOperationException("Not implemented yet");
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

    // TODO excluded quoted identifiers
    private String toUpperCase(String ddl) {
        return ddl.toUpperCase(Locale.ENGLISH);
    }
}
