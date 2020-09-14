/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.antlr;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import io.debezium.antlr.AntlrDdlParser;
import io.debezium.antlr.AntlrDdlParserListener;
import io.debezium.antlr.DataTypeResolver;
import io.debezium.connector.oracle.antlr.listener.OracleDmlParserListener;
import io.debezium.connector.oracle.logminer.OracleChangeRecordValueConverter;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntry;
import io.debezium.ddl.parser.oracle.generated.PlSqlLexer;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.relational.SystemVariables;
import io.debezium.relational.Tables;

/**
 * This is the main Oracle Antlr DML parser
 */
public class OracleDmlParser extends AntlrDdlParser<PlSqlLexer, PlSqlParser> {

    protected final String catalogName;
    protected final String schemaName;
    private final OracleChangeRecordValueConverter converter;
    private LogMinerDmlEntry dmlEntry;

    public OracleDmlParser(boolean throwErrorsFromTreeWalk, final String catalogName, final String schemaName, OracleChangeRecordValueConverter converter) {
        super(throwErrorsFromTreeWalk);
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.converter = converter;
    }

    public LogMinerDmlEntry getDmlEntry() {
        return dmlEntry;
    }

    public void setDmlEntry(LogMinerDmlEntry dml) {
        this.dmlEntry = dml;
    }

    @Override
    public void parse(String dmlContent, Tables databaseTables) {
        if (!dmlContent.endsWith(";")) {
            dmlContent = dmlContent + ";";
        }
        // DML content is case sensitive
        super.parse(dmlContent, databaseTables);
    }

    @Override
    public ParseTree parseTree(PlSqlParser parser) {
        return parser.unit_statement();
    }

    @Override
    protected AntlrDdlParserListener createParseTreeWalkerListener() {
        return new OracleDmlParserListener(catalogName, schemaName, this);
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
        return null;
    }

    @Override
    protected SystemVariables createNewSystemVariablesInstance() {
        return null;
    }

    public OracleChangeRecordValueConverter getConverters() {
        return converter;
    }
}
