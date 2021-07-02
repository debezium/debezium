/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.antlr.listener;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTreeListener;

import io.debezium.antlr.AntlrDdlParserListener;
import io.debezium.antlr.ProxyParseTreeListenerUtil;
import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParserBaseListener;
import io.debezium.text.ParsingException;

/**
 * This class is Oracle main DDL parser listener class.
 * It instantiates supported listeners, walks listeners through every parsing rule and collects parsing exceptions.
 *
 */
public class OracleDdlParserListener extends PlSqlParserBaseListener implements AntlrDdlParserListener {

    private final List<ParseTreeListener> listeners = new CopyOnWriteArrayList<>();
    private final Collection<ParsingException> errors = new ArrayList<>();

    public OracleDdlParserListener(final String catalogName, final String schemaName,
                                   final OracleDdlParser parser) {
        listeners.add(new CreateTableParserListener(catalogName, schemaName, parser, listeners));
        listeners.add(new AlterTableParserListener(catalogName, schemaName, parser, listeners));
        listeners.add(new DropTableParserListener(catalogName, schemaName, parser));
        listeners.add(new CommentParserListener(catalogName, schemaName, parser));
    }

    @Override
    public Collection<ParsingException> getErrors() {
        return errors;
    }

    @Override
    public void enterEveryRule(ParserRuleContext ctx) {
        ProxyParseTreeListenerUtil.delegateEnterRule(ctx, listeners, errors);
    }

    @Override
    public void exitEveryRule(ParserRuleContext ctx) {
        ProxyParseTreeListenerUtil.delegateExitRule(ctx, listeners, errors);
    }
}
