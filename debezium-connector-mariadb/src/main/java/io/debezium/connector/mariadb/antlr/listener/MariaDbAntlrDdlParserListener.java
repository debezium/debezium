/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.antlr.listener;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.antlr.v4.runtime.tree.TerminalNode;

import io.debezium.antlr.AntlrDdlParserListener;
import io.debezium.antlr.ProxyParseTreeListenerUtil;
import io.debezium.connector.mariadb.antlr.MariaDbAntlrDdlParser;
import io.debezium.ddl.parser.mariadb.generated.MariaDBParser;
import io.debezium.ddl.parser.mariadb.generated.MariaDBParserBaseListener;
import io.debezium.text.ParsingException;

/**
 * Parser listener for MariaDB column definition queries.<p></p>
 *
 * Its purpose is to delegate events to defined collections of concrete parser listeners. Each
 * listener handles the specified type of DDL statement.<p></p>
 *
 * This listener is catching all parsing exceptions and implements a skip mechanism for BEGIN ... END
 * statements. No event will be delegated during skipping phase.
 *
 * @author Chris Cranford
 */
public class MariaDbAntlrDdlParserListener extends MariaDBParserBaseListener implements AntlrDdlParserListener {

    private final List<ParseTreeListener> listeners = new CopyOnWriteArrayList<>();
    private final Collection<ParsingException> errors = new ArrayList<>();
    private boolean skipNodes;
    private int skippedNodesCount = 0;

    public MariaDbAntlrDdlParserListener(MariaDbAntlrDdlParser parser) {
        listeners.add(new CreateAndAlterDatabaseParserListener(parser));
        listeners.add(new DropDatabaseParserListener(parser));
        listeners.add(new CreateTableParserListener(parser, listeners));
        listeners.add(new AlterTableParserListener(parser, listeners));
        listeners.add(new DropTableParserListener(parser));
        listeners.add(new RenameTableParserListener(parser));
        listeners.add(new TruncateTableParserListener(parser));
        listeners.add(new CreateViewParserListener(parser, listeners));
        listeners.add(new AlterViewParserListener(parser, listeners));
        listeners.add(new DropViewParserListener(parser));
        listeners.add(new CreateUniqueIndexParserListener(parser));
        listeners.add(new SetStatementParserListener(parser));
        listeners.add(new UseStatementParserListener(parser));
    }

    @Override
    public Collection<ParsingException> getErrors() {
        return errors;
    }

    @Override
    public void enterEveryRule(ParserRuleContext ctx) {
        if (skipNodes) {
            skippedNodesCount++;
        }
        else {
            ProxyParseTreeListenerUtil.delegateEnterRule(ctx, listeners, errors);
        }
    }

    @Override
    public void exitEveryRule(ParserRuleContext ctx) {
        if (skipNodes) {
            if (skippedNodesCount == 0) {
                // back in the node where skipping started
                skipNodes = false;
            }
            else {
                // going up in a tree, means decreasing a number of skipped nodes
                skippedNodesCount--;
            }
        }
        else {
            ProxyParseTreeListenerUtil.delegateExitRule(ctx, listeners, errors);
        }
    }

    @Override
    public void visitErrorNode(ErrorNode node) {
        ProxyParseTreeListenerUtil.visitErrorNode(node, listeners, errors);
    }

    @Override
    public void visitTerminal(TerminalNode node) {
        ProxyParseTreeListenerUtil.visitTerminal(node, listeners, errors);
    }

    @Override
    public void enterRoutineBody(MariaDBParser.RoutineBodyContext ctx) {
        // this is a grammar rule for BEGIN ... END part of statements. Skip it.
        skipNodes = true;
    }
}
