/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener;

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
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.text.ParsingException;

/**
 * Parser listener for MySQL column definition queries. Its purpose is to delegate events
 * to defined collection of concrete parser listeners. Each listener handles the specified type of DDL statement.
 * <p>
 * This listener is catching all occurred parsing exceptions and implements a skipping logic for BEGIN ... END
 * statements. No event will be delegated during skipping phase.
 */
public class MySqlAntlrDdlParserListener extends MySqlParserBaseListener implements AntlrDdlParserListener {

    /**
     * Collection of listeners for delegation of events.
     */
    private final List<ParseTreeListener> listeners = new CopyOnWriteArrayList<>();

    /**
     * Flag for skipping phase.
     */
    private boolean skipNodes;

    /**
     * Count of skipped nodes. Each enter event during skipping phase will increase the counter
     * and each exit event will decrease it. When counter will be decreased to 0, the skipping phase will end.
     */
    private int skippedNodesCount = 0;

    /**
     * Collection of catched exceptions.
     */
    private final Collection<ParsingException> errors = new ArrayList<>();

    public MySqlAntlrDdlParserListener(MySqlAntlrDdlParser parser) {
        // initialize listeners
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

    /**
     * Returns all caught errors during tree walk.
     *
     * @return list of Parsing exceptions
     */
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
    public void enterRoutineBody(MySqlParser.RoutineBodyContext ctx) {
        // this is a grammar rule for BEGIN ... END part of statements. Skip it.
        skipNodes = true;
    }
}
