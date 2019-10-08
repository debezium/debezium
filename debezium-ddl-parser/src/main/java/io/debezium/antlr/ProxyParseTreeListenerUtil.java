/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
/*
 * Copyright (c) 2012-2017 The ANTLR Project. All rights reserved.
 *
 * Licensed under the BSD 3-clause license, available at https://raw.githubusercontent.com/antlr/antlr4/master/LICENSE.txt
 */

package io.debezium.antlr;

import java.util.Collection;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.antlr.v4.runtime.tree.TerminalNode;

import io.debezium.text.ParsingException;

/**
 * Utility class that implements a helper method for achieving a delegation of parsed rules to multiple listeners.
 *
 * For example implementation of proxy parse tree listener see
 * <a href="https://github.com/antlr/antlr4/issues/841">ANTLR issue</a> about it.
 */
public class ProxyParseTreeListenerUtil {

    private ProxyParseTreeListenerUtil() {
        // instance of util class should never exist
    }

    /**
     * Delegates enter rule event to collection of parsing listeners and captures parsing exceptions that may appear.
     *
     * @param ctx enter rule context; may not be null
     * @param listeners collection of listeners; may not be null
     * @param errors collection of errors; may not be null
     */
    public static void delegateEnterRule(ParserRuleContext ctx, Collection<ParseTreeListener> listeners, Collection<ParsingException> errors) {
        for (ParseTreeListener listener : listeners) {
            try {
                listener.enterEveryRule(ctx);
                ctx.enterRule(listener);
            }
            catch (ParsingException parsingException) {
                AntlrDdlParser.accumulateParsingFailure(parsingException, errors);
            }
        }
    }

    /**
     * Delegates exit rule event to collection of parsing listeners and captures parsing exceptions that may appear.
     *
     * @param ctx exit rule context; may not be null
     * @param listeners collection of listeners; may not be null
     * @param errors collection of errors; may not be null
     */
    public static void delegateExitRule(ParserRuleContext ctx, Collection<ParseTreeListener> listeners, Collection<ParsingException> errors) {
        for (ParseTreeListener listener : listeners) {
            try {
                ctx.exitRule(listener);
                listener.exitEveryRule(ctx);
            }
            catch (ParsingException parsingException) {
                AntlrDdlParser.accumulateParsingFailure(parsingException, errors);
            }
        }

    }

    /**
     * Delegates visit error node event to collection of parsing listeners and captures parsing exceptions that may appear.
     *
     * @param node error node; may not be null
     * @param listeners collection of listeners; may not be null
     * @param errors collection of errors; may not be null
     */
    public static void visitErrorNode(ErrorNode node, Collection<ParseTreeListener> listeners, Collection<ParsingException> errors) {
        for (ParseTreeListener listener : listeners) {
            try {
                listener.visitErrorNode(node);
            }
            catch (ParsingException parsingException) {
                AntlrDdlParser.accumulateParsingFailure(parsingException, errors);
            }
        }
    }

    /**
     * Delegates visit terminal event to collection of parsing listeners and captures parsing exceptions that may appear.
     *
     * @param node terminal node; may not be null
     * @param listeners collection of listeners; may not be null
     * @param errors collection of errors; may not be null
     */
    public static void visitTerminal(TerminalNode node, Collection<ParseTreeListener> listeners, Collection<ParsingException> errors) {
        for (ParseTreeListener listener : listeners) {
            try {
                listener.visitTerminal(node);
            }
            catch (ParsingException parsingException) {
                AntlrDdlParser.accumulateParsingFailure(parsingException, errors);
            }
        }
    }

}
