/*
 * Copyright (c) 2012-2017 The ANTLR Project. All rights reserved.
 *
 * Licensed under the BSD 3-clause license, available at https://raw.githubusercontent.com/antlr/antlr4/master/LICENSE.txt
 */

package io.debezium.antlr;

import io.debezium.text.ParsingException;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.Collection;

/**
 * Instances of this class allows multiple listeners to receive events
 * while walking the parse tree. For example:
 * <p>
 * <pre>
 * ProxyParseTreeListener proxy = new ProxyParseTreeListener();
 * ParseTreeListener listener1 = ... ;
 * ParseTreeListener listener2 = ... ;
 * proxy.add( listener1 );
 * proxy.add( listener2 );
 * ParseTreeWalker.DEFAULT.walk( proxy, ctx );
 * </pre>
 */
public class ProxyParseTreeListener {

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
