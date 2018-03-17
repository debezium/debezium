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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;

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
public class ProxyParseTreeListener implements ParseTreeListener {
    private List<ParseTreeListener> listeners;

    private Collection<ParsingException> errors = new ArrayList<>();
    private final BiFunction<ParsingException, Collection<ParsingException>, Collection<ParsingException>> accumulateError;
    /**
     * Creates a new proxy without an empty list of listeners. Add
     * listeners before walking the tree.
     */
    public ProxyParseTreeListener(BiFunction<ParsingException, Collection<ParsingException>, Collection<ParsingException>> accumulateError) {
        // Setting the listener to null automatically instantiates a new list.
        this(null, accumulateError);
    }

    /**
     * Creates a new proxy with the given list of listeners.
     *
     * @param listeners A list of listerners to receive events.
     */
    public ProxyParseTreeListener(List<ParseTreeListener> listeners, BiFunction<ParsingException, Collection<ParsingException>, Collection<ParsingException>> accumulateError) {
        this.accumulateError = accumulateError;
        setListeners(listeners);
    }

    @Override
    public void enterEveryRule(ParserRuleContext ctx) {
        for (ParseTreeListener listener : getListeners()) {
            try {
                listener.enterEveryRule(ctx);
                ctx.enterRule(listener);
            } catch (ParsingException parsingException) {
                accumulateError.apply(parsingException, errors);
            }
        }
    }

    @Override
    public void exitEveryRule(ParserRuleContext ctx) {
        for (ParseTreeListener listener : getListeners()) {
            try {
                ctx.exitRule(listener);
                listener.exitEveryRule(ctx);
            } catch (ParsingException parsingException) {
                accumulateError.apply(parsingException, errors);
            }
        }
    }

    @Override
    public void visitErrorNode(ErrorNode node) {
        for (ParseTreeListener listener : getListeners()) {
            try {
                listener.visitErrorNode(node);
            } catch (ParsingException parsingException) {
                accumulateError.apply(parsingException, errors);
            }
        }
    }

    @Override
    public void visitTerminal(TerminalNode node) {
        for (ParseTreeListener listener : getListeners()) {
            try {
                listener.visitTerminal(node);
            } catch (ParsingException parsingException) {
                accumulateError.apply(parsingException, errors);
            }
        }
    }

    /**
     * Adds the given listener to the list of event notification recipients.
     *
     * @param listener A listener to begin receiving events.
     */
    public void add(ParseTreeListener listener) {
        getListeners().add(listener);
    }

    /**
     * Removes the given listener to the list of event notification recipients.
     *
     * @param listener A listener to stop receiving events.
     * @return false The listener was not registered to receive events.
     */
    public boolean remove(ParseTreeListener listener) {
        return getListeners().remove(listener);
    }

    /**
     * Returns the list of listeners.
     *
     * @return The list of listeners to receive tree walking events.
     */
    private List<ParseTreeListener> getListeners() {
        return this.listeners;
    }

    /**
     * Changes the list of listeners to receive events. If the given list of
     * listeners is null, an empty list will be created.
     *
     * @param listeners A list of listeners to receive tree walking
     *                  events.
     */
    public void setListeners(List<ParseTreeListener> listeners) {
        if (listeners == null) {
            listeners = createParseTreeListenerList();
        }

        this.listeners = listeners;
    }

    /**
     * Creates a CopyOnWriteArrayList to permit concurrent mutative
     * operations.
     *
     * @return A thread-safe, mutable list of event listeners.
     */
    protected List<ParseTreeListener> createParseTreeListenerList() {
        return new CopyOnWriteArrayList<ParseTreeListener>();
    }

    /**
     * Returns all caught errors during tree walk.
     *
     * @return list of Parsing exceptions
     */
    public Collection<ParsingException> getErrors() {
        return errors;
    }
}