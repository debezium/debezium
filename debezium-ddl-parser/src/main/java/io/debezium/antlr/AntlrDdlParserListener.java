/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.antlr;

import io.debezium.text.ParsingException;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.util.Collection;

/**
 * Interface for listeners used by {@link ParseTreeWalker}.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public interface AntlrDdlParserListener extends ParseTreeListener {

    /**
     * Returns errors that occurred during parsed tree walk.
     *
     * @return collection of {@link ParsingException}s.
     */
    Collection<ParsingException> getErrors();

}
