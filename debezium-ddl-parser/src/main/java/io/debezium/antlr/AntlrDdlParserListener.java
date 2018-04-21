/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.antlr;

import io.debezium.text.ParsingException;
import org.antlr.v4.runtime.tree.ParseTreeListener;

import java.util.Collection;

/**
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public interface AntlrDdlParserListener extends ParseTreeListener {

    Collection<ParsingException> getErrors();

}
