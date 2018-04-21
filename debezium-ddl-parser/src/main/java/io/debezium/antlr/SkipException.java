/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.antlr;

import org.antlr.v4.runtime.ParserRuleContext;

/**
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class SkipException extends RuntimeException {

    private Class<? extends ParserRuleContext> ctxClass;

    public SkipException(Class<? extends ParserRuleContext> ctxClass) {
        this.ctxClass = ctxClass;
    }

    public Class<? extends ParserRuleContext> getCtxClass() {
        return ctxClass;
    }
}
