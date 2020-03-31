/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.filter;

import javax.script.Bindings;

/**
 * An implementation of the expression language evaluator based GraalVM.
 * The engine needs to be configured to allow calling of Java methods, see <a href="https://github.com/graalvm/graaljs/blob/master/docs/user/ScriptEngine.md#setting-options-via-bindings">Graal JS docs</a>.
 *
 * @author Jiri Pechanec
 */
public class GraalJsEngine extends Jsr223Engine {

    protected void configureEngine() {
        final Bindings binding = engine.createBindings();
        binding.put("polyglot.js.allowAllAccess", true);
    }
}
