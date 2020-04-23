/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.scripting;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.kafka.connect.connector.ConnectRecord;

import io.debezium.DebeziumException;

/**
 * An implementation of the expression language evaluator based on JSR 223 scripting languages.
 * The expression receives variables to work with
 * <ul>
 * <li>key - key of the record</li>
 * <li>value - value of the record</li>
 * <li>keySchema - schema for key</li>
 * <li>valueSchema - schema for value</li>
 * </ul>
 *
 * @author Jiri Pechanec
 */
public class Jsr223Engine implements Engine {

    private String expression;
    private CompiledScript script;
    protected ScriptEngine engine;

    @Override
    public void configure(String language, String expression) {
        this.expression = expression;
        final ScriptEngineManager factory = new ScriptEngineManager();
        engine = factory.getEngineByName(language);
        if (engine == null) {
            throw new DebeziumException("Implementation of language '" + language + "' not found on the classpath");
        }
        configureEngine();

        if (engine instanceof Compilable) {
            try {
                script = ((Compilable) engine).compile(expression);
            }
            catch (ScriptException e) {
                throw new DebeziumException(e);
            }
        }
    }

    protected void configureEngine() {
    }

    protected Bindings getBindings(ConnectRecord<?> record) {
        final Bindings bindings = engine.createBindings();

        bindings.put("key", record.key());
        bindings.put("value", record.value());
        bindings.put("keySchema", record.keySchema());
        bindings.put("valueSchema", record.valueSchema());

        return bindings;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T eval(ConnectRecord<?> record, Class<T> type) {
        Bindings bindings = getBindings(record);

        try {
            final Object result = script != null ? script.eval(bindings) : engine.eval(expression, bindings);
            if (result == null || type.isAssignableFrom(result.getClass())) {
                return (T) result;
            }
            else {
                throw new DebeziumException("Value '" + result + "' returned by the expression is not a " + type.getSimpleName());
            }
        }
        catch (Exception e) {
            throw new DebeziumException("Error while evaluating expression '" + expression + "' for record '" + record + "'", e);
        }
    }
}
