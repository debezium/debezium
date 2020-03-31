/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.filter;

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

    @Override
    public boolean eval(ConnectRecord<?> record) {
        final Bindings binding = engine.createBindings();
        binding.put("key", record.key());
        binding.put("value", record.value());
        binding.put("keySchema", record.keySchema());
        binding.put("valueSchema", record.valueSchema());
        try {
            final Object result = script != null ? script.eval(binding) : engine.eval(expression, binding);
            if (result instanceof Boolean) {
                return (Boolean) result;
            }
            else {
                throw new DebeziumException("Value '" + result + "' returned by the condition is not a boolean");
            }
        }
        catch (Exception e) {
            throw new DebeziumException("Error while evaluating expression '" + expression + "' for record '" + record + "'", e);
        }
    }

}
