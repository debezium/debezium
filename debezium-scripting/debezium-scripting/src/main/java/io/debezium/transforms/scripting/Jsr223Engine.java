/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.scripting;

import java.util.HashMap;
import java.util.Map;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.header.Header;

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

        bindings.put("key", key(record));
        bindings.put("value", value(record));
        bindings.put("keySchema", record.keySchema());
        bindings.put("valueSchema", record.valueSchema());
        bindings.put("topic", record.topic());
        bindings.put("header", headers(record));

        return bindings;
    }

    protected Object key(ConnectRecord<?> record) {
        return record.key();
    }

    protected Object value(ConnectRecord<?> record) {
        return record.value();
    }

    protected RecordHeader header(Header header) {
        return new RecordHeader(header.schema(), header.value());
    }

    protected Object headers(ConnectRecord<?> record) {
        return doHeaders(record);
    }

    protected Map<String, RecordHeader> doHeaders(ConnectRecord<?> record) {
        final Map<String, RecordHeader> headers = new HashMap<>();
        for (Header header : record.headers()) {
            headers.put(header.key(), header(header));
        }
        return headers;
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
