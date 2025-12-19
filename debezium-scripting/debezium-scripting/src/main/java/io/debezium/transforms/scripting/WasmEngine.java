/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.scripting;

import static io.debezium.transforms.ScriptingTransformation.CHICORY_ENGINE;
import static io.debezium.transforms.ScriptingTransformation.CHICORY_INTERPRETER_ENGINE;

import java.net.URI;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.header.Header;

import com.dylibso.chicory.wasm.Parser;

import io.debezium.DebeziumException;
import io.debezium.transforms.scripting.wasm.ChicoryEngine;

/**
 * An implementation of the expression language evaluator based on Chicory.
 */
public class WasmEngine implements Engine {
    private boolean compiler;
    private String expression;
    private ChicoryEngine engine;

    @Override
    public void configure(String language, String expression) {
        Objects.requireNonNull(language);
        Objects.requireNonNull(expression);
        this.expression = expression;
        switch (language) {
            case CHICORY_ENGINE:
                compiler = true;
                break;
            case CHICORY_INTERPRETER_ENGINE:
                compiler = false;
                break;
            default:
                throw new DebeziumException("Attempted to use unsupported Wasm Engine: '" + language + "', currently we support 'chicory'");
        }

        // reusing the "expression" configuration to load from the disk the .wasm file
        engine = ChicoryEngine.builder()
                .withCompiler(compiler)
                .withWasmModule(Parser.parse(Path.of(URI.create(expression))))
                .build();
    }

    protected Map<String, Object> getBindings(ConnectRecord<?> record) {
        final Map<String, Object> bindings = new HashMap<>();

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
        Map<String, Object> bindings = getBindings(record);

        try {
            final Object result = engine.eval(bindings);
            if (result == null || type.isAssignableFrom(result.getClass())) {
                return (T) result;
            }
            else {
                throw new DebeziumException("Value '" + result + "' returned by the expression is not a " + type.getSimpleName());
            }
        }
        catch (Exception e) {
            throw new DebeziumException("Error while evaluating wasm file '" + expression + "' for record '" + record + "'", e);
        }
    }
}
