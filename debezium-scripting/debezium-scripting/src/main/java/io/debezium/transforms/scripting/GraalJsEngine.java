/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.scripting;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.script.Bindings;
import javax.script.ScriptContext;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyObject;

/**
 * An implementation of the expression language evaluator based on GraalVM. Key
 * and value structs are exposed as {@code ProxyObject}s, allowing for
 * simplified property references.
 *
 * @author Jiri Pechanec
 *
 * @link https://www.graalvm.org/sdk/javadoc/org/graalvm/polyglot/proxy/ProxyObject.html
 */
public class GraalJsEngine extends Jsr223Engine {

    @Override
    protected void configureEngine() {
        final Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
        bindings.put("polyglot.js.allowHostAccess", true);
    }

    @Override
    protected Object key(ConnectRecord<?> record) {
        return asProxyObject((Struct) record.key());
    }

    @Override
    protected Object value(ConnectRecord<?> record) {
        return asProxyObject((Struct) record.value());
    }

    @Override
    protected Object headers(ConnectRecord<?> record) {
        return asProxyObject(doHeaders(record));
    }

    @Override
    protected RecordHeader header(Header header) {
        if (header.value() instanceof Struct) {
            return new RecordHeader(header.schema(), asProxyObject((Struct) header.value()));
        }
        return super.header(header);
    }

    /**
     * Exposes the given struct as a {@link ProxyObject}, allowing for simplified
     * property references, also providing any write access.
     */
    private ProxyObject asProxyObject(Struct struct) {
        return new ProxyObject() {

            @Override
            public void putMember(String key, Value value) {
                throw new UnsupportedOperationException("Record attributes must not be modified from within this transformation");
            }

            @Override
            public boolean hasMember(String key) {
                return struct.schema().field(key) != null;
            }

            @Override
            public Object getMemberKeys() {
                List<String> fieldNames = new ArrayList<>(struct.schema().fields().size());

                for (Field field : struct.schema().fields()) {
                    fieldNames.add(field.name());
                }

                return fieldNames;
            }

            @Override
            public Object getMember(String key) {
                Object value = struct.get(key);

                if (value instanceof Struct) {
                    return asProxyObject((Struct) value);
                }

                return value;
            }
        };
    }

    /**
     * Exposes the given Map as a {@link ProxyObject}, allowing for simplified
     * property reference.
     */
    private ProxyObject asProxyObject(Map<String, ?> map) {
        return new ProxyObject() {

            @Override
            public void putMember(String key, Value value) {
                throw new UnsupportedOperationException("Record attributes must not be modified from within this transformation");
            }

            @Override
            public boolean hasMember(String key) {
                return map.containsKey(key);
            }

            @Override
            public Object getMemberKeys() {
                return map.keySet();
            }

            @Override
            public Object getMember(String key) {
                return map.get(key);
            }
        };
    }
}
