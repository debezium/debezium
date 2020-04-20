/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.filter;

import java.util.ArrayList;
import java.util.List;

import javax.script.Bindings;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyObject;

/**
 * An implementation of the expression language evaluator based on GraalVM. Key
 * and value structs are exposed as {@code ProxyObject}s, allowing for
 * simplified property references.
 *
 * @author Jiri Pechanec
 *
 * @see https://www.graalvm.org/sdk/javadoc/org/graalvm/polyglot/proxy/ProxyObject.html
 */
public class GraalJsEngine extends Jsr223Engine {

    @Override
    protected void configureEngine() {
    }

    @Override
    protected Bindings getBindings(ConnectRecord<?> record) {
        Bindings bindings = engine.createBindings();

        bindings.put("key", asProxyObject((Struct) record.key()));
        bindings.put("value", asProxyObject((Struct) record.value()));
        bindings.put("keySchema", record.keySchema());
        bindings.put("valueSchema", record.valueSchema());

        return bindings;
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
}
