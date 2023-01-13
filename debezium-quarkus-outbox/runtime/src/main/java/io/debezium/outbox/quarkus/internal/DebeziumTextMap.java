/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.internal;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;

import io.debezium.DebeziumException;
import io.opentracing.propagation.TextMap;

public class DebeziumTextMap implements TextMap {

    private final Properties props = new Properties();

    @Override
    public Iterator<Entry<String, String>> iterator() {
        return null;
    }

    @Override
    public void put(String key, String value) {
        props.put(key, value);
    }

    public String export() {
        try (Writer sw = new StringWriter()) {
            props.store(sw, null);
            return sw.toString();
        }
        catch (IOException e) {
            throw new DebeziumException(e);
        }
    }
}
