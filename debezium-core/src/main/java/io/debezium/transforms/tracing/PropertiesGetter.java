/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.tracing;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import io.debezium.DebeziumException;
import io.opentelemetry.context.propagation.TextMapGetter;

public enum PropertiesGetter implements TextMapGetter<Properties> {
    INSTANCE;

    @Override
    public Iterable<String> keys(Properties properties) {
        return properties.keySet().stream().map(Object::toString).collect(Collectors.toSet());
    }

    @Override
    public String get(Properties properties, String s) {
        if (Objects.nonNull(properties) && properties.containsKey(s)) {
            return (String) properties.get(s);
        }
        return null;
    }

    public static Properties extract(String span) {
        Properties props = new Properties();
        try (Reader sr = new StringReader(span)) {
            props.load(sr);
            return props;
        }
        catch (IOException e) {
            throw new DebeziumException(e);
        }
    }

}
