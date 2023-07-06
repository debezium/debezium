/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.internal;

import io.debezium.DebeziumException;
import io.opentelemetry.context.propagation.TextMapSetter;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public class DataMapTracingSetter implements TextMapSetter<Map<String, Object>> {

    private DataMapTracingSetter() {}

    public static DataMapTracingSetter create() {
        return new DataMapTracingSetter();
    }

    @Override
    public void set(@Nullable Map<String, Object> dataMap, @Nonnull String key, @Nonnull String value) {
        Properties props = new Properties();
        props.put(key, value);
        String context = export(props);
        if (Objects.nonNull(dataMap)) {
            dataMap.put(OutboxConstants.TRACING_SPAN_CONTEXT, context);
        }
    }

    private static String export(Properties props) {
        try (Writer sw = new StringWriter()) {
            props.store(sw, null);
            return sw.toString();
        }
        catch (IOException e) {
            throw new DebeziumException(e);
        }
    }
}
