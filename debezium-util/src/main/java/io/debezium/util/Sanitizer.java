/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.util.regex.Pattern;

import javax.management.ObjectName;

/**
 * Utility for sanitizing values used to build JMX {@link ObjectName}s.
 * <p>
 * This is an internal replacement for {@code org.apache.kafka.common.utils.Sanitizer#jmxSanitize(String)}.
 * That class was relocated to {@code org.apache.kafka.common.utils.internals} by upstream KAFKA-20297,
 * which is not a stable API for connector code to depend on across Kafka versions.
 */
public class Sanitizer {

    private static final Pattern MBEAN_PATTERN = Pattern.compile("[\\w-%\\. \t]*");

    private Sanitizer() {
    }

    /**
     * Quote the value if it contains characters that are not safe to use unquoted in a JMX
     * {@link ObjectName}, otherwise return it unchanged.
     */
    public static String jmxSanitize(String value) {
        return MBEAN_PATTERN.matcher(value).matches() ? value : ObjectName.quote(value);
    }
}
