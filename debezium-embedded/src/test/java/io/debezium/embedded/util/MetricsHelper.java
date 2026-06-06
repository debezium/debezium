/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.util;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import io.debezium.DebeziumException;

/**
 * Utility class that provides common metrics-related functions.
 *
 * @author Chris Cranford
 */
public class MetricsHelper {

    public static <T> T getStreamingMetric(String connector, String server, String contextName, String metricName) {
        try {
            return getAttribute(getStreamingMetricsObjectName(connector, server, contextName), metricName);
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }
    }

    public static <T> T getStreamingMetric(String connector, String server, String contextName, String task, String database, String metricName) {
        try {
            return getAttribute(getStreamingMetricsObjectName(connector, server, contextName, task, database), metricName);
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }
    }

    public static <T> T getSnapshotMetric(String connector, String server, String metricName) {
        try {
            return getAttribute(getSnapshotMetricsObjectName(connector, server), metricName);
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }
    }

    public static <T> T getSnapshotMetric(String connector, String server, String task, String database, String metricName) {
        try {
            return getAttribute(getSnapshotMetricsObjectName(connector, server, task, database), metricName);
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }
    }

    public static <T> T getSnapshotMetric(String connector, String server, Map<String, String> props, String metricName) {
        try {
            return getAttribute(getSnapshotMetricsObjectName(connector, server, props), metricName);
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }
    }

    public static <T> T getSnapshotMetric(String connector, String server, String task, String database, Map<String, String> props, String metricName) {
        try {
            return getAttribute(getSnapshotMetricsObjectName(connector, server, task, database, props), metricName);
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }
    }

    public static ObjectName getSnapshotMetricsObjectName(String connector, String server) throws MalformedObjectNameException {
        return new ObjectName("debezium." + connector + ":type=connector-metrics,context=snapshot,server=" + server);
    }

    public static ObjectName getSnapshotMetricsObjectName(String connector, String server, String task, String database) throws MalformedObjectNameException {

        Map<String, String> props = new HashMap<>();
        props.put("task", task);
        props.put("database", database);

        return getSnapshotMetricsObjectName(connector, server, props);
    }

    public static ObjectName getSnapshotMetricsObjectName(String connector, String server, Map<String, String> props) throws MalformedObjectNameException {
        String additionalProperties = props.entrySet().stream()
                .filter(e -> e.getValue() != null)
                .map(e -> String.format("%s=%s", e.getKey(), e.getValue()))
                .collect(Collectors.joining(","));

        if (!additionalProperties.isEmpty()) {
            return new ObjectName("debezium." + connector + ":type=connector-metrics,context=snapshot,server=" + server + "," + additionalProperties);
        }

        return getSnapshotMetricsObjectName(connector, server);
    }

    public static ObjectName getSnapshotMetricsObjectName(String connector, String server, String task, String database, Map<String, String> props)
            throws MalformedObjectNameException {

        Map<String, String> taskAndDatabase = new HashMap<>();
        taskAndDatabase.put("task", task);
        taskAndDatabase.put("database", database);

        String additionalProperties = Stream.of(props.entrySet(), taskAndDatabase.entrySet()).flatMap(Set::stream)
                .filter(e -> e.getValue() != null)
                .map(e -> String.format("%s=%s", e.getKey(), e.getValue()))
                .collect(Collectors.joining(","));

        if (!additionalProperties.isEmpty()) {
            return new ObjectName("debezium." + connector + ":type=connector-metrics,context=snapshot,server=" + server + "," + additionalProperties);
        }

        return getSnapshotMetricsObjectName(connector, server);
    }

    public static ObjectName getStreamingMetricsObjectName(String connector, String server) throws MalformedObjectNameException {
        return getStreamingMetricsObjectName(connector, server, getStreamingNamespace());
    }

    public static ObjectName getStreamingMetricsObjectName(String connector, String server, String context, String task, String database)
            throws MalformedObjectNameException {

        Map<String, String> props = new HashMap<>();
        props.put("task", task);
        props.put("database", database);

        return getStreamingMetricsObjectName(connector, server, props);
    }

    public static ObjectName getStreamingMetricsObjectName(String connector, String server, String task, String database, Map<String, String> customTags)
            throws MalformedObjectNameException {

        Map<String, String> props = new HashMap<>();
        props.put("task", task);
        props.put("database", database);
        props.putAll(customTags);

        return getStreamingMetricsObjectName(connector, server, props);
    }

    public static ObjectName getStreamingMetricsObjectName(String connector, String server, String context) throws MalformedObjectNameException {
        return new ObjectName("debezium." + connector + ":type=connector-metrics,context=" + context + ",server=" + server);
    }

    public static ObjectName getStreamingMetricsObjectName(String connector, String server, String context, String task) throws MalformedObjectNameException {
        return new ObjectName("debezium." + connector + ":type=connector-metrics,context=" + context + ",server=" + server + ",task=" + task);
    }

    public static ObjectName getStreamingMetricsObjectName(String connector, String server, Map<String, String> props) throws MalformedObjectNameException {
        String additionalProperties = props.entrySet().stream()
                .filter(e -> e.getValue() != null)
                .map(e -> String.format("%s=%s", e.getKey(), e.getValue()))
                .collect(Collectors.joining(","));

        if (!additionalProperties.isEmpty()) {
            return new ObjectName(
                    "debezium." + connector + ":type=connector-metrics,context=" + getStreamingNamespace() + ",server=" + server + "," + additionalProperties);
        }

        return getStreamingMetricsObjectName(connector, server);
    }

    public static String getStreamingNamespace() {
        return System.getProperty("test.streaming.metrics.namespace", "streaming");
    }

    @SuppressWarnings("unchecked")
    private static <T> T getAttribute(ObjectName objectName, String metricName) throws Exception {
        return (T) ManagementFactory.getPlatformMBeanServer().getAttribute(objectName, metricName);
    }
}
