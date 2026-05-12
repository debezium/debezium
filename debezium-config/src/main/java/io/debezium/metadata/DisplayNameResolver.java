/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Derives human-readable display names from fully-qualified component class names.
 * <p>
 * The algorithm extracts the simple class name, strips mechanical suffixes based on the
 * component type, splits CamelCase into words, applies brand-name merging and abbreviation
 * expansion, then assembles a display name with an appropriate prefix and type suffix.
 */
public class DisplayNameResolver {

    private static final String SOURCE_CONNECTOR_TYPE = "source-connector";
    private static final String SINK_CONNECTOR_TYPE = "sink-connector";
    private static final String SERVER_SINK_TYPE = "server-sink";
    private static final String CONVERTER_TYPE = "converter";
    private static final String CUSTOM_CONVERTER_TYPE = "custom-converter";

    private static final List<BrandSequence> BRAND_SEQUENCES = List.of(
            new BrandSequence(new String[]{ "Nats", "Jet", "Stream" }, "NATS JetStream"),
            new BrandSequence(new String[]{ "Pub", "Sub", "Lite" }, "PubSub Lite"),
            new BrandSequence(new String[]{ "Mongo", "Db" }, "MongoDB"),
            new BrandSequence(new String[]{ "My", "Sql" }, "MySQL"),
            new BrandSequence(new String[]{ "Maria", "Db" }, "MariaDB"),
            new BrandSequence(new String[]{ "Sql", "Server" }, "SQL Server"),
            new BrandSequence(new String[]{ "Rabbit", "Mq" }, "RabbitMQ"),
            new BrandSequence(new String[]{ "Rocket", "Mq" }, "RocketMQ"),
            new BrandSequence(new String[]{ "Pub", "Sub" }, "PubSub"),
            new BrandSequence(new String[]{ "Event", "Hubs" }, "Event Hubs"),
            new BrandSequence(new String[]{ "Instruct", "Lab" }, "InstructLab"),
            new BrandSequence(new String[]{ "Jet", "Stream" }, "JetStream"),
            new BrandSequence(new String[]{ "Redis", "Stream" }, "Redis Stream"));

    private static final Map<String, String> WORD_REPLACEMENTS = Map.ofEntries(
            Map.entry("Jdbc", "JDBC"),
            Map.entry("Http", "HTTP"),
            Map.entry("Sqs", "SQS"),
            Map.entry("Sns", "SNS"),
            Map.entry("Nats", "NATS"),
            Map.entry("Db2", "DB2"),
            Map.entry("Ssl", "SSL"),
            Map.entry("Sql", "SQL"),
            Map.entry("Json", "JSON"),
            Map.entry("Mq", "MQ"));

    private static final Map<String, List<String>> TYPE_SUFFIXES = Map.of(
            SOURCE_CONNECTOR_TYPE, List.of("Connector"),
            SINK_CONNECTOR_TYPE, List.of("SinkConnector"),
            SERVER_SINK_TYPE, List.of("ChangeConsumer", "SinkConsumer"),
            CONVERTER_TYPE, List.of("Converter"),
            CUSTOM_CONVERTER_TYPE, List.of("Converter"));

    private static final Map<String, String> TYPE_DISPLAY_SUFFIXES = Map.of(
            SOURCE_CONNECTOR_TYPE, "Connector",
            SINK_CONNECTOR_TYPE, "Sink Connector",
            SERVER_SINK_TYPE, "Server Sink",
            CONVERTER_TYPE, "Converter",
            CUSTOM_CONVERTER_TYPE, "Custom Converter");

    private DisplayNameResolver() {
    }

    /**
     * Derives a human-readable display name from a fully-qualified class name and component type.
     *
     * @param className the fully-qualified class name
     * @param type the component type (e.g. "source-connector", "server-sink", "transformation")
     * @return a human-readable display name
     */
    public static String resolve(String className, String type) {
        String simpleName = className.substring(className.lastIndexOf('.') + 1);

        String innerClassSuffix = "";
        int dollarIndex = simpleName.indexOf('$');
        if (dollarIndex >= 0) {
            innerClassSuffix = " (" + simpleName.substring(dollarIndex + 1) + ")";
            simpleName = simpleName.substring(0, dollarIndex);
        }

        simpleName = stripTypeSuffix(simpleName, type);

        List<String> words = splitCamelCase(simpleName);
        words = mergeBrandNames(words);
        words = words.stream()
                .map(w -> WORD_REPLACEMENTS.getOrDefault(w, w))
                .collect(Collectors.toList());

        String prefix = className.startsWith("org.apache.kafka") ? "Kafka" : "Debezium";
        String typeSuffix = TYPE_DISPLAY_SUFFIXES.getOrDefault(type, "");

        StringBuilder result = new StringBuilder(prefix);
        for (String word : words) {
            result.append(' ').append(word);
        }
        if (!innerClassSuffix.isEmpty()) {
            result.append(innerClassSuffix);
        }
        if (!typeSuffix.isEmpty()) {
            result.append(' ').append(typeSuffix);
        }
        return result.toString();
    }

    private static String stripTypeSuffix(String simpleName, String type) {
        List<String> suffixes = TYPE_SUFFIXES.getOrDefault(type, List.of());
        for (String suffix : suffixes) {
            if (simpleName.endsWith(suffix) && simpleName.length() > suffix.length()) {
                return simpleName.substring(0, simpleName.length() - suffix.length());
            }
        }
        return simpleName;
    }

    static List<String> splitCamelCase(String name) {
        List<String> words = new ArrayList<>();
        int start = 0;
        for (int i = 1; i < name.length(); i++) {
            char prev = name.charAt(i - 1);
            char curr = name.charAt(i);

            if (Character.isLowerCase(prev) && Character.isUpperCase(curr)) {
                words.add(name.substring(start, i));
                start = i;
            }
            else if (Character.isUpperCase(prev) && Character.isUpperCase(curr)
                    && i + 1 < name.length() && Character.isLowerCase(name.charAt(i + 1))) {
                words.add(name.substring(start, i));
                start = i;
            }
        }
        if (start < name.length()) {
            words.add(name.substring(start));
        }
        return words;
    }

    static List<String> mergeBrandNames(List<String> words) {
        List<String> result = new ArrayList<>(words);
        for (BrandSequence brand : BRAND_SEQUENCES) {
            String[] sequence = brand.words;
            int seqLen = sequence.length;
            for (int i = 0; i <= result.size() - seqLen; i++) {
                boolean match = true;
                for (int j = 0; j < seqLen; j++) {
                    if (!result.get(i + j).equals(sequence[j])) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    result.set(i, brand.displayForm);
                    for (int j = 1; j < seqLen; j++) {
                        result.remove(i + 1);
                    }
                    break;
                }
            }
        }
        return result;
    }

    private record BrandSequence(String[] words, String displayForm) {
    }
}
