/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import io.debezium.annotation.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Responsible for selecting the Kafka topic that the record will get send to.
 */
public class CassandraTopicSelector {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraTopicSelector.class);

    private static final String DEFAULT_DELIMITER = ".";

    private final String prefix;
    private final String delimiter;
    private final TableTopicNamer tableTopicNamer;

    private CassandraTopicSelector(String prefix, String delimiter, TableTopicNamer tableTopicNamer) {
        this.prefix = prefix;
        this.delimiter = delimiter;
        this.tableTopicNamer = new TopicNameCache(new TopicNameSanitizer(tableTopicNamer));
    }

    static CassandraTopicSelector defaultSelector(String topicPrefix) {
        return new CassandraTopicSelector(
                topicPrefix,
                DEFAULT_DELIMITER,
                (keyspaceTable, prefix, delimiter) -> String.join(delimiter, prefix, keyspaceTable.keyspace, keyspaceTable.table));
    }

    String topicNameFor(KeyspaceTable keyspaceTable) {
        return tableTopicNamer.topicNameFor(keyspaceTable, prefix, delimiter);
    }

    @FunctionalInterface
    public interface TableTopicNamer {
        String topicNameFor(KeyspaceTable keyspaceTable, String prefix, String delimiter);
    }

    /**
     * A topic namer that replaces any characters invalid in a topic name with {@code _}.
     */
    private static class TopicNameSanitizer implements TableTopicNamer {
        private static final String REPLACEMENT_CHAR = "_";
        private final TableTopicNamer delegate;

        TopicNameSanitizer(TableTopicNamer delegate) {
            this.delegate = delegate;
        }

        @Override
        public String topicNameFor(KeyspaceTable keyspaceTable, String prefix, String delimiter) {
            String topicName = delegate.topicNameFor(keyspaceTable, prefix, delimiter);

            StringBuilder sanitizedNameBuilder = new StringBuilder(topicName.length());
            boolean changed = false;

            for (int i = 0; i < topicName.length(); i++) {
                char c = topicName.charAt(i);
                if (isValidTopicNameCharacter(c)) {
                    sanitizedNameBuilder.append(c);
                } else {
                    sanitizedNameBuilder.append(REPLACEMENT_CHAR);
                    changed = true;
                }
            }

            if (changed) {
                String sanitizedName = sanitizedNameBuilder.toString();
                LOGGER.warn("Topic '{}' name isn't a valid topic, replacing it with '{}'", topicName, sanitizedName);
                return sanitizedName;
            } else {
                return topicName;
            }
        }

        /**
         * Whether the given character is a legal character of a Kafka topic name. Legal characters are
         * {@code [a-zA-Z0-9._-]}.
         *
         * {@see https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/internals/Topic.java}
         */
        private boolean isValidTopicNameCharacter(char c) {
            return c == '.' || c == '_' || c == '-' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')  || (c >= '0' && c <= '9');
        }
    }

    @ThreadSafe
    private static class TopicNameCache implements TableTopicNamer {
        private final ConcurrentHashMap<KeyspaceTable, String> topicNames;
        private final TableTopicNamer delegate;

        TopicNameCache(TableTopicNamer delegate) {
            this.topicNames = new ConcurrentHashMap<>();
            this.delegate = delegate;
        }

        @Override
        public String topicNameFor(KeyspaceTable keyspaceTable, String prefix, String delimiter) {
            return topicNames.computeIfAbsent(keyspaceTable, i -> delegate.topicNameFor(i, prefix, delimiter));
        }
    }
}
