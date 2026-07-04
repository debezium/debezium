/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.sink.SinkTask;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;

import io.debezium.DebeziumException;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.SchemaEvolutionMode;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.naming.TemporaryBackwardCompatibleCollectionNamingStrategyProxy;
import io.debezium.metadata.CollectionId;
import io.debezium.sink.naming.CollectionNamingStrategy;
import io.debezium.sink.naming.DefaultCollectionNamingStrategy;
import io.debezium.util.Strings;

final class StartupCollectionValidator {

    private static final String TOPIC_PLACEHOLDER = "${topic}";
    private static final String SOURCE_PLACEHOLDER_PREFIX = "${source.";

    private StartupCollectionValidator() {
    }

    static void validate(JdbcSinkConnectorConfig config, SessionFactory sessionFactory, DatabaseDialect dialect, Map<String, String> props) {
        if (!config.getSchemaEvolutionMode().validateOnStartup()) {
            return;
        }

        final Set<CollectionId> collectionIds = resolveCollectionIds(config, dialect, props);
        try (StatelessSession validationSession = sessionFactory.openStatelessSession()) {
            for (CollectionId collectionId : collectionIds) {
                final boolean exists = validationSession.doReturningWork(connection -> dialect.tableExists(connection, collectionId));
                if (!exists) {
                    throw new DebeziumException(String.format(
                            "Target table '%s' does not exist, but '%s' is set to '%s'. "
                                    + "Create the target table before starting the connector or use '%s=%s'.",
                            collectionId.toFullIdentiferString(),
                            JdbcSinkConnectorConfig.SCHEMA_EVOLUTION,
                            SchemaEvolutionMode.NONE_VALIDATED.getValue(),
                            JdbcSinkConnectorConfig.SCHEMA_EVOLUTION,
                            SchemaEvolutionMode.BASIC.getValue()));
                }
            }
        }
    }

    static Set<CollectionId> resolveCollectionIds(JdbcSinkConnectorConfig config, DatabaseDialect dialect, Map<String, String> props) {
        return resolveCollectionNames(config, props)
                .stream()
                .map(collectionName -> {
                    final CollectionId collectionId = dialect.getCollectionId(collectionName);
                    if (collectionId == null) {
                        throw new DebeziumException("Unable to resolve target table name '" + collectionName + "' for startup validation.");
                    }
                    return collectionId;
                })
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    static Set<String> resolveCollectionNames(JdbcSinkConnectorConfig config, Map<String, String> props) {
        final String collectionNameFormat = config.getCollectionNameFormat();
        if (Strings.isNullOrEmpty(collectionNameFormat)) {
            throw new DebeziumException("'" + JdbcSinkConnectorConfig.SCHEMA_EVOLUTION + "=" + SchemaEvolutionMode.NONE_VALIDATED.getValue()
                    + "' requires '" + JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT + "' to be configured.");
        }

        if (!usesDefaultCollectionNamingStrategy(config)) {
            throw new DebeziumException("'" + JdbcSinkConnectorConfig.SCHEMA_EVOLUTION + "=" + SchemaEvolutionMode.NONE_VALIDATED.getValue()
                    + "' supports startup table validation only with the default collection naming strategy.");
        }

        if (collectionNameFormat.contains(SOURCE_PLACEHOLDER_PREFIX)) {
            throw new DebeziumException("'" + JdbcSinkConnectorConfig.SCHEMA_EVOLUTION + "=" + SchemaEvolutionMode.NONE_VALIDATED.getValue()
                    + "' cannot validate target tables at startup when '" + JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT
                    + "' contains source field placeholders.");
        }

        if (!collectionNameFormat.contains(TOPIC_PLACEHOLDER)) {
            return Set.of(collectionNameFormat);
        }

        if (!Strings.isNullOrEmpty(props.get(SinkTask.TOPICS_REGEX_CONFIG))) {
            throw new DebeziumException("'" + JdbcSinkConnectorConfig.SCHEMA_EVOLUTION + "=" + SchemaEvolutionMode.NONE_VALIDATED.getValue()
                    + "' cannot validate target tables at startup when '" + SinkTask.TOPICS_REGEX_CONFIG + "' is used.");
        }

        final Set<String> topics = parseTopics(props.get(SinkTask.TOPICS_CONFIG));
        if (topics.isEmpty()) {
            throw new DebeziumException("'" + JdbcSinkConnectorConfig.SCHEMA_EVOLUTION + "=" + SchemaEvolutionMode.NONE_VALIDATED.getValue()
                    + "' requires statically configured '" + SinkTask.TOPICS_CONFIG + "' when '" + JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT
                    + "' contains '${topic}'.");
        }

        return topics
                .stream()
                .map(topic -> collectionNameFormat.replace(TOPIC_PLACEHOLDER, topic.replace(".", "_")))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @SuppressWarnings("unchecked")
    private static Set<String> parseTopics(String topics) {
        final List<String> parsedTopics = (List<String>) ConfigDef.parseType(SinkTask.TOPICS_CONFIG, topics, ConfigDef.Type.LIST);
        if (parsedTopics == null) {
            return Set.of();
        }
        return parsedTopics.stream()
                .filter(topic -> !topic.isEmpty())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @SuppressWarnings("deprecation")
    private static boolean usesDefaultCollectionNamingStrategy(JdbcSinkConnectorConfig config) {
        CollectionNamingStrategy strategy = config.getCollectionNamingStrategy();
        if (strategy instanceof TemporaryBackwardCompatibleCollectionNamingStrategyProxy proxy) {
            strategy = proxy.getOriginalStrategy();
        }
        return strategy instanceof DefaultCollectionNamingStrategy;
    }
}
