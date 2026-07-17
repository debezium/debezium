/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
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

final class CollectionsExistsValidator {

    private static final String TOPIC_PLACEHOLDER = "${topic}";
    private static final String SOURCE_PLACEHOLDER_PREFIX = "${source.";

    private final JdbcSinkConnectorConfig config;
    private final SessionFactory sessionFactory;
    private final DatabaseDialect dialect;
    private final Set<String> staticallyConfiguredTopics;
    private final boolean topicsRegexConfigured;

    CollectionsExistsValidator(JdbcSinkConnectorConfig config,
                               SessionFactory sessionFactory,
                               DatabaseDialect dialect,
                               String topics,
                               boolean topicsRegexConfigured) {
        this.config = config;
        this.sessionFactory = sessionFactory;
        this.dialect = dialect;
        this.staticallyConfiguredTopics = parseTopics(topics);
        this.topicsRegexConfigured = topicsRegexConfigured;
    }

    void validate() {
        final Set<String> collectionNames = resolveCollectionNames(staticallyConfiguredTopics);
        if (collectionNames.isEmpty() && topicsRegexConfigured) {
            return;
        }
        if (collectionNames.isEmpty()) {
            throw new DebeziumException("'" + JdbcSinkConnectorConfig.SCHEMA_EVOLUTION + "=" + SchemaEvolutionMode.VALIDATE_ONLY.getValue()
                    + "' requires '" + SinkTask.TOPICS_CONFIG + "' or '" + SinkTask.TOPICS_REGEX_CONFIG + "' when '"
                    + JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT + "' contains '${topic}'.");
        }

        validateCollectionNames(collectionNames);
    }

    void validateAssignedTopics(Collection<String> topics) {
        if (!topicsRegexConfigured || !config.getCollectionNameFormat().contains(TOPIC_PLACEHOLDER)) {
            return;
        }

        validateCollectionNames(resolveCollectionNames(topics));
    }

    private void validateCollectionNames(Collection<String> collectionNames) {
        final Set<CollectionId> collectionIds = resolveCollectionIds(collectionNames);
        if (collectionIds.isEmpty()) {
            return;
        }
        try (StatelessSession validationSession = sessionFactory.openStatelessSession()) {
            for (CollectionId collectionId : collectionIds) {
                final boolean exists = validationSession.doReturningWork(connection -> dialect.tableExists(connection, collectionId));
                if (!exists) {
                    throw new DebeziumException(String.format(
                            "Target table '%s' does not exist, but '%s' is set to '%s'. "
                                    + "Create the target table before the connector processes records or use '%s=%s'.",
                            collectionId.toFullIdentiferString(),
                            JdbcSinkConnectorConfig.SCHEMA_EVOLUTION,
                            SchemaEvolutionMode.VALIDATE_ONLY.getValue(),
                            JdbcSinkConnectorConfig.SCHEMA_EVOLUTION,
                            SchemaEvolutionMode.BASIC.getValue()));
                }
            }
        }
    }

    Set<CollectionId> resolveCollectionIds(Collection<String> collectionNames) {
        return collectionNames
                .stream()
                .map(collectionName -> {
                    final CollectionId collectionId = dialect.getCollectionId(collectionName);
                    if (collectionId == null) {
                        throw new DebeziumException("Unable to resolve target table name '" + collectionName + "' for table existence validation.");
                    }
                    return collectionId;
                })
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    Set<String> resolveCollectionNames(Collection<String> topics) {
        final String collectionNameFormat = config.getCollectionNameFormat();
        if (Strings.isNullOrEmpty(collectionNameFormat)) {
            throw new DebeziumException("'" + JdbcSinkConnectorConfig.SCHEMA_EVOLUTION + "=" + SchemaEvolutionMode.VALIDATE_ONLY.getValue()
                    + "' requires '" + JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT + "' to be configured.");
        }

        final DefaultCollectionNamingStrategy namingStrategy = getDefaultCollectionNamingStrategy();
        if (namingStrategy == null) {
            throw new DebeziumException("'" + JdbcSinkConnectorConfig.SCHEMA_EVOLUTION + "=" + SchemaEvolutionMode.VALIDATE_ONLY.getValue()
                    + "' supports table existence validation only with the default collection naming strategy.");
        }

        if (collectionNameFormat.contains(SOURCE_PLACEHOLDER_PREFIX)) {
            throw new DebeziumException("'" + JdbcSinkConnectorConfig.SCHEMA_EVOLUTION + "=" + SchemaEvolutionMode.VALIDATE_ONLY.getValue()
                    + "' cannot validate target tables when '" + JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT
                    + "' contains source field placeholders.");
        }

        if (!collectionNameFormat.contains(TOPIC_PLACEHOLDER)) {
            return Set.of(collectionNameFormat);
        }

        return topics
                .stream()
                .map(topic -> namingStrategy.resolveCollectionName(topic, collectionNameFormat))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @SuppressWarnings("unchecked")
    private Set<String> parseTopics(String topics) {
        final List<String> parsedTopics = (List<String>) ConfigDef.parseType(SinkTask.TOPICS_CONFIG, topics, ConfigDef.Type.LIST);
        if (parsedTopics == null) {
            return Set.of();
        }
        return parsedTopics.stream()
                .filter(topic -> !topic.isEmpty())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @SuppressWarnings("deprecation")
    private DefaultCollectionNamingStrategy getDefaultCollectionNamingStrategy() {
        CollectionNamingStrategy strategy = config.getCollectionNamingStrategy();
        // The connector configuration wraps even the default naming strategy in this proxy.
        // Unwrap it so that validation can recognize and reuse the underlying default strategy.
        if (strategy instanceof TemporaryBackwardCompatibleCollectionNamingStrategyProxy proxy) {
            strategy = proxy.getOriginalStrategy();
        }
        return strategy instanceof DefaultCollectionNamingStrategy defaultStrategy ? defaultStrategy : null;
    }
}
