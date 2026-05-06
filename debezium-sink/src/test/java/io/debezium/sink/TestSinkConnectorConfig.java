/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink;

import java.util.Set;

import io.debezium.sink.filter.FieldFilterFactory.FieldNameFilter;
import io.debezium.sink.naming.CollectionNamingStrategy;
import io.debezium.sink.naming.DefaultCollectionNamingStrategy;

public class TestSinkConnectorConfig implements SinkConnectorConfig {

    private int batchSize = 500;
    private PrimaryKeyMode primaryKeyMode = PrimaryKeyMode.NONE;
    private Set<String> primaryKeyFields = Set.of();
    private FieldNameFilter fieldFilter = null;
    private boolean truncateEnabled = false;
    private boolean deleteEnabled = false;
    private String collectionNameFormat = "${topic}";
    private String cloudEventsSchemaNamePattern = ".*CloudEvents\\.Envelope$";
    private KeyedMessageBatchMode keyedMessageBatchMode = KeyedMessageBatchMode.DEDUPLICATION;
    private CollectionNamingStrategy collectionNamingStrategy = new DefaultCollectionNamingStrategy();

    public TestSinkConnectorConfig withBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public TestSinkConnectorConfig withPrimaryKeyMode(PrimaryKeyMode primaryKeyMode) {
        this.primaryKeyMode = primaryKeyMode;
        return this;
    }

    public TestSinkConnectorConfig withPrimaryKeyFields(Set<String> primaryKeyFields) {
        this.primaryKeyFields = primaryKeyFields;
        return this;
    }

    public TestSinkConnectorConfig withFieldFilter(FieldNameFilter fieldFilter) {
        this.fieldFilter = fieldFilter;
        return this;
    }

    public TestSinkConnectorConfig withTruncateEnabled(boolean truncateEnabled) {
        this.truncateEnabled = truncateEnabled;
        return this;
    }

    public TestSinkConnectorConfig withDeleteEnabled(boolean deleteEnabled) {
        this.deleteEnabled = deleteEnabled;
        return this;
    }

    public TestSinkConnectorConfig withCollectionNameFormat(String collectionNameFormat) {
        this.collectionNameFormat = collectionNameFormat;
        return this;
    }

    public TestSinkConnectorConfig withCloudEventsSchemaNamePattern(String cloudEventsSchemaNamePattern) {
        this.cloudEventsSchemaNamePattern = cloudEventsSchemaNamePattern;
        return this;
    }

    public TestSinkConnectorConfig withKeyedMessageBatchMode(KeyedMessageBatchMode keyedMessageBatchMode) {
        this.keyedMessageBatchMode = keyedMessageBatchMode;
        return this;
    }

    public TestSinkConnectorConfig withCollectionNamingStrategy(CollectionNamingStrategy collectionNamingStrategy) {
        this.collectionNamingStrategy = collectionNamingStrategy;
        return this;
    }

    @Override
    public String getCollectionNameFormat() {
        return collectionNameFormat;
    }

    @Override
    public KeyedMessageBatchMode getKeyedMessageBatchMode() {
        return keyedMessageBatchMode;
    }

    @Override
    public PrimaryKeyMode getPrimaryKeyMode() {
        return primaryKeyMode;
    }

    @Override
    public Set<String> getPrimaryKeyFields() {
        return primaryKeyFields;
    }

    @Override
    public boolean isTruncateEnabled() {
        return truncateEnabled;
    }

    @Override
    public boolean isDeleteEnabled() {
        return deleteEnabled;
    }

    @Override
    public String useTimeZone() {
        return "UTC";
    }

    @Override
    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public CollectionNamingStrategy getCollectionNamingStrategy() {
        return collectionNamingStrategy;
    }

    @Override
    public FieldNameFilter fieldFilter() {
        return fieldFilter;
    }

    @Override
    public String cloudEventsSchemaNamePattern() {
        return cloudEventsSchemaNamePattern;
    }
}
