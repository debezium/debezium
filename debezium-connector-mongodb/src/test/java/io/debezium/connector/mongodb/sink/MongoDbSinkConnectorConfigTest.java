/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.sink.SinkConnectorConfig.PrimaryKeyMode;
import io.debezium.sink.naming.DefaultCollectionNamingStrategy;
import io.debezium.sink.naming.DefaultColumnNamingStrategy;

class MongoDbSinkConnectorConfigTest {

    private MongoDbSinkConnectorConfig createConfig(String... keyValues) {
        var builder = Configuration.create()
                .with(MongoDbSinkConnectorConfig.CONNECTION_STRING, "mongodb://localhost:27017")
                .with(MongoDbSinkConnectorConfig.SINK_DATABASE, "testdb");
        for (int i = 0; i < keyValues.length; i += 2) {
            builder = builder.with(keyValues[i], keyValues[i + 1]);
        }
        return new MongoDbSinkConnectorConfig(builder.build());
    }

    @Test
    void shouldUseDefaultCollectionNamingStrategy() {
        var config = createConfig();
        assertThat(config.getCollectionNamingStrategy()).isInstanceOf(DefaultCollectionNamingStrategy.class);
    }

    @Test
    void shouldUseDefaultColumnNamingStrategy() {
        var config = createConfig();
        assertThat(config.getColumnNamingStrategy()).isInstanceOf(DefaultColumnNamingStrategy.class);
    }

    @Test
    void shouldParseFieldIncludeList() {
        var config = createConfig("field.include.list", "id,name");
        var filter = config.fieldFilter();

        assertThat(filter.matches("any.topic", "id")).isTrue();
        assertThat(filter.matches("any.topic", "name")).isTrue();
        assertThat(filter.matches("any.topic", "email")).isFalse();
    }

    @Test
    void shouldParseFieldExcludeList() {
        var config = createConfig("field.exclude.list", "password,secret");
        var filter = config.fieldFilter();

        assertThat(filter.matches("any.topic", "name")).isTrue();
        assertThat(filter.matches("any.topic", "password")).isFalse();
        assertThat(filter.matches("any.topic", "secret")).isFalse();
    }

    @Test
    void shouldParseTopicQualifiedFieldIncludeList() {
        var config = createConfig("field.include.list", "my.topic:id,my.topic:name");
        var filter = config.fieldFilter();

        assertThat(filter.matches("my.topic", "id")).isTrue();
        assertThat(filter.matches("my.topic", "name")).isTrue();
        assertThat(filter.matches("other.topic", "id")).isFalse();
    }

    @Test
    void shouldReturnDefaultFilterWhenNoFilterConfigured() {
        var config = createConfig();
        var filter = config.fieldFilter();

        assertThat(filter.matches("any.topic", "any_field")).isTrue();
    }

    @Test
    void shouldParseCollectionNameFormat() {
        var config = createConfig("collection.name.format", "prefix_${topic}");
        assertThat(config.getCollectionNameFormat()).isEqualTo("prefix_${topic}");
    }

    @Test
    void shouldParseBatchSize() {
        var config = createConfig("batch.size", "111");
        assertThat(config.getBatchSize()).isEqualTo(111);
    }

    @Test
    void shouldDefaultBatchSizeTo500() {
        var config = createConfig();
        assertThat(config.getBatchSize()).isEqualTo(500);
    }

    @Test
    void shouldParseTruncateEnabled() {
        var config = createConfig("truncate.enabled", "true");
        assertThat(config.isTruncateEnabled()).isTrue();
    }

    @Test
    void shouldDefaultTruncateDisabled() {
        var config = createConfig();
        assertThat(config.isTruncateEnabled()).isFalse();
    }

    @Test
    void shouldParseDeleteEnabled() {
        var config = createConfig("delete.enabled", "true");
        assertThat(config.isDeleteEnabled()).isTrue();
    }

    @Test
    void shouldDefaultDeleteDisabled() {
        var config = createConfig();
        assertThat(config.isDeleteEnabled()).isFalse();
    }

    @Test
    void shouldParseCloudEventsSchemaNamePattern() {
        var config = createConfig("cloud.events.schema.name.pattern", ".*MyCloudEvents$");
        assertThat(config.cloudEventsSchemaNamePattern()).isEqualTo(".*MyCloudEvents$");
    }

    @Test
    void shouldDefaultCloudEventsPattern() {
        var config = createConfig();
        assertThat(config.cloudEventsSchemaNamePattern()).isEqualTo(".*CloudEvents\\.Envelope$");
    }

    @Test
    void shouldReturnSinkDatabaseName() {
        var config = createConfig();
        assertThat(config.getSinkDatabaseName()).isEqualTo("testdb");
    }

    // --- CONFIG_DEFINITION registration: these framework fields were previously read from config
    // but absent from CONFIG_DEFINITION / ALL_FIELDS, so they had no validation, no documentation,
    // and no config introspection via the Kafka Connect REST API. ---

    @Test
    void allFieldsShouldIncludeTruncateEnabled() {
        assertThat(MongoDbSinkConnectorConfig.ALL_FIELDS.fieldWithName("truncate.enabled")).isNotNull();
    }

    @Test
    void allFieldsShouldIncludeDeleteEnabled() {
        assertThat(MongoDbSinkConnectorConfig.ALL_FIELDS.fieldWithName("delete.enabled")).isNotNull();
    }

    @Test
    void allFieldsShouldIncludeFieldIncludeList() {
        assertThat(MongoDbSinkConnectorConfig.ALL_FIELDS.fieldWithName("field.include.list")).isNotNull();
    }

    @Test
    void allFieldsShouldIncludeFieldExcludeList() {
        assertThat(MongoDbSinkConnectorConfig.ALL_FIELDS.fieldWithName("field.exclude.list")).isNotNull();
    }

    @Test
    void allFieldsShouldIncludeCloudEventsSchemaNamePattern() {
        assertThat(MongoDbSinkConnectorConfig.ALL_FIELDS.fieldWithName("cloud.events.schema.name.pattern")).isNotNull();
    }

    // --- primary.key.mode: inherited as-is from the shared SinkConnectorConfig interface, same
    // default (NONE) as JDBC. Only consulted by the SCES-exclusive FilteringSinkDocumentResolver;
    // the legacy RawSinkDocumentResolver never reads it. ---

    @Test
    void shouldDefaultPrimaryKeyModeToNone() {
        var config = createConfig();
        assertThat(config.getPrimaryKeyMode()).isEqualTo(PrimaryKeyMode.NONE);
    }

    @Test
    void shouldParsePrimaryKeyModeRecordKey() {
        var config = createConfig(MongoDbSinkConnectorConfig.PRIMARY_KEY_MODE_FIELD.name(), "record_key");
        assertThat(config.getPrimaryKeyMode()).isEqualTo(PrimaryKeyMode.RECORD_KEY);
    }

    @Test
    void shouldParsePrimaryKeyModeKafka() {
        var config = createConfig(MongoDbSinkConnectorConfig.PRIMARY_KEY_MODE_FIELD.name(), "kafka");
        assertThat(config.getPrimaryKeyMode()).isEqualTo(PrimaryKeyMode.KAFKA);
    }

    @Test
    void shouldParsePrimaryKeyModeRecordValue() {
        var config = createConfig(MongoDbSinkConnectorConfig.PRIMARY_KEY_MODE_FIELD.name(), "record_value");
        assertThat(config.getPrimaryKeyMode()).isEqualTo(PrimaryKeyMode.RECORD_VALUE);
    }

    @Test
    void shouldParsePrimaryKeyModeRecordHeader() {
        var config = createConfig(MongoDbSinkConnectorConfig.PRIMARY_KEY_MODE_FIELD.name(), "record_header");
        assertThat(config.getPrimaryKeyMode()).isEqualTo(PrimaryKeyMode.RECORD_HEADER);
    }

    @Test
    void shouldParsePrimaryKeyFields() {
        var config = createConfig("primary.key.fields", "id,tenant");
        assertThat(config.getPrimaryKeyFields()).containsExactlyInAnyOrder("id", "tenant");
    }

    @Test
    void validateShouldRejectBothFieldIncludeAndExcludeList() {
        var config = createConfig(
                MongoDbSinkConnectorConfig.FIELD_INCLUDE_LIST_FIELD.name(), "id,name",
                MongoDbSinkConnectorConfig.FIELD_EXCLUDE_LIST_FIELD.name(), "description");

        assertThatThrownBy(config::validate)
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining(MongoDbSinkConnectorConfig.FIELD_INCLUDE_LIST_FIELD.name())
                .hasMessageContaining(MongoDbSinkConnectorConfig.FIELD_EXCLUDE_LIST_FIELD.name());
    }

    @Test
    void validateShouldSucceedWithAllPreviouslyUnregisteredFieldsSet() {
        var config = createConfig(
                MongoDbSinkConnectorConfig.TRUNCATE_ENABLED_FIELD.name(), "true",
                MongoDbSinkConnectorConfig.DELETE_ENABLED_FIELD.name(), "true",
                MongoDbSinkConnectorConfig.FIELD_INCLUDE_LIST_FIELD.name(), "id,name",
                MongoDbSinkConnectorConfig.CLOUDEVENTS_SCHEMA_NAME_PATTERN_FIELD.name(), ".*CloudEvents\\.Envelope$");

        // validate() throws ConnectException if config.validateAndRecord(ALL_FIELDS, ...) fails;
        // it must not throw now that these fields are part of ALL_FIELDS and pass validation.
        config.validate();
    }
}
