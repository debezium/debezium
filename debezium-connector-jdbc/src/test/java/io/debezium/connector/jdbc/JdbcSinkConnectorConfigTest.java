/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import org.hibernate.cfg.AvailableSettings;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Field;
import io.debezium.connector.jdbc.naming.CustomCollectionNamingStrategy;
import io.debezium.connector.jdbc.naming.CustomColumnNamingStrategy;
import io.debezium.connector.jdbc.naming.TemporaryBackwardCompatibleCollectionNamingStrategyProxy;
import io.debezium.doc.FixFor;
import io.debezium.sink.SinkConnectorConfig.PrimaryKeyMode;
import io.debezium.sink.naming.CollectionNamingStrategy;
import io.debezium.sink.naming.DefaultCollectionNamingStrategy;

/**
 * Unit tests for the {@link JdbcSinkConnectorConfig} class.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class JdbcSinkConnectorConfigTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSinkConnectorConfigTest.class);

    @Test
    public void testMissingRequiredConfigurationPropertiesFail() {
        final Field connectionUrl = JdbcSinkConnectorConfig.CONNECTION_URL_FIELD;
        final Field connectionUserName = JdbcSinkConnectorConfig.CONNECTION_USER_FIELD;
        final Field connectionPassword = JdbcSinkConnectorConfig.CONNECTION_PASSWORD_FIELD;

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Collections.emptyMap());
        assertThat(config.validateAndRecord(List.of(connectionUrl, connectionUserName, connectionPassword), LOGGER::error)).isFalse();
    }

    @Test
    public void testNonDefaultInsertModeProperty() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, "upsert");

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        assertThat(config.validateAndRecord(List.of(JdbcSinkConnectorConfig.INSERT_MODE_FIELD), LOGGER::error)).isFalse();
        assertThat(config.getInsertMode()).isEqualTo(JdbcSinkConnectorConfig.InsertMode.UPSERT);
    }

    @Test
    public void testNonDefaultDeleteEnabledPropertyWithPrimaryKeyModeNotRecordKey() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcSinkConnectorConfig.DELETE_ENABLED, "true");
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, "record_value");

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        assertThat(config.validateAndRecord(List.of(JdbcSinkConnectorConfig.DELETE_ENABLED_FIELD, JdbcSinkConnectorConfig.PRIMARY_KEY_MODE_FIELD), LOGGER::error))
                .isFalse();
    }

    @Test
    public void testNonDefaultDeleteEnabledPropertyWithPrimaryKeyModeRecordKey() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcSinkConnectorConfig.DELETE_ENABLED, "true");
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, "record_key");

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        assertThat(config.validateAndRecord(List.of(JdbcSinkConnectorConfig.DELETE_ENABLED_FIELD, JdbcSinkConnectorConfig.PRIMARY_KEY_MODE_FIELD), LOGGER::error))
                .isTrue();
        assertThat(config.isDeleteEnabled()).isTrue();
    }

    @Test
    public void testNonDefaultTableNameFormatProperty() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT, "e2e-${topic}");

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        assertThat(config.validateAndRecord(List.of(JdbcSinkConnectorConfig.COLLECTION_NAME_FORMAT_FIELD), LOGGER::error)).isTrue();
        assertThat(config.getCollectionNameFormat()).isEqualTo("e2e-${topic}");
    }

    @Test
    public void testNonDefaultPrimaryKeyModeProperty() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, "record_value");

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        assertThat(config.validateAndRecord(List.of(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE_FIELD), LOGGER::error)).isTrue();
        assertThat(config.getPrimaryKeyMode()).isEqualTo(PrimaryKeyMode.RECORD_VALUE);
    }

    @Test
    public void testNonDefaultPrimaryKeyFieldsProperty() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS, "id,name");

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        assertThat(config.validateAndRecord(List.of(JdbcSinkConnectorConfig.PRIMARY_KEY_FIELDS_FIELD), LOGGER::error)).isTrue();
        assertThat(config.getPrimaryKeyFields()).contains("id", "name");
    }

    @Test
    public void testNonDefaultSqlSelverIdentityTableNamesProperty() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcSinkConnectorConfig.SQLSERVER_IDENTITY_INSERT, "true");

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        assertThat(config.validateAndRecord(List.of(JdbcSinkConnectorConfig.SQLSERVER_IDENTITY_INSERT_FIELD), LOGGER::error)).isTrue();
        assertThat(config.isSqlServerIdentityInsert()).isTrue();
    }

    @Test
    @FixFor("DBZ-7431")
    public void testOverrideHibernateConfigurationProperties() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcSinkConnectorConfig.CONNECTION_PROVIDER, "io.debezium.AcmeConnectionProvider");
        properties.put(JdbcSinkConnectorConfig.CONNECTION_URL, "jdbc://url");
        properties.put(JdbcSinkConnectorConfig.CONNECTION_USER, "user");
        properties.put(JdbcSinkConnectorConfig.CONNECTION_PASSWORD, "pass");

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final Properties ormProperties = config.getHibernateConfiguration().getProperties();
        assertThat(ormProperties).isNotNull();
        assertThat(ormProperties.get(AvailableSettings.CONNECTION_PROVIDER)).isEqualTo("io.debezium.AcmeConnectionProvider");
        assertThat(ormProperties.get(AvailableSettings.JAKARTA_JDBC_URL)).isEqualTo("jdbc://url");
        assertThat(ormProperties.get(AvailableSettings.JAKARTA_JDBC_USER)).isEqualTo("user");
        assertThat(ormProperties.get(AvailableSettings.JAKARTA_JDBC_PASSWORD)).isEqualTo("pass");
    }

    @Test
    @FixFor("DBZ-8151")
    public void testPrimaryKeyRecordValueDoesNotRequirePrimaryKeyFields() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcSinkConnectorConfig.CONNECTION_PROVIDER, "io.debezium.AcmeConnectionProvider");
        properties.put(JdbcSinkConnectorConfig.CONNECTION_URL, "jdbc://url");
        properties.put(JdbcSinkConnectorConfig.CONNECTION_USER, "user");
        properties.put(JdbcSinkConnectorConfig.CONNECTION_PASSWORD, "pass");
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, "upsert");
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, "record_value");

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        assertThat(config.validateAndRecord(List.of(JdbcSinkConnectorConfig.INSERT_MODE_FIELD), LOGGER::error)).isTrue();
    }

    @Test
    public void testDeprecatedDatabaseTimeZone() {
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of(JdbcSinkConnectorConfig.DEPRECATED_DATABASE_TIME_ZONE, "CEST"));
        AtomicReference<String> errorMessage = new AtomicReference<>();
        assertThat(config.validateAndRecord(List.of(JdbcSinkConnectorConfig.USE_TIME_ZONE_FIELD), errorMessage::set)).isTrue();
        assertEquals(
                "The 'use.time.zone' value is invalid: Warning: Using deprecated config option \"database.time_zone\".",
                errorMessage.get());
        assertEquals("CEST", config.useTimeZone());
    }

    @Test
    public void testDeprecatedTableNamingStrategy() {
        var properties = Map.of(
                JdbcSinkConnectorConfig.DEPRECATED_TABLE_NAMING_STRATEGY, "table.naming.strategy",
                JdbcSinkConnectorConfig.COLLECTION_NAMING_STRATEGY, JdbcSinkConnectorConfig.DEPRECATED_TABLE_NAMING_STRATEGY);

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);

        Field.Set fields = Field.setOf(JdbcSinkConnectorConfig.COLLECTION_NAMING_STRATEGY_FIELD);

        AtomicReference<String> errorMessage = new AtomicReference<>();
        boolean isValid = config.validateAndRecord(fields, errorMessage::set);
        assertTrue(isValid, "Validation should pass.");
        assertEquals(
                "The 'collection.naming.strategy' value 'table.naming.strategy' is invalid: Warning: Using deprecated config option \"table.naming.strategy\".",
                errorMessage.get());
    }

    @Test
    public void testProxyWithDeprecatedTableNamingStrategy() {
        Map<String, String> props = Map.of(
                JdbcSinkConnectorConfig.DEPRECATED_TABLE_NAMING_STRATEGY, "deprecated_table_strategy",
                JdbcSinkConnectorConfig.COLLECTION_NAMING_STRATEGY, DefaultCollectionNamingStrategy.class.getName());

        JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(props);

        CollectionNamingStrategy strategy = config.getCollectionNamingStrategy();
        assertTrue(strategy instanceof TemporaryBackwardCompatibleCollectionNamingStrategyProxy,
                "Expected a proxy for backward compatibility");
    }

    @Test
    public void testCustomColumnNamingStrategyIntegration() {
        CustomColumnNamingStrategy strategy = new CustomColumnNamingStrategy();

        Map<String, String> props = new HashMap<>();
        props.put("column.naming.style", "snake_case");
        props.put("column.naming.prefix", "pre_");
        props.put("column.naming.suffix", "_suf");
        strategy.configure(props);

        LOGGER.debug("Testing column naming with properties: {}", props);

        String resolvedName = strategy.resolveColumnName("testColumn");
        assertThat(resolvedName).isEqualTo("pre_test_column_suf");
    }

    @Test
    public void testCustomCollectionNamingStrategyIntegration() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcSinkConnectorConfig.COLLECTION_NAMING_STRATEGY, CustomCollectionNamingStrategy.class.getName());
        properties.put("collection.naming.style", "snake_case");
        properties.put("collection.naming.prefix", "tbl_");
        properties.put("collection.naming.suffix", "_table");

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        CollectionNamingStrategy strategy = config.getCollectionNamingStrategy();

        assertThat(strategy).isInstanceOf(TemporaryBackwardCompatibleCollectionNamingStrategyProxy.class);

        CollectionNamingStrategy actualStrategy = ((TemporaryBackwardCompatibleCollectionNamingStrategyProxy) strategy).getOriginalStrategy();
        assertThat(actualStrategy).isInstanceOf(CustomCollectionNamingStrategy.class);

        String resolvedName = strategy.resolveCollectionName(null, "TestCollection");
        assertThat(resolvedName).isEqualTo("tbl_test_collection_table");
    }

    @Test
    @FixFor("DBZ-7810")
    public void testEmptyPassword() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcSinkConnectorConfig.CONNECTION_PROVIDER, "io.debezium.AcmeConnectionProvider");
        properties.put(JdbcSinkConnectorConfig.CONNECTION_URL, "jdbc://url");
        properties.put(JdbcSinkConnectorConfig.CONNECTION_USER, "user");
        properties.put(JdbcSinkConnectorConfig.CONNECTION_PASSWORD, ""); // Empty password

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final Properties ormProperties = config.getHibernateConfiguration().getProperties();
        assertThat(ormProperties).isNotNull();
        assertThat(ormProperties.get(AvailableSettings.CONNECTION_PROVIDER)).isEqualTo("io.debezium.AcmeConnectionProvider");
        assertThat(ormProperties.get(AvailableSettings.JAKARTA_JDBC_URL)).isEqualTo("jdbc://url");
        assertThat(ormProperties.get(AvailableSettings.JAKARTA_JDBC_USER)).isEqualTo("user");
        assertThat(ormProperties.get(AvailableSettings.JAKARTA_JDBC_PASSWORD)).isNull(); // Password should be null
    }

    // @Test
    // public void testNonDefaultSchemaEvolutionProperty() {
    // final Map<String, String> properties = new HashMap<>();
    // properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, "advanced");
    //
    // final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
    // assertThat(config.validateAndRecord(List.of(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION_FIELD), LOGGER::error)).isTrue();
    // assertThat(config.getSchemaEvolutionMode()).isEqualTo(SchemaEvolutionMode.ADVANCED);
    // }

    // @Test
    // public void testDataTypeMappingProperty() {
    // final Map<String, String> properties = new HashMap<>();
    // properties.put(JdbcSinkConnectorConfig.DATA_TYPE_MAPPING, "table.column:GEOMETRY,table.column2:INT");
    //
    // final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
    // assertThat(config.validateAndRecord(List.of(JdbcSinkConnectorConfig.DATA_TYPE_MAPPING_FIELD), LOGGER::error)).isTrue();
    // assertThat(config.getDataTypeMapping()).contains("table.column:GEOMETRY", "table.column2:INT");
    // }
}
