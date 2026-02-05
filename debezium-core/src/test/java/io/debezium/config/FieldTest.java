/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

public class FieldTest {

    // Test enum implementing EnumeratedValue for DBZ-1543
    public enum TestAdapter implements EnumeratedValue {
        LOG_MINER("LogMiner"),
        LOG_MINER_UNBUFFERED("LogMiner_Unbuffered"),
        XSTREAM("XStream"),
        OLR("OLR");

        private final String value;

        TestAdapter(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    @Test
    @FixFor("DBZ-8832")
    public void shouldHaveDeprecatedAliasesAndDefault() {
        Field field = Field.create("new.field")
                .withDescription("a description")
                .withDeprecatedAliases("deprecated.field")
                .withDefault("default");

        assertThat(field.deprecatedAliases()).isNotEmpty();
        assertThat(field.defaultValue()).isNotNull();
    }

    @Test
    @FixFor("DBZ-8832")
    public void shouldHaveDefaultAndDeprecatedAliases() {
        Field field = Field.create("new.field")
                .withDescription("a description")
                .withDefault("default")
                .withDeprecatedAliases("deprecated.field");

        assertThat(field.deprecatedAliases()).isNotEmpty();
        assertThat(field.defaultValue()).isNotNull();
    }

    @Test
    public void shouldReturnDependantFieldsBasedOnValue() {
        Field authType = Field.create("auth_type")
                .withDescription("Authorization type")
                .withDefault("NONE")
                .withAllowedValues(Set.of("NONE", "BASIC", "SSL"))
                .withDependents("BASIC", DependentFieldMatcher.exact("username", "password"))
                .withDependents("SSL", DependentFieldMatcher.exact("ssl.protocol"));

        Field username = Field.create("username");
        Field password = Field.create("password");
        Field sslProtocol = Field.create("ssl.protocol");

        ConfigDefinition configDef = ConfigDefinition.editor()
                .name("test-connector")
                .connector(authType, username, password, sslProtocol)
                .create();

        Field resolvedAuthType = configDef.connector().stream()
                .filter(f -> f.name().equals("auth_type"))
                .findFirst()
                .orElseThrow();

        assertThat(resolvedAuthType.dependents("BASIC")).containsExactlyInAnyOrder("password", "username");
        assertThat(resolvedAuthType.dependents("SSL")).containsExactly("ssl.protocol");
    }

    @Test
    public void shouldReturnSameDependantFieldsForMultipleParentValues() {
        // Create fields with matchers
        Field adapter = Field.create("connector_adapter")
                .withDescription("Connector adapter type")
                .withDefault("LogMiner")
                .withAllowedValues(Set.of("LogMiner", "LogMiner_Unbuffered", "XStream", "OLR"))
                .withDependents(List.of("LogMiner", "LogMiner_Unbuffered"),
                        DependentFieldMatcher.exact("log.mining.strategy", "log.mining.batch.size.min", "database.url"))
                .withDependents("XStream", DependentFieldMatcher.exact("database.out.server.name"))
                .withDependents("OLR", DependentFieldMatcher.exact("openlogreplicator.host", "openlogreplicator.port"));

        Field logMiningStrategy = Field.create("log.mining.strategy");
        Field logMiningBatchSize = Field.create("log.mining.batch.size.min");
        Field databaseUrl = Field.create("database.url");
        Field xstreamServer = Field.create("database.out.server.name");
        Field olrHost = Field.create("openlogreplicator.host");
        Field olrPort = Field.create("openlogreplicator.port");

        // Create ConfigDefinition to trigger pattern resolution
        ConfigDefinition configDef = ConfigDefinition.editor()
                .name("test-connector")
                .connector(adapter, logMiningStrategy, logMiningBatchSize, databaseUrl, xstreamServer, olrHost, olrPort)
                .create();

        // Get resolved field from ConfigDefinition
        Field resolvedAdapter = configDef.connector().stream()
                .filter(f -> f.name().equals("connector_adapter"))
                .findFirst()
                .orElseThrow();

        // Both LogMiner and LogMiner_Unbuffered should have the same dependents
        assertThat(resolvedAdapter.dependents("LogMiner"))
                .containsExactlyInAnyOrder("database.url", "log.mining.batch.size.min", "log.mining.strategy");
        assertThat(resolvedAdapter.dependents("LogMiner_Unbuffered"))
                .containsExactlyInAnyOrder("database.url", "log.mining.batch.size.min", "log.mining.strategy");

        // XStream should have its own dependents
        assertThat(resolvedAdapter.dependents("XStream"))
                .containsExactly("database.out.server.name");

        // OLR should have its own dependents
        assertThat(resolvedAdapter.dependents("OLR"))
                .containsExactlyInAnyOrder("openlogreplicator.host", "openlogreplicator.port");
    }

    @Test
    public void aChildFiledShouldHaveARecommenderBasedOnParentValue() {
        // Create fields with matchers
        Field authType = Field.create("auth_type")
                .withDescription("Authorization type")
                .withDefault("NONE")
                .withAllowedValues(Set.of("NONE", "BASIC", "SSL"))
                .withDependents("BASIC", DependentFieldMatcher.exact("username", "password"))
                .withDependents("SSL", DependentFieldMatcher.exact("ssl.protocol"));

        Field username = Field.create("username")
                .withDescription("Username for basic authentication");

        Field password = Field.create("password");
        Field sslProtocol = Field.create("ssl.protocol");

        // Create ConfigDefinition to trigger pattern resolution
        ConfigDefinition configDef = ConfigDefinition.editor()
                .name("test-connector")
                .connector(authType, username, password, sslProtocol)
                .create();

        // Create ConfigDef from resolved fields
        ConfigDef kafkaConfigDef = configDef.configDef();

        ConfigDef.ConfigKey child = kafkaConfigDef.configKeys().get("username");
        assertThat(child.recommender).isNotNull();
        assertThat(child.recommender.visible(child.name, Map.of("auth_type", "BASIC"))).isTrue();
    }

    @Test
    @FixFor("DBZ-1543")
    public void shouldMatchCaseBetweenAllowedValuesAndValueDependants() {

        // Test that allowedValues and valueDependants keys use the same case
        // This is critical for value-based dependencies to work correctly in the UI
        Field adapter = Field.create("connection.adapter")
                .withDescription("Connection Adapter")
                .withEnum(TestAdapter.class, TestAdapter.LOG_MINER)
                .withDependents(List.of("LogMiner", "LogMiner_Unbuffered"),
                        DependentFieldMatcher.exact("log.mining.buffer.type"));

        Field bufferType = Field.create("log.mining.buffer.type")
                .withDescription("Buffer type");

        ConfigDefinition configDef = ConfigDefinition.editor()
                .name("test-connector")
                .connector(adapter, bufferType)
                .create();

        Field resolvedAdapter = configDef.connector().stream()
                .filter(f -> f.name().equals("connection.adapter"))
                .findFirst()
                .orElseThrow();

        Set<String> allowedValues = (Set<String>) resolvedAdapter.allowedValues();
        Map<Object, List<String>> valueDependants = resolvedAdapter.valueDependants();

        assertThat(allowedValues).contains("logminer");
        assertThat(valueDependants).containsKey("logminer");
        assertThat(valueDependants.get("logminer")).containsExactly("log.mining.buffer.type");

        assertThat(allowedValues).contains("logminer_unbuffered");
        assertThat(valueDependants).containsKey("logminer_unbuffered");
        assertThat(valueDependants.get("logminer_unbuffered")).containsExactly("log.mining.buffer.type");
    }
}
