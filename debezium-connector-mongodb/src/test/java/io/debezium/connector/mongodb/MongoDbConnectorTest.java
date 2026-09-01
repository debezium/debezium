/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.List;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Connector;
import org.junit.jupiter.api.Test;

import com.mongodb.connection.ClusterType;

import io.debezium.connector.mongodb.connection.MongoDbConnectionContext;

/**
 * @author Randall Hauch
 *
 */
public class MongoDbConnectorTest {

    @Test
    void shouldReturnConfigurationDefinition() {
        assertConfigDefIsValid(new MongoDbConnector(), MongoDbConnectorConfig.ALL_FIELDS);
    }

    protected static void assertConfigDefIsValid(Connector connector, io.debezium.config.Field.Set fields) {
        ConfigDef configDef = connector.config();
        assertThat(configDef).isNotNull();
        fields.forEach(expected -> {
            assertThat(configDef.names()).contains(expected.name());
            ConfigKey key = configDef.configKeys().get(expected.name());
            assertThat(key).isNotNull();
            assertThat(key.name).isEqualTo(expected.name());
            assertThat(key.displayName).isEqualTo(expected.displayName());
            assertThat(key.importance).isEqualTo(expected.importance());
            assertThat(key.documentation).isEqualTo(expected.description());
            assertThat(key.type).isEqualTo(expected.type());
            if (expected.type() == Type.CLASS) {
                assertThat(((Class<?>) key.defaultValue).getName()).isEqualTo((String) expected.defaultValue());
            }
            else if (expected.type() == ConfigDef.Type.LIST && key.defaultValue != null) {
                assertThat(key.defaultValue).isEqualTo(Arrays.asList(expected.defaultValue()));
            }
            else {
                assertThat(key.defaultValue).isEqualTo(expected.defaultValue());
            }
            assertThat(key.dependents).isEqualTo(expected.dependents());
            assertThat(key.width).isNotNull();
            assertThat(key.group).isNotNull();
            assertThat(key.orderInGroup).isGreaterThan(0);
            assertThat(key.validator).isNull();
            assertThat(key.recommender).isNull();
        });
    }

    @Test
    void validateClusterTopologyShouldRejectStandaloneServer() {
        assertThat(validateTopology(ClusterType.STANDALONE, true))
                .hasSize(1)
                .element(0).asString()
                .contains("standalone")
                .contains("replica set or sharded cluster");
    }

    @Test
    void validateClusterTopologyShouldAcceptSupportedTopologies() {
        assertThat(validateTopology(ClusterType.REPLICA_SET, true)).isEmpty();
        assertThat(validateTopology(ClusterType.SHARDED, true)).isEmpty();
        assertThat(validateTopology(ClusterType.LOAD_BALANCED, true)).isEmpty();
        assertThat(validateTopology(ClusterType.UNKNOWN, true)).isEmpty();
    }

    @Test
    void validateClusterTopologyShouldRequireReplicaSetName() {
        assertThat(validateTopology(ClusterType.REPLICA_SET, false))
                .hasSize(1)
                .element(0).asString()
                .contains("Replica set not specified");
    }

    private static List<String> validateTopology(ClusterType clusterType, boolean hasReplicaSetNameIfRequired) {
        var connectionContext = mock(MongoDbConnectionContext.class);
        given(connectionContext.getClusterType()).willReturn(clusterType);
        given(connectionContext.hasReplicaSetNameIfRequired()).willReturn(hasReplicaSetNameIfRequired);

        var validation = new ConfigValue(MongoDbConnectorConfig.CONNECTION_STRING.name());
        MongoDbConnector.validateClusterTopology(connectionContext, validation);
        return validation.errorMessages();
    }

}
