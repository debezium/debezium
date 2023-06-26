/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.fest.assertions.Assertions.assertThat;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Randall Hauch
 *
 */
public class MongoDbConnectorTest {

    @Test
    public void shouldReturnConfigurationDefinition() {
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
            assertThat(key.defaultValue).isEqualTo(expected.defaultValue());
            assertThat(key.dependents).isEqualTo(expected.dependents());
            assertThat(key.width).isNotNull();
            assertThat(key.group).isNotNull();
            assertThat(key.orderInGroup).isGreaterThan(0);
            assertThat(key.validator).isNull();
            assertThat(key.recommender).isNull();
        });
    }

    @Test
    public void assertValidateReplicaSetNames() {
        MongoDbConnector connector = new MongoDbConnector();
        ReplicaSets actualReplicaSets = ReplicaSets.parse("rs1/1.2.3.4:27017");
        String expectedHostString = "rs1/1.2.3.4:27017";
        // This should pass.
        connector.validateReplicaSets(expectedHostString, actualReplicaSets);

        // Assert NullPointerException.
        Assert.assertThrows(ConnectException.class, () -> connector.validateReplicaSets(null, actualReplicaSets));
        Assert.assertThrows(ConnectException.class, () -> connector.validateReplicaSets(expectedHostString, null));
        // Assert replica set names not matching.
        Assert.assertThrows(ConnectException.class, () -> connector.validateReplicaSets("rs2/1.2.3.4:27017", actualReplicaSets));
        Assert.assertThrows(ConnectException.class, () -> connector.validateReplicaSets("1.2.3.4:27017", actualReplicaSets));
        Assert.assertThrows(ConnectException.class, () -> connector.validateReplicaSets("rs1", actualReplicaSets)); // this resolves to rs1:27017 with empty relica set name
        Assert.assertThrows(ConnectException.class, () -> connector.validateReplicaSets("", actualReplicaSets));
        Assert.assertThrows(ConnectException.class, () -> connector.validateReplicaSets(expectedHostString, ReplicaSets.parse("rs2/1.2.3.4:27017")));
        Assert.assertThrows(ConnectException.class, () -> connector.validateReplicaSets(expectedHostString, ReplicaSets.parse("1.2.3.4:27017")));
        Assert.assertThrows(ConnectException.class, () -> connector.validateReplicaSets(expectedHostString, ReplicaSets.parse("rs1")));
        Assert.assertThrows(ConnectException.class, () -> connector.validateReplicaSets(expectedHostString, ReplicaSets.parse("")));
    }
}
