/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.fest.assertions.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Assert;
import org.junit.Test;

import io.debezium.config.Configuration;

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

    @Test
    public void assertCreateTaskConfigs() {
        MongoDbConnector connector = new MongoDbConnector();

        connector.start(
                Configuration.create()
                        .with(MongoDbConnectorConfig.LOGICAL_NAME, "mongo")
                        .with(MongoDbConnectorConfig.HOSTS, "rs1/")
                        .build()
                        .asMap());

        // Case 1: Regular setup with single replset
        // Since number of tasks > number of replsets, only two tasks should be assigned to the two replset
        String replset1 = "rs1/1.2.3.4:27017";
        String replset2 = "rs2/2.3.4.5:27017";
        int numTasks = 3;
        ReplicaSets actualReplicaSets = ReplicaSets.parse(String.format("%s;%s", replset1, replset2));
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        connector.createTaskConfigs(numTasks, actualReplicaSets, taskConfigs);

        assertThat(taskConfigs.size()).isEqualTo(2);
        String[] output1 = new String[]{ taskConfigs.get(0).get(MongoDbConnectorConfig.HOSTS.name()), taskConfigs.get(1).get(MongoDbConnectorConfig.HOSTS.name()) };
        assertThat(output1).containsOnly(replset1, replset2);
        assertThat(taskConfigs.get(0).get(MongoDbConnectorConfig.TASK_ID.name())).isEqualTo("0");
        assertThat(taskConfigs.get(1).get(MongoDbConnectorConfig.TASK_ID.name())).isEqualTo("1");

        // Case 2: Regular setup with multiple replset
        // Since number of tasks < number of replsets, a task will be assigned to two replsets
        String replset3 = "rs3/1.2.3.4:27017";
        String replset4 = "rs4/1.2.3.4:27017";
        actualReplicaSets = ReplicaSets.parse(String.format("%s;%s;%s;%s", replset1, replset2, replset3, replset4));
        taskConfigs = new ArrayList<>();
        connector.createTaskConfigs(numTasks, actualReplicaSets, taskConfigs);

        assertThat(taskConfigs.size()).isEqualTo(3);
        List<ReplicaSet> output2 = new ArrayList<>();
        for (int i = 0; i < taskConfigs.size(); i++) {
            ReplicaSets replicaSets = ReplicaSets.parse(taskConfigs.get(i).get(MongoDbConnectorConfig.HOSTS.name()));
            assertThat(replicaSets.replicaSetCount()).isIn(1, 2); // the set has either 1 or 2 replsets
            output2.addAll(replicaSets.all());
            assertThat(taskConfigs.get(i).get(MongoDbConnectorConfig.TASK_ID.name())).isEqualTo(String.valueOf(i));
        }
        // Check that all the replsets collected from the tasks in output2 are exactly actualReplicaSets
        assertThat(output2.size()).isEqualTo(actualReplicaSets.replicaSetCount());
        assertThat(output2).containsOnly(actualReplicaSets.all().toArray(new ReplicaSet[0]));

        // Case 3: Multitask setup
        // The single replset should be assigned to all tasks
        connector.start(
                Configuration.create()
                        .with(MongoDbConnectorConfig.LOGICAL_NAME, "mongo")
                        .with(MongoDbConnectorConfig.HOSTS, "rs1/")
                        .with(MongoDbConnectorConfig.MONGODB_MULTI_TASK_ENABLED, true)
                        .build()
                        .asMap());

        actualReplicaSets = ReplicaSets.parse(replset1);
        taskConfigs = new ArrayList<>();
        connector.createTaskConfigs(numTasks, actualReplicaSets, taskConfigs);

        assertThat(taskConfigs.size()).isEqualTo(numTasks);
        for (int i = 0; i < taskConfigs.size(); i++) {
            assertThat(taskConfigs.get(i).get(MongoDbConnectorConfig.HOSTS.name())).isEqualTo(replset1);
            assertThat(taskConfigs.get(i).get(MongoDbConnectorConfig.TASK_ID.name())).isEqualTo(String.valueOf(i));
        }
    }
}
