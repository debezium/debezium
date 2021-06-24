/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Test;

import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;

import io.debezium.config.Configuration;

/**
 * @author Chris Collingwood
 */
public class MongoUtilIT extends AbstractMongoIT {

    @Test
    public void testGetPrimaryAddress() {

        ReplicaSet rs = ReplicaSet.parse("rs0/localhost:27017");
        Configuration testConfig = Configuration.fromSystemProperties("connector.").edit()
                .withDefault(MongoDbConnectorConfig.HOSTS, rs.toString())
                .with(MongoDbConnectorConfig.AUTO_DISCOVER_MEMBERS, true)
                .withDefault(MongoDbConnectorConfig.LOGICAL_NAME, "mongo1").build();

        ConnectionContext context = new ConnectionContext(testConfig);
        MongoClient replicaSetClient = context.clientForReplicaSet(rs);

        ServerAddress primaryAddress = MongoUtil.getPrimaryAddress(replicaSetClient);
        assertThat(primaryAddress.getHost()).isEqualTo("localhost");
        assertThat(primaryAddress.getPort()).isEqualTo(27017);

    }

}
