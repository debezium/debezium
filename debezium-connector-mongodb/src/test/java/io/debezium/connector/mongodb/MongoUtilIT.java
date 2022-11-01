/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import com.mongodb.ServerAddress;

/**
 * @author Chris Collingwood
 */
public class MongoUtilIT extends AbstractMongoIT {

    @Test
    public void testGetPrimaryAddress() {
        useConfiguration(config.edit()
                .with(MongoDbConnectorConfig.AUTO_DISCOVER_MEMBERS, true)
                .build());

        primary.execute("shouldConnect", mongo -> {
            ServerAddress primaryAddress = MongoUtil.getPrimaryAddress(mongo);
            assertThat(primaryAddress.getHost()).isEqualTo("localhost");
            assertThat(primaryAddress.getPort()).isEqualTo(27017);
        });
    }

}
