/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

import org.junit.Test;

import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ServerDescription;

/**
 * @author Chris Collingwood
 */
public class MongoUtilIT extends AbstractMongoIT {

    @Test
    public void testGetPrimaryAddress() {
        useConfiguration(config.edit()
                .with(MongoDbConnectorConfig.AUTO_DISCOVER_MEMBERS, true)
                .build());

        Optional<ServerAddress> expectedPrimaryAddress;

        try (var client = connect()) {
            client.listDatabaseNames().first();

            var servers = client.getClusterDescription().getServerDescriptions();

            expectedPrimaryAddress = servers.stream()
                    .filter(ServerDescription::isPrimary)
                    .findFirst()
                    .map(ServerDescription::getAddress);
        }

        assertThat(expectedPrimaryAddress).isPresent();

        primary.execute("shouldConnect", mongo -> {
            ServerAddress primaryAddress = MongoUtil.getPreferredAddress(mongo, ReadPreference.primary());
            assertThat(primaryAddress.getHost()).isEqualTo(expectedPrimaryAddress.map(ServerAddress::getHost).get());
            assertThat(primaryAddress.getPort()).isEqualTo(expectedPrimaryAddress.map(ServerAddress::getPort).get());
        });
    }

}
