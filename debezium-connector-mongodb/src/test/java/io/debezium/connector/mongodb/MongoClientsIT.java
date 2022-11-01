/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;

import io.debezium.config.Configuration;

/**
 * @author Randall Hauch
 *
 */
public class MongoClientsIT {

    private static List<ServerAddress> addresses;

    @BeforeClass
    public static void beforeAll() {
        Configuration config = TestHelper.getConfiguration();
        String host = config.getString(MongoDbConnectorConfig.HOSTS);
        addresses = MongoUtil.parseAddresses(host);
    }

    private MongoClients clients;

    @Before
    public void beforeEach() {
        clients = MongoClients.create().build();
    }

    @After
    public void afterEach() {
        if (clients != null) {
            try {
                clients.clear();
            }
            finally {
                clients = null;
            }
        }
    }

    @Test
    public void shouldReturnSameInstanceForSameAddress() {
        addresses.forEach(address -> {
            MongoClient client1 = clients.clientFor(address);
            MongoClient client2 = clients.clientFor(address);
            assertThat(client1).isSameAs(client2);

            MongoClient client3 = clients.clientFor(address.toString());
            MongoClient client4 = clients.clientFor(address);
            assertThat(client3).isSameAs(client4);
            assertThat(client3).isSameAs(client1);

            MongoClient client5 = clients.clientFor(address.toString());
            MongoClient client6 = clients.clientFor(address.toString());
            assertThat(client5).isSameAs(client6);
            assertThat(client5).isSameAs(client1);
        });
    }

    @Test
    public void shouldReturnSameInstanceForSameAddresses() {
        MongoClient client1 = clients.clientForMembers(addresses);
        MongoClient client2 = clients.clientForMembers(addresses);
        assertThat(client1).isSameAs(client2);

        MongoClient client3 = clients.clientForMembers(addresses);
        MongoClient client4 = clients.clientForMembers(addresses);
        assertThat(client3).isSameAs(client4);
        assertThat(client3).isSameAs(client1);

        String addressesStr = MongoUtil.toString(addresses);
        MongoClient client5 = clients.clientForMembers(addressesStr);
        MongoClient client6 = clients.clientForMembers(addressesStr);
        assertThat(client5).isSameAs(client6);
        assertThat(client5).isSameAs(client1);
    }
}
