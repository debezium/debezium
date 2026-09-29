/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.Test;

import com.mongodb.MongoSocketOpenException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;

import io.debezium.util.Collect;

/**
 * Tests to verify mongodb utilities
 */
public class MongoUtilTest {

    @Test
    void shouldGetClusterDescription() {
        ClusterDescription expectedClusterDescription = new ClusterDescription(
                ClusterConnectionMode.MULTIPLE,
                ClusterType.REPLICA_SET,
                List.of());

        var client = mock(MongoClient.class);
        when(client.getClusterDescription()).thenReturn(expectedClusterDescription);

        var actualDescription = MongoUtils.clusterDescription(client);
        assertThat(actualDescription).isEqualTo(expectedClusterDescription);
    }

    @Test
    void shouldGetClusterDescriptionAfterForcedConnection() {
        ClusterDescription unknwonClusterDescription = new ClusterDescription(
                ClusterConnectionMode.MULTIPLE,
                ClusterType.UNKNOWN,
                List.of());

        ClusterDescription expectedClusterDescription = new ClusterDescription(
                ClusterConnectionMode.MULTIPLE,
                ClusterType.REPLICA_SET,
                List.of());

        var client = mock(MongoClient.class);
        var database = mock(MongoDatabase.class);
        var metadataDatabase = mock(MongoDatabase.class);
        var readPreference = ReadPreference.secondary(new TagSet(new Tag("region", "east")));
        var helloResult = new BsonDocument("ok", new BsonInt64(1));

        when(client.getClusterDescription()).thenReturn(unknwonClusterDescription, expectedClusterDescription);
        when(client.getReadPreference()).thenReturn(readPreference);
        when(client.getDatabase("admin")).thenReturn(database);
        when(database.withReadConcern(ReadConcern.DEFAULT)).thenReturn(metadataDatabase);
        when(metadataDatabase.runCommand(
                new BsonDocument("hello", new BsonInt32(1)),
                readPreference,
                BsonDocument.class))
                .thenReturn(helloResult);

        var actualDescription = MongoUtils.clusterDescription(client);
        assertThat(actualDescription).isEqualTo(expectedClusterDescription);
        verify(database).withReadConcern(ReadConcern.DEFAULT);
        verify(metadataDatabase).runCommand(
                new BsonDocument("hello", new BsonInt32(1)),
                readPreference,
                BsonDocument.class);
        verify(client, never()).listDatabaseNames();
    }

    @Test
    void shouldRunHelloUsingConfiguredTaggedSecondaryAndDefaultReadConcern() {
        var client = mock(MongoClient.class);
        var database = mock(MongoDatabase.class);
        var metadataDatabase = mock(MongoDatabase.class);
        var readPreference = ReadPreference.secondary(new TagSet(new Tag("region", "east")));
        var operationTime = new BsonTimestamp(10, 2);

        when(client.getReadPreference()).thenReturn(readPreference);
        when(client.getDatabase("dbA")).thenReturn(database);
        when(database.withReadConcern(ReadConcern.DEFAULT)).thenReturn(metadataDatabase);
        when(metadataDatabase.runCommand(
                new BsonDocument("hello", new BsonInt32(1)),
                readPreference,
                BsonDocument.class))
                .thenReturn(new BsonDocument("operationTime", operationTime));

        assertThat(MongoUtils.hello(client, "dbA")).isEqualTo(operationTime);
        verify(database).withReadConcern(ReadConcern.DEFAULT);
        verify(metadataDatabase).runCommand(
                eq(new BsonDocument("hello", new BsonInt32(1))),
                eq(readPreference),
                eq(BsonDocument.class));
    }

    @Test
    void shouldGetReplicaSetName() {
        var rsNames = Collect.arrayListOf(null, "rs0", "rs1");
        var addresses = Collect.arrayListOf(new ServerAddress("host0"),
                new ServerAddress("host1"),
                new ServerAddress("host2"));

        List<ServerDescription> serverDescriptions = List.of(
                ServerDescription.builder()
                        .address(addresses.get(0))
                        .state(ServerConnectionState.CONNECTING)
                        .exception(new MongoSocketOpenException("can't connect", addresses.get(0)))
                        .build(),
                ServerDescription.builder()
                        .address(addresses.get(1))
                        .state(ServerConnectionState.CONNECTED)
                        .setName(rsNames.get(1))
                        .build(),
                ServerDescription.builder()
                        .address(addresses.get(2))
                        .state(ServerConnectionState.CONNECTED)
                        .setName(rsNames.get(2)) // In reality servers will have the same rs name
                        .build());

        ClusterDescription clusterDescription = new ClusterDescription(
                ClusterConnectionMode.MULTIPLE,
                ClusterType.REPLICA_SET,
                serverDescriptions);

        var actualRsName = MongoUtils.replicaSetName(clusterDescription);

        assertThat(actualRsName).hasValue(rsNames.get(1));
    }

    @Test
    void shouldNotGetReplicaSetName() {
        var address = new ServerAddress("host0");

        List<ServerDescription> serverDescriptions = List.of(
                ServerDescription.builder()
                        .address(address)
                        .state(ServerConnectionState.CONNECTING)
                        .exception(new MongoSocketOpenException("can't connect", address))
                        .build());

        ClusterDescription clusterDescription = new ClusterDescription(
                ClusterConnectionMode.MULTIPLE,
                ClusterType.REPLICA_SET,
                serverDescriptions);

        var actualRsName = MongoUtils.replicaSetName(clusterDescription);

        assertThat(actualRsName).isEmpty();
    }

}
