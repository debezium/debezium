/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.mongodb.MongoSocketOpenException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListCollectionNamesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;

import io.debezium.connector.mongodb.connection.client.MongoDbClientFactory;
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
    }

    @Test
    void shouldRunHelloUsingConfiguredTaggedSecondary() {
        var client = mock(MongoClient.class);
        var database = mock(MongoDatabase.class);
        var metadataDatabase = mock(MongoDatabase.class);
        var readPreference = ReadPreference.secondary(new TagSet(new Tag("region", "east")));
        var operationTime = new BsonTimestamp(10, 2);
        var result = new BsonDocument("operationTime", operationTime);

        when(client.getReadPreference()).thenReturn(readPreference);
        when(client.getDatabase("dbA")).thenReturn(database);
        when(database.withReadConcern(ReadConcern.DEFAULT)).thenReturn(metadataDatabase);
        when(metadataDatabase.runCommand(anyBsonDocument(), eq(readPreference), eq(BsonDocument.class)))
                .thenReturn(result);

        assertThat(MongoUtils.hello(client, "dbA")).isEqualTo(operationTime);
        verify(database).withReadConcern(ReadConcern.DEFAULT);
        var command = captureCommand(metadataDatabase);
        assertThat(command).isEqualTo(BsonDocument.parse("{ hello: 1 }"));
    }

    @Test
    void shouldReadTopologyDocumentsUsingDefaultReadConcern() {
        var clientFactory = mock(MongoDbClientFactory.class);
        var client = mock(MongoClient.class);
        var database = mock(MongoDatabase.class);
        @SuppressWarnings("unchecked")
        var collection = (MongoCollection<Document>) mock(MongoCollection.class);
        @SuppressWarnings("unchecked")
        var metadataCollection = (MongoCollection<Document>) mock(MongoCollection.class);
        @SuppressWarnings("unchecked")
        var documents = (FindIterable<Document>) mock(FindIterable.class);
        @SuppressWarnings("unchecked")
        var cursor = (MongoCursor<Document>) mock(MongoCursor.class);
        var shard = new Document("_id", "shardA");

        doAnswer(invocation -> {
            invocation.getArgument(1, java.util.function.Consumer.class).accept("config");
            return null;
        }).when(clientFactory).forEachDatabaseName(eq(client), org.mockito.ArgumentMatchers.any());
        doAnswer(invocation -> {
            invocation.getArgument(2, java.util.function.Consumer.class).accept("shards");
            return null;
        }).when(clientFactory).forEachCollectionNameInDatabase(eq(client), eq("config"), org.mockito.ArgumentMatchers.any());
        when(client.getDatabase("config")).thenReturn(database);
        when(database.getCollection("shards")).thenReturn(collection);
        when(collection.withReadConcern(ReadConcern.DEFAULT)).thenReturn(metadataCollection);
        when(metadataCollection.find()).thenReturn(documents);
        when(documents.iterator()).thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(true, false);
        when(cursor.next()).thenReturn(shard);

        var results = new ArrayList<Document>();
        MongoUtils.onCollectionDocuments(clientFactory, client, "config", "shards", results::add);

        assertThat(results).containsExactly(shard);
        verify(collection).withReadConcern(ReadConcern.DEFAULT);
        verify(cursor).close();
    }

    @Test
    void shouldPreserveCompatibilityMethodSignatures() throws NoSuchMethodException {
        assertThat(MongoUtils.class.getMethod("forEachDatabaseName", MongoClient.class, java.util.function.Consumer.class)).isNotNull();
        assertThat(MongoUtils.class.getMethod("forEachCollectionNameInDatabase", MongoClient.class, String.class,
                java.util.function.Consumer.class)).isNotNull();
        assertThat(MongoUtils.class.getMethod("onDatabase", MongoClient.class, String.class, java.util.function.Consumer.class)).isNotNull();
        assertThat(MongoUtils.class.getMethod("onCollection", MongoClient.class, String.class, String.class,
                java.util.function.Consumer.class)).isNotNull();
        assertThat(MongoUtils.class.getMethod("onCollectionDocuments", MongoClient.class, String.class, String.class,
                io.debezium.function.BlockingConsumer.class)).isNotNull();
    }

    @Test
    void shouldUseLegacyCollectionEnumerationForCustomPrimaryClient() {
        var client = mock(MongoClient.class);
        var database = mock(MongoDatabase.class);
        @SuppressWarnings("unchecked")
        var collectionNames = mock(ListCollectionNamesIterable.class);
        @SuppressWarnings("unchecked")
        var cursor = (MongoCursor<String>) mock(MongoCursor.class);

        when(client.getReadPreference()).thenReturn(ReadPreference.primary());
        when(client.getDatabase("dbA")).thenReturn(database);
        when(database.listCollectionNames()).thenReturn(collectionNames);
        when(collectionNames.iterator()).thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(true, false);
        when(cursor.next()).thenReturn("collectionA");

        var results = new ArrayList<String>();
        MongoUtils.forEachCollectionNameInDatabase(client, "dbA", results::add);

        assertThat(results).containsExactly("collectionA");
        verify(cursor).close();
    }

    @Test
    void shouldRejectCustomSecondaryClientWithoutFactoryAdapter() {
        var client = mock(MongoClient.class);
        when(client.getReadPreference()).thenReturn(ReadPreference.secondary());

        assertThatThrownBy(() -> MongoUtils.forEachCollectionNameInDatabase(client, "dbA", name -> {
        }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("MongoDbClientFactory-aware overload");
    }

    @Test
    void shouldListDatabasesForCustomSecondaryClientUsingDefaultReadConcern() {
        var client = mock(MongoClient.class);
        var database = mock(MongoDatabase.class);
        var metadataDatabase = mock(MongoDatabase.class);
        var readPreference = ReadPreference.secondary(new TagSet(new Tag("region", "east")));
        var result = new BsonDocument("databases", new BsonArray(List.of(
                new BsonDocument("name", new BsonString("dbA")))));

        when(client.getReadPreference()).thenReturn(readPreference);
        when(client.getDatabase("admin")).thenReturn(database);
        when(database.withReadConcern(ReadConcern.DEFAULT)).thenReturn(metadataDatabase);
        when(metadataDatabase.runCommand(anyBsonDocument(), eq(readPreference), eq(BsonDocument.class))).thenReturn(result);

        var databaseNames = new ArrayList<String>();
        MongoUtils.forEachDatabaseName(client, databaseNames::add);

        assertThat(databaseNames).containsExactly("dbA");
        verify(database).withReadConcern(ReadConcern.DEFAULT);
        assertThat(captureCommand(metadataDatabase))
                .isEqualTo(BsonDocument.parse("{ listDatabases: 1, nameOnly: true }"));
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

    private static BsonDocument anyBsonDocument() {
        return org.mockito.ArgumentMatchers.any(BsonDocument.class);
    }

    private static BsonDocument captureCommand(MongoDatabase database) {
        var command = ArgumentCaptor.forClass(BsonDocument.class);
        verify(database).runCommand(command.capture(), org.mockito.ArgumentMatchers.any(ReadPreference.class), eq(BsonDocument.class));
        return command.getValue();
    }

}
