/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.connection.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.mongodb.client.MongoClient;
import com.mongodb.client.internal.MongoClientImpl;
import com.mongodb.client.internal.OperationExecutor;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.ListCollectionsOperation;
import com.mongodb.internal.operation.ListDatabasesOperation;

import io.debezium.connector.mongodb.MongoUtils;

class MongoDbClientOperationsTest {

    @Test
    void shouldListDatabaseNamesUsingPrimaryByDefault() {
        var client = mock(MongoClientImpl.class);
        var fallbackClientSupplier = mockClientSupplier();
        var executor = mock(OperationExecutor.class);
        @SuppressWarnings("unchecked")
        var cursor = (BatchCursor<BsonDocument>) mock(BatchCursor.class);
        var readPreference = ReadPreference.primary();

        when(client.getReadPreference()).thenReturn(readPreference);
        when(client.getCodecRegistry()).thenReturn(MongoClientSettings.getDefaultCodecRegistry());
        when(client.getSettings()).thenReturn(MongoClientSettings.builder().retryReads(true).build());
        when(client.getOperationExecutor()).thenReturn(executor);
        when(executor.execute(any(ListDatabasesOperation.class), eq(readPreference), eq(ReadConcern.DEFAULT)))
                .thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(true, false);
        when(cursor.next()).thenReturn(List.of(new BsonDocument("name", new BsonString("dbA"))));

        var databaseNames = new ArrayList<String>();
        MongoUtils.forEachDatabaseName(client, databaseNames::add);

        assertThat(databaseNames).containsExactly("dbA");
        assertDatabaseOperation(executor, readPreference);
        verify(cursor).close();
        verify(fallbackClientSupplier, never()).get();
    }

    @Test
    void shouldListDatabaseNamesAcrossCursorBatchesUsingTaggedSecondaryAndDefaultReadConcern() {
        var client = mock(MongoClientImpl.class);
        var fallbackClientSupplier = mockClientSupplier();
        var executor = mock(OperationExecutor.class);
        @SuppressWarnings("unchecked")
        var cursor = (BatchCursor<BsonDocument>) mock(BatchCursor.class);
        var readPreference = ReadPreference.secondary(new TagSet(new Tag("region", "east")));

        when(client.getReadPreference()).thenReturn(readPreference);
        when(client.getCodecRegistry()).thenReturn(MongoClientSettings.getDefaultCodecRegistry());
        when(client.getSettings()).thenReturn(MongoClientSettings.builder().retryReads(true).build());
        when(client.getOperationExecutor()).thenReturn(executor);
        when(executor.execute(any(ListDatabasesOperation.class), eq(readPreference), eq(ReadConcern.DEFAULT)))
                .thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(true, true, false);
        when(cursor.next()).thenReturn(
                List.of(new BsonDocument("name", new BsonString("dbA"))),
                List.of(new BsonDocument("name", new BsonString("dbB"))));

        var databaseNames = new ArrayList<String>();
        MongoDbClientOperations.forEachDatabaseName(client, fallbackClientSupplier, databaseNames::add);

        assertThat(databaseNames).containsExactly("dbA", "dbB");
        assertDatabaseOperation(executor, readPreference);
        verify(cursor).close();
        verify(fallbackClientSupplier, never()).get();
    }

    @Test
    void shouldListCollectionNamesAcrossCursorBatchesUsingConfiguredSecondary() {
        var client = mock(MongoClientImpl.class);
        var fallbackClientSupplier = mockClientSupplier();
        var executor = mock(OperationExecutor.class);
        @SuppressWarnings("unchecked")
        var cursor = (BatchCursor<BsonDocument>) mock(BatchCursor.class);
        var readPreference = ReadPreference.secondary();

        when(client.getReadPreference()).thenReturn(readPreference);
        when(client.getCodecRegistry()).thenReturn(MongoClientSettings.getDefaultCodecRegistry());
        when(client.getSettings()).thenReturn(MongoClientSettings.builder().retryReads(true).build());
        when(client.getOperationExecutor()).thenReturn(executor);
        when(executor.execute(any(ListCollectionsOperation.class), eq(readPreference), eq(ReadConcern.DEFAULT)))
                .thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(true, true, false);
        when(cursor.next()).thenReturn(
                List.of(new BsonDocument("name", new BsonString("collectionA"))),
                List.of(new BsonDocument("name", new BsonString("collectionB"))));

        var collectionNames = new ArrayList<String>();
        MongoUtils.forEachCollectionNameInDatabase(client, "dbA", collectionNames::add);

        assertThat(collectionNames).containsExactly("collectionA", "collectionB");
        assertCollectionOperation(executor, readPreference);
        verify(cursor).close();
        verify(fallbackClientSupplier, never()).get();
    }

    @Test
    void shouldUseSettingsEquivalentClientForWrappedMongoClient() {
        var wrappedClient = mock(MongoClient.class);
        var fallbackClient = mock(MongoClientImpl.class);
        var executor = mock(OperationExecutor.class);
        @SuppressWarnings("unchecked")
        var cursor = (BatchCursor<BsonDocument>) mock(BatchCursor.class);
        var readPreference = ReadPreference.secondary(new TagSet(new Tag("region", "east")));

        when(wrappedClient.getReadPreference()).thenReturn(readPreference);
        when(fallbackClient.getCodecRegistry()).thenReturn(MongoClientSettings.getDefaultCodecRegistry());
        when(fallbackClient.getSettings()).thenReturn(MongoClientSettings.builder().retryReads(true).build());
        when(fallbackClient.getOperationExecutor()).thenReturn(executor);
        when(executor.execute(any(ListCollectionsOperation.class), eq(readPreference), eq(ReadConcern.DEFAULT)))
                .thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(true, false);
        when(cursor.next()).thenReturn(List.of(new BsonDocument("name", new BsonString("collectionA"))));

        var collectionNames = new ArrayList<String>();
        MongoDbClientOperations.forEachCollectionNameInDatabase(wrappedClient, () -> fallbackClient, "dbA", collectionNames::add);

        assertThat(collectionNames).containsExactly("collectionA");
        assertCollectionOperation(executor, readPreference);
        verify(cursor).close();
        verify(fallbackClient).close();
    }

    @Test
    void shouldSupportWrappedClientThroughCustomFactoryContract() {
        var wrappedClient = mock(MongoClient.class);
        var metadataClient = mock(MongoClientImpl.class);
        var executor = mock(OperationExecutor.class);
        @SuppressWarnings("unchecked")
        var cursor = (BatchCursor<BsonDocument>) mock(BatchCursor.class);
        var readPreference = ReadPreference.secondary(new TagSet(new Tag("region", "east")));
        var factory = new MongoDbClientFactory() {
            @Override
            public MongoClientSettings getMongoClientSettings() {
                return MongoClientSettings.builder().readPreference(readPreference).build();
            }

            @Override
            public MongoClient getMongoClient() {
                return wrappedClient;
            }

            @Override
            public MongoClient getMetadataMongoClient() {
                return metadataClient;
            }
        };

        when(wrappedClient.getReadPreference()).thenReturn(readPreference);
        when(metadataClient.getCodecRegistry()).thenReturn(MongoClientSettings.getDefaultCodecRegistry());
        when(metadataClient.getSettings()).thenReturn(factory.getMongoClientSettings());
        when(metadataClient.getOperationExecutor()).thenReturn(executor);
        when(executor.execute(any(ListCollectionsOperation.class), eq(readPreference), eq(ReadConcern.DEFAULT)))
                .thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(true, false);
        when(cursor.next()).thenReturn(List.of(new BsonDocument("name", new BsonString("collectionA"))));

        var collectionNames = new ArrayList<String>();
        MongoUtils.forEachCollectionNameInDatabase(factory, factory.getMongoClient(), "dbA", collectionNames::add);

        assertThat(collectionNames).containsExactly("collectionA");
        assertCollectionOperation(executor, readPreference);
        verify(metadataClient).close();
    }

    @SuppressWarnings("unchecked")
    private static Supplier<MongoClient> mockClientSupplier() {
        return mock(Supplier.class);
    }

    @SuppressWarnings("rawtypes")
    private static void assertDatabaseOperation(OperationExecutor executor, ReadPreference readPreference) {
        var operation = ArgumentCaptor.forClass(ListDatabasesOperation.class);
        verify(executor).execute(operation.capture(), eq(readPreference), eq(ReadConcern.DEFAULT));
        assertThat(operation.getValue().getNameOnly()).isTrue();
        assertThat(operation.getValue().getAuthorizedDatabasesOnly()).isNull();
        assertThat(operation.getValue().getRetryReads()).isTrue();
    }

    @SuppressWarnings("rawtypes")
    private static void assertCollectionOperation(OperationExecutor executor, ReadPreference readPreference) {
        var operation = ArgumentCaptor.forClass(ListCollectionsOperation.class);
        verify(executor).execute(operation.capture(), eq(readPreference), eq(ReadConcern.DEFAULT));
        assertThat(operation.getValue().isNameOnly()).isTrue();
        assertThat(operation.getValue().isAuthorizedCollections()).isFalse();
    }
}
