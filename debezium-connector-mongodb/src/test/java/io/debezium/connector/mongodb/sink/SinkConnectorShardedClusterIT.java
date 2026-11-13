/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.connector.mongodb.sink.junit.NetworkIsolatedMongoDbDatabaseProvider;
import io.debezium.connector.mongodb.sink.junit.SinkRecordFactory;
import io.debezium.connector.mongodb.sink.junit.SinkRecordFactoryArgumentsProvider;
import io.debezium.testing.testcontainers.MongoDbDeployment;
import io.debezium.testing.testcontainers.util.DockerUtils;

public class SinkConnectorShardedClusterIT implements SinkConnectorIT {

    protected static MongoDbDeployment mongo;

    @Override
    public MongoDbDeployment getMongoDbDeployment() {
        return mongo;
    }

    @BeforeAll
    static void beforeAll() {
        DockerUtils.enableFakeDnsIfRequired();
        mongo = new NetworkIsolatedMongoDbDatabaseProvider().mongoDbShardedCluster();
        mongo.start();
    }

    @AfterEach
    void afterEach() {
        cleanDatabase(DATABASE_NAME);
    }

    @AfterAll
    static void afterAll() {
        if (mongo != null) {
            mongo.stop();
        }
        DockerUtils.disableFakeDns();
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void testSinkConnectorWritesRecords(SinkRecordFactory factory) {
        checkSinkConnectorWritesRecords(factory);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void testMultiTopicRoutesToSeparateCollections(SinkRecordFactory factory) {
        checkMultiTopicRoutesToSeparateCollections(factory);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void testFieldFiltering(SinkRecordFactory factory) {
        checkFieldFiltering(factory);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void testPrimaryKeyModeDeduplication(SinkRecordFactory factory) {
        checkPrimaryKeyModeDeduplication(factory);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void testDeleteRemovesDocument(SinkRecordFactory factory) {
        checkDeleteRemovesDocument(factory);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void testTombstoneIsSkipped(SinkRecordFactory factory) {
        checkTombstoneIsSkipped(factory);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void testTruncateIsSkippedWhenDisabled(SinkRecordFactory factory) {
        checkTruncateIsSkippedWhenDisabled(factory);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void testTruncateDeletesAllDocumentsWhenEnabled(SinkRecordFactory factory) {
        checkTruncateDeletesAllDocumentsWhenEnabled(factory);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void testKafkaPrimaryKeyMode(SinkRecordFactory factory) {
        checkKafkaPrimaryKeyModeCreatesOneDocumentPerRecord(factory);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void testFieldIncludeList(SinkRecordFactory factory) {
        checkFieldIncludeList(factory);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void testRecordValuePrimaryKeyMode(SinkRecordFactory factory) {
        checkRecordValuePrimaryKeyMode(factory);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void testRecordHeaderPrimaryKeyMode(SinkRecordFactory factory) {
        checkRecordHeaderPrimaryKeyMode(factory);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void testPrimaryKeyFieldsRestriction(SinkRecordFactory factory) {
        checkPrimaryKeyFieldsRestriction(factory);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void testSameKeyUpsertProducesOneDocument(SinkRecordFactory factory) {
        checkSameKeyUpsertProducesOneDocument(factory);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void testKeylessPassthroughPreservesAllRecords(SinkRecordFactory factory) {
        checkKeylessPassthroughPreservesAllRecords(factory);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void testKeylessPassthroughPreservesIdenticalRecords(SinkRecordFactory factory) {
        checkKeylessPassthroughPreservesIdenticalRecords(factory);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void testDeduplicatingBufferCollapsesSameKeyUpdates(SinkRecordFactory factory) {
        checkDeduplicatingBufferCollapsesSameKeyUpdates(factory);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void testPassthroughBufferAppliesAllKeyedEvents(SinkRecordFactory factory) {
        checkPassthroughBufferAppliesAllKeyedEvents(factory);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void testPassthroughBufferWithDistinctKeys(SinkRecordFactory factory) {
        checkPassthroughBufferWithDistinctKeys(factory);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void testPassthroughBufferWithMixedKeys(SinkRecordFactory factory) {
        checkPassthroughBufferWithMixedKeys(factory);
    }
}
