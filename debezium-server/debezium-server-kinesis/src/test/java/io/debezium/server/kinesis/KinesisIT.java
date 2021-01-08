/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.kinesis;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import io.debezium.server.DebeziumServer;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to Kinesis stream.
 *
 * @author Jiri Pechanec
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
public class KinesisIT {

    private static final int MESSAGE_COUNT = 4;
    // The stream of this name must exist and be empty
    private static final String STREAM_NAME = "testc.inventory.customers";

    protected static KinesisClient kinesis = null;

    {
        Testing.Files.delete(KinesisTestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(KinesisTestConfigSource.OFFSET_STORE_PATH);
    }

    @Inject
    DebeziumServer server;

    void setupDependencies(@Observes ConnectorStartedEvent event) {
        kinesis = KinesisClient.builder()
                .region(Region.of(KinesisTestConfigSource.KINESIS_REGION))
                .credentialsProvider(ProfileCredentialsProvider.create("default"))
                .build();
    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw (Exception) event.getError().get();
        }
    }

    @Test
    public void testKinesis() throws Exception {
        Testing.Print.enable();
        final GetShardIteratorResponse iteratorResponse = kinesis.getShardIterator(GetShardIteratorRequest.builder()
                .streamName(STREAM_NAME)
                .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                .shardId("0")
                .build());
        final List<Record> records = new ArrayList<>();
        Awaitility.await().atMost(Duration.ofSeconds(KinesisTestConfigSource.waitForSeconds())).until(() -> {
            final GetRecordsResponse recordsResponse = kinesis.getRecords(GetRecordsRequest.builder()
                    .shardIterator(iteratorResponse.shardIterator())
                    .limit(MESSAGE_COUNT)
                    .build());
            records.addAll(recordsResponse.records());
            return records.size() >= MESSAGE_COUNT;
        });
    }
}
