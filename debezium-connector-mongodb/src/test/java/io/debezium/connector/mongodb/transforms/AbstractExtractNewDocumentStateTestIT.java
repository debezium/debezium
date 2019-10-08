/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;

import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.ConnectionContext.MongoPrimary;
import io.debezium.connector.mongodb.MongoDbConnector;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.connector.mongodb.MongoDbTaskContext;
import io.debezium.connector.mongodb.ReplicaSet;
import io.debezium.connector.mongodb.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.embedded.AbstractConnectorTest;

/**
 * Baseline for all integrations tests regarding MongoDB Update Operations
 *
 * @author Renato Mefi
 */
public abstract class AbstractExtractNewDocumentStateTestIT extends AbstractConnectorTest {

    protected static final String DB_NAME = "transform_operations";
    private MongoDbTaskContext context;

    protected ExtractNewDocumentState<SourceRecord> transformation;

    protected abstract String getCollectionName();

    protected String topicName() {
        return String.format("mongo.%s.%s", DB_NAME, this.getCollectionName());
    }

    @Before
    public void beforeEach() {
        Debug.disable();
        Print.disable();
        stopConnector();
        initializeConnectorTestFramework();

        transformation = new ExtractNewDocumentState<>();
        transformation.configure(Collections.emptyMap());

        // Use the DB configuration to define the connector's configuration ...
        Configuration config = TestHelper.getConfiguration().edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_WHITELIST, DB_NAME + "." + this.getCollectionName())
                .with(MongoDbConnectorConfig.LOGICAL_NAME, "mongo")
                .build();

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(primary(), DB_NAME);

        // Start the connector ...
        start(MongoDbConnector.class, config);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            if (context != null) {
                context.getConnectionContext().shutdown();
            }
        }
        transformation.close();
    }

    SourceRecord getRecordByOperation(Envelope.Operation operation) throws InterruptedException {
        final SourceRecord candidateRecord = getNextRecord();

        if (!((Struct) candidateRecord.value()).get("op").equals(operation.code())) {
            // MongoDB is not providing really consistent snapshot, so the initial insert
            // can arrive both in initial sync snapshot and in oplog
            return getRecordByOperation(operation);
        }

        return candidateRecord;
    }

    SourceRecord getNextRecord() throws InterruptedException {
        SourceRecords records = consumeRecordsByTopic(1);

        assertThat(records.recordsForTopic(this.topicName()).size()).isEqualTo(1);

        return records.recordsForTopic(this.topicName()).get(0);
    }

    protected SourceRecord getUpdateRecord() throws InterruptedException {
        return getRecordByOperation(Envelope.Operation.UPDATE);
    }

    protected MongoPrimary primary() {
        ReplicaSet replicaSet = ReplicaSet.parse(context.getConnectionContext().hosts());
        return context.getConnectionContext().primaryFor(
                replicaSet, context.filters(), connectionErrorHandler(3));
    }

    private BiConsumer<String, Throwable> connectionErrorHandler(int numErrorsBeforeFailing) {
        AtomicInteger attempts = new AtomicInteger();
        return (desc, error) -> {
            if (attempts.incrementAndGet() > numErrorsBeforeFailing) {
                fail("Unable to connect to primary after " + numErrorsBeforeFailing + " errors trying to " + desc + ": " + error);
            }
            logger.error("Error while attempting to {}: {}", desc, error.getMessage(), error);
        };
    }
}
