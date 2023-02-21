/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.AbstractMongoConnectorIT;
import io.debezium.connector.mongodb.MongoDbConnector;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.connector.mongodb.MongoDbTaskContext;
import io.debezium.connector.mongodb.TestHelper;
import io.debezium.data.Envelope;

/**
 * Baseline for all integrations tests regarding MongoDB Update Operations
 *
 * @author Renato Mefi
 */
public abstract class AbstractExtractNewDocumentStateTestIT extends AbstractMongoConnectorIT {

    protected static final String DB_NAME = "transform_operations";
    protected static final String SERVER_NAME = "mongo";

    protected ExtractNewDocumentState<SourceRecord> transformation;

    protected abstract String getCollectionName();

    protected String topicName() {
        return String.format("%s.%s.%s", SERVER_NAME, DB_NAME, this.getCollectionName());
    }

    @Before
    public void beforeEach() {
        // Use the DB configuration to define the connector's configuration ...
        Configuration config = getBaseConfigBuilder().build();

        beforeEach(config);
    }

    public void beforeEach(Configuration config) {
        super.beforeEach();

        transformation = new ExtractNewDocumentState<>();
        transformation.configure(Collections.emptyMap());

        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config);

        // Cleanup database
        TestHelper.cleanDatabase(mongo, DB_NAME);

        // Start the connector ...
        start(MongoDbConnector.class, config);
    }

    @After
    public void afterEach() {
        super.afterEach();
        transformation.close();
    }

    protected void restartConnectorWithoutEmittingTombstones() {
        // stop connector
        afterEach();

        // reconfigure and restart
        Configuration config = getBaseConfigBuilder()
                .with(MongoDbConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .build();

        beforeEach(config);
    }

    protected Configuration.Builder getBaseConfigBuilder() {
        return TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, DB_NAME + "." + this.getCollectionName())
                .with(CommonConnectorConfig.TOPIC_PREFIX, SERVER_NAME);
    }

    protected void restartConnectorWithConfig(Configuration config) {
        // stop connector
        afterEach();

        beforeEach(config);
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
}
