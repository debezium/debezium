/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cockroachdb;

import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Maps a CockroachDB enriched changefeed message into a Kafka Connect {@link SourceRecord}s to be sent to Kafka.
 * Expects JSON-parsed values and metadata to be passed in for construction.
 *
 * @author Virag Tripathi
 */
public class CockroachDBSourceRecordMapper implements ChangeRecordEmitter.Receiver<CockroachDBPartition> {

    private final CockroachDBPartition partition;
    private final OffsetContext offsetContext;
    private final TopicNamingStrategy<DataCollectionId> topicNamingStrategy;
    private final ChangeEventQueue<DataChangeEvent> queue;

    public CockroachDBSourceRecordMapper(CockroachDBPartition partition,
                                         OffsetContext offsetContext,
                                         TopicNamingStrategy<DataCollectionId> topicNamingStrategy,
                                         ChangeEventQueue<DataChangeEvent> queue) {
        this.partition = partition;
        this.offsetContext = offsetContext;
        this.topicNamingStrategy = topicNamingStrategy;
        this.queue = queue;
    }

    @Override
    public void changeRecord(CockroachDBPartition partition,
                             DataCollectionSchema schema,
                             Envelope.Operation operation,
                             Object key,
                             Struct value,
                             OffsetContext offset,
                             org.apache.kafka.connect.header.ConnectHeaders headers) {

        final String topic = topicNamingStrategy.dataChangeTopic(schema.id());
        final Schema keySchema = schema.getKeySchema();
        final Schema valueSchema = schema.getEnvelopeSchema().schema();

        SourceRecord record = new SourceRecord(
                partition.getSourcePartition(),
                offset.getOffset(),
                topic,
                null, // partition is determined by Kafka
                keySchema,
                key,
                valueSchema,
                value,
                offset.getClock().currentTimeInMillis(),
                headers);

        queue.enqueue(new DataChangeEvent(record));
    }

    @Override
    public void schemaChange(String database, List<io.debezium.schema.SchemaChangeEvent> changes) {
        // No-op for CockroachDB
    }
}
