/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.dlq.ErrorReporter;
import io.debezium.sink.DebeziumSinkRecord;

final class MongoSinkRecordProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoSinkRecordProcessor.class);

    private MongoSinkRecordProcessor() {
    }

    static List<List<MongoProcessedSinkRecordData>> orderedGroupByTopicAndNamespace(
                                                                                    final Collection<SinkRecord> records,
                                                                                    final MongoDbSinkConnectorConfig sinkConfig,
                                                                                    final ErrorReporter errorReporter) {
        LOGGER.debug("Number of sink records to process: {}", records.size());

        List<List<MongoProcessedSinkRecordData>> orderedProcessedSinkRecordData = new ArrayList<>();
        List<MongoProcessedSinkRecordData> groupedBatch = new ArrayList<>();
        MongoProcessedSinkRecordData previous = null;

        for (SinkRecord kafkaSinkRecord : records) {
            DebeziumSinkRecord record = new KafkaDebeziumSinkRecord(kafkaSinkRecord, sinkConfig.cloudEventsSchemaNamePattern());
            MongoProcessedSinkRecordData processedData = new MongoProcessedSinkRecordData(record, sinkConfig);

            if (processedData.getException() != null) {
                errorReporter.report(processedData.getSinkRecord(), processedData.getException());
                continue;
            }
            else if (processedData.getNamespace() == null || processedData.getWriteModel() == null) {
                // Some CDC events can be Noops (eg tombstone events)
                continue;
            }

            if (previous == null) {
                previous = processedData;
            }

            int maxBatchSize = processedData.getConfig().getBatchSize();
            if (maxBatchSize > 0 && groupedBatch.size() == maxBatchSize
                    || !previous.getSinkRecord().topicName().equals(processedData.getSinkRecord().topicName())
                    || !previous.getNamespace().equals(processedData.getNamespace())) {

                orderedProcessedSinkRecordData.add(groupedBatch);
                groupedBatch = new ArrayList<>();
            }
            previous = processedData;
            groupedBatch.add(processedData);
        }

        if (!groupedBatch.isEmpty()) {
            orderedProcessedSinkRecordData.add(groupedBatch);
        }
        return orderedProcessedSinkRecordData;
    }
}
