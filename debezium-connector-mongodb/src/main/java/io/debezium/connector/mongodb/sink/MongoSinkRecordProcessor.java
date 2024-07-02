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

import io.debezium.dlq.ErrorReporter;

final class MongoSinkRecordProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoSinkRecordProcessor.class);

    static List<List<MongoProcessedSinkRecordData>> orderedGroupByTopicAndNamespace(
                                                                                    final Collection<SinkRecord> records,
                                                                                    final MongoDbSinkConnectorConfig sinkConfig,
                                                                                    final ErrorReporter errorReporter) {
        LOGGER.debug("Number of sink records to process: {}", records.size());

        List<List<MongoProcessedSinkRecordData>> orderedProcessedSinkRecordData = new ArrayList<>();
        List<MongoProcessedSinkRecordData> currentGroup = new ArrayList<>();
        MongoProcessedSinkRecordData previous = null;

        for (SinkRecord record : records) {
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
            if (maxBatchSize > 0 && currentGroup.size() == maxBatchSize
                    || !previous.getSinkRecord().topic().equals(processedData.getSinkRecord().topic())
                    || !previous.getNamespace().equals(processedData.getNamespace())) {

                orderedProcessedSinkRecordData.add(currentGroup);
                currentGroup = new ArrayList<>();
            }
            previous = processedData;
            currentGroup.add(processedData);
        }

        if (!currentGroup.isEmpty()) {
            orderedProcessedSinkRecordData.add(currentGroup);
        }
        return orderedProcessedSinkRecordData;
    }

    private MongoSinkRecordProcessor() {
    }
}