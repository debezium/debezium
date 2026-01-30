/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.extractnewstate;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.ExtractField;

import io.debezium.data.Envelope;

/**
 * A {@link ExtractRecordStrategy} is used by the transformer to determine
 * how to extract Truncate, Delete, Create and Update record from {@link Envelope}
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Harvey Yue
 */
public interface ExtractRecordStrategy<R extends ConnectRecord<R>> {

    R handleTruncateRecord(R record);

    R handleTombstoneRecord(R record);

    R handleDeleteRecord(R record);

    R handleRecord(R record);

    ExtractField<R> afterDelegate();

    ExtractField<R> beforeDelegate();

    ExtractField<R> updateDescriptionDelegate();

    boolean isRewriteMode();

    void close();
}
