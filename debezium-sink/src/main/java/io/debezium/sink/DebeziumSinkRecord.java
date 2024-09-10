/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink;

import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct; // @TODO find a good replacement for Kafka Connect's Struct and Schema

import io.debezium.annotation.Immutable;
import io.debezium.schema.SchemaFactory;
import io.debezium.sink.SinkConnectorConfig.PrimaryKeyMode;

@Immutable
public interface DebeziumSinkRecord {

    String SCHEMA_CHANGE_VALUE = SchemaFactory.SCHEMA_CHANGE_VALUE;

    String topicName();

    Integer partition();

    long offset();

    Object key();

    Schema keySchema();

    Object value();

    Schema valueSchema();

    boolean isDebeziumMessage();

    boolean isSchemaChange();

    boolean isTombstone();

    boolean isDelete();

    boolean isTruncate();

    Struct getPayload();

    List<String> keyFieldNames();

    Struct getKeyStruct(PrimaryKeyMode primaryKeyMode);

}
