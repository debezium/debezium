/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink;

import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct; // @TODO find a good replacement for Kafka Connect's Struct and Schema

import io.debezium.annotation.Immutable;
import io.debezium.schema.SchemaFactory;
import io.debezium.sink.field.FieldDescriptor;
import io.debezium.sink.filter.FieldFilterFactory;

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

    Map<String, FieldDescriptor> allFields();

    Struct getFilteredKey(SinkConnectorConfig.PrimaryKeyMode primaryKeyMode, Set<String> primaryKeyFields, FieldFilterFactory.FieldNameFilter fieldsFilter);

    Struct getFilteredPayload(FieldFilterFactory.FieldNameFilter fieldsFilter);

}
