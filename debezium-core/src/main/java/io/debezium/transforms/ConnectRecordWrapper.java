package io.debezium.transforms;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;

import io.debezium.transforms.tracing.ValueSchemaWrapper;

public interface ConnectRecordWrapper<R extends ConnectRecordWrapper<R>> {
    Object value();

    Schema valueSchema();

    Schema keySchema();

    Headers headers();

    Object key();

    String topic();

    Map<String, ?> kafkaPartition();

    Long timestamp();

    <R extends ConnectRecordWrapper<R>> R newRecord(String newTopic, Map<String, ?> kafkaPartition, Schema newKeySchema, Struct newKey, ValueSchemaWrapper valueSchema,
                                                    Object value, long timestamp);

    <R extends ConnectRecordWrapper<R>> R newRecord(String newTopic, Map<String, ?> kafkaPartition, Schema newKeySchema, Struct newKey, Schema valueSchema,
                                                    Object value, long timestamp);

    <R extends ConnectRecordWrapper<R>> R newRecord(String newTopic, Map<String, ?> kafkaPartition, Schema newKeySchema, Object newKey, Schema valueSchema,
                                                    Object value, long timestamp);

    <R extends ConnectRecordWrapper<R>> R newRecord(String string, Object o, Schema defineRecordKeySchema, Object defineRecordKey, Schema updatedSchema,
                                                    Object updatedValue, Long timestamp, Headers headers);
}
