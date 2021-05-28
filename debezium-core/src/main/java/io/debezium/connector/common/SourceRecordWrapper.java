package io.debezium.connector.common;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;

public interface SourceRecordWrapper {

    SourceRecordWrapper newRecord(String topic, Integer kafkaPartition, Schema keySchema, Object key, Schema valueSchema, Object value, Long timestamp);

    SourceRecordWrapper newRecord(String topic, Integer kafkaPartition, Schema keySchema, Object key, Schema valueSchema, Object value,
                                  Long timestamp, Iterable<Header> headers);

    Map<String, Object> sourceOffset();

    String topic();

    Object key();

    Object value();

    Integer kafkaPartition();

    Schema keySchema();

    Schema valueSchema();

    Long timestamp();

    Headers headers();

    String sourcePartition();
}
