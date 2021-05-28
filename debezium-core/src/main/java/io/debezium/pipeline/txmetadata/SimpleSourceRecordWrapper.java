package io.debezium.pipeline.txmetadata;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;

import io.debezium.connector.common.SourceRecordWrapper;

public class SimpleSourceRecordWrapper implements SourceRecordWrapper {

    private String topic;
    private Integer kafkaPartition;
    private Schema keySchema;
    private Object key;
    private Schema valueSchema;
    private Object value;
    private Long timestamp;
    private Headers headers;

    private Map<String, ?> sourcePartition;
    private Map<String, ?> sourceOffset;

    public SimpleSourceRecordWrapper(Map<String, ?> partition, Map<String, ?> offset, String topicName, Object o, Schema schema, Struct key, Schema valueSchema,
                                     Struct value) {
        this.sourcePartition = partition;
        this.sourceOffset = offset;
        this.topic = topicName;
        this.keySchema = schema;
        this.key = key;
        this.valueSchema = valueSchema;
        this.value = value;
    }

    public SimpleSourceRecordWrapper(Map<String, ?> sourcePartition, Map<String, ?> sourceOffset, String topic, Integer kafkaPartition, Schema keySchema, Object key,
                                     Schema valueSchema, Object value, Long timestamp, Iterable<Header> headers) {
        this.sourcePartition = sourcePartition;
        this.sourceOffset = sourceOffset;
        this.topic = topic;
        this.kafkaPartition = kafkaPartition;
        this.keySchema = keySchema;
        this.key = key;
        this.valueSchema = valueSchema;
        this.value = value;
        this.timestamp = timestamp;
        this.headers = new ConnectHeaders(headers);
    }

    public SimpleSourceRecordWrapper(String sourcePartition, Map<String, ?> newOffset, String topic, Integer kafkaPartition, Schema keySchema, Object key,
                                     Schema valueSchema, Object value) {

    }

    public SimpleSourceRecordWrapper(Map<String, ?> partition, Map<String, ?> sourceRecordOffset, String topicName, Integer partitionNum, Schema keySchema, Object oldKey,
                                     Object o, Object o1) {

    }

    public SimpleSourceRecordWrapper(Map<String, ?> partition, Map<String, ?> sourceRecordOffset, String topicName, Integer partitionNum, Schema keySchema, Struct newkey,
                                     Schema schema, Struct update) {

    }

    @Override
    public SourceRecordWrapper newRecord(String topic, Integer kafkaPartition, Schema keySchema, Object key, Schema valueSchema, Object value, Long timestamp) {
        return newRecord(topic, kafkaPartition, keySchema, key, valueSchema, value, timestamp, headers().duplicate());
    }

    @Override
    public SourceRecordWrapper newRecord(String topic, Integer kafkaPartition, Schema keySchema, Object key, Schema valueSchema, Object value,
                                         Long timestamp, Iterable<Header> headers) {
        return new SimpleSourceRecordWrapper(sourcePartition, sourceOffset, topic, kafkaPartition, keySchema, key, valueSchema, value, timestamp, headers);
    }

    @Override
    public Map<String, Object> sourceOffset() {
        return (Map<String, Object>) sourceOffset;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public Object key() {
        return key;
    }

    @Override
    public Object value() {
        return value;
    }

    public SimpleSourceRecordWrapper(String topic, Integer kafkaPartition,
                                     Schema keySchema, Object key,
                                     Schema valueSchema, Object value,
                                     Long timestamp) {
        this(topic, kafkaPartition, keySchema, key, valueSchema, value, timestamp, new ConnectHeaders());
    }

    public SimpleSourceRecordWrapper(String topic, Integer kafkaPartition,
                                     Schema keySchema, Object key,
                                     Schema valueSchema, Object value,
                                     Long timestamp, Iterable<Header> headers) {
        this.topic = topic;
        this.kafkaPartition = kafkaPartition;
        this.keySchema = keySchema;
        this.key = key;
        this.valueSchema = valueSchema;
        this.value = value;
        this.timestamp = timestamp;
        if (headers instanceof ConnectHeaders) {
            this.headers = (ConnectHeaders) headers;
        }
        else {
            this.headers = new ConnectHeaders(headers);
        }
    }

    @Override
    public Integer kafkaPartition() {
        return kafkaPartition;
    }

    @Override
    public Schema keySchema() {
        return keySchema;
    }

    @Override
    public Schema valueSchema() {
        return valueSchema;
    }

    @Override
    public Long timestamp() {
        return timestamp;
    }

    /**
     * Get the headers for this record.
     *
     * @return the headers; never null
     */
    @Override
    public Headers headers() {
        return headers;
    }

    @Override
    public String sourcePartition() {
        return null;
    }

    @Override
    public String toString() {
        return "ConnectRecord{" +
                "topic='" + topic + '\'' +
                ", kafkaPartition=" + kafkaPartition +
                ", key=" + key +
                ", keySchema=" + keySchema +
                ", value=" + value +
                ", valueSchema=" + valueSchema +
                ", timestamp=" + timestamp +
                ", headers=" + headers +
                '}';
    }

    @Override
    public int hashCode() {
        int result = topic != null ? topic.hashCode() : 0;
        result = 31 * result + (kafkaPartition != null ? kafkaPartition.hashCode() : 0);
        result = 31 * result + (keySchema != null ? keySchema.hashCode() : 0);
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (valueSchema != null ? valueSchema.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        result = 31 * result + headers.hashCode();
        return result;
    }
}
