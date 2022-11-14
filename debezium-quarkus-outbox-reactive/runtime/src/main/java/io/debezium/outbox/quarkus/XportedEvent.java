package io.debezium.outbox.quarkus;

import java.time.Instant;
import java.util.Map;

public class XportedEvent {

    private long aggregateId;
    private String aggregateType;
    private String type;
    private Instant timestamp;
    private String payload;

    private Map<String, Object> additionalValues;

    public String getAggregateType() {
        return aggregateType;
    }

    public void setAggregateType(String aggregateType) {
        this.aggregateType = aggregateType;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, Object> getAdditionalValues() {
        return additionalValues;
    }

    public void setAdditionalValues(Map<String, Object> additionalValues) {
        this.additionalValues = additionalValues;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public long getAggregateId() {
        return aggregateId;
    }

    public void setAggregateId(long aggregateId) {
        this.aggregateId = aggregateId;
    }

}