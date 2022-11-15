package io.debezium.outbox.quarkus;

import java.time.Instant;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

public class XportedEvent {
    private static final ObjectMapper mapper = new ObjectMapper();
    private long aggregateId;
    private String aggregateType;
    private String type;
    private Instant timestamp;
    private Object payload;
    private Map<String, Object> additionalValues;

    public XportedEvent(ExportedEvent<?, ?> inEvent) {
        this.aggregateId = (long) inEvent.getAggregateId();
        this.aggregateType = inEvent.getAggregateType();
        this.type = inEvent.getType();
        this.timestamp = inEvent.getTimestamp();
        System.out.println(inEvent.getPayload().getClass());
        // if (inEvent.getPayload() instanceof JsonNode) {
        // try {
        // this.payload = mapper.writeValueAsString(inEvent.getPayload());
        // }
        // catch (JsonProcessingException e) {
        // throw new RuntimeException(e);
        // }
        // }
        // else {
        // this.payload = (String) inEvent.getPayload();
        // }
        //
        this.payload = inEvent.getPayload();
        System.out.println(this.payload);
        this.additionalValues = inEvent.getAdditionalFieldValues();

    }

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

    public Object getPayload() {
        return payload;
    }

    public void setPayload(Object payload) {
        this.payload = payload;
    }

    public long getAggregateId() {
        return aggregateId;
    }

    public void setAggregateId(long aggregateId) {
        this.aggregateId = aggregateId;
    }

}
