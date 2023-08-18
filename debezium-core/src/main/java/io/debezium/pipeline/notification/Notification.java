/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.notification;

import java.beans.ConstructorProperties;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.pipeline.notification.channels.jmx.JmxNotificationChannel;

public class Notification {

    private static final Logger LOGGER = LoggerFactory.getLogger(JmxNotificationChannel.class);

    public static final String ID_KEY = "id";
    public static final String TYPE = "type";
    public static final String AGGREGATE_TYPE = "aggregate_type";
    public static final String ADDITIONAL_DATA = "additional_data";
    public static final ObjectMapper MAPPER = new ObjectMapper();

    @JsonIgnore
    private final String id;
    private final String aggregateType;
    private final String type;
    private final Map<String, String> additionalData;

    @ConstructorProperties({ "id", "aggregateType", "type", "additionalData" })
    public Notification(String id, String aggregateType, String type, Map<String, String> additionalData) {
        this.id = id;
        this.aggregateType = aggregateType;
        this.type = type;
        this.additionalData = additionalData;
    }

    public String getId() {
        return id;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public String getType() {
        return type;
    }

    public Map<String, String> getAdditionalData() {
        return additionalData;
    }

    @Override
    public String toString() {
        return "{" +
                "aggregateType='" + aggregateType + '\'' +
                ", type='" + type + '\'' +
                ", additionalData=" + additionalData +
                '}';
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Notification that = (Notification) o;
        return Objects.equals(id, that.id) && Objects.equals(aggregateType, that.aggregateType) && Objects.equals(type,
                that.type) && Objects.equals(additionalData, that.additionalData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, aggregateType, type, additionalData);
    }

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        }
        catch (JsonProcessingException e) {
            LOGGER.warn("Error converting the notification object to json. Providing default toString value...", e);
            return toString();
        }
    }

    public static final class Builder {
        private String id;
        private String aggregateType;
        private String type;
        private Map<String, String> additionalData;

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withAggregateType(String aggregateType) {
            this.aggregateType = aggregateType;
            return this;
        }

        public Builder withType(String type) {
            this.type = type;
            return this;
        }

        public Builder withAdditionalData(Map<String, String> additionalData) {
            this.additionalData = additionalData;
            return this;
        }

        public Notification build() {
            return new Notification(id, aggregateType, type, additionalData);
        }
    }
}
