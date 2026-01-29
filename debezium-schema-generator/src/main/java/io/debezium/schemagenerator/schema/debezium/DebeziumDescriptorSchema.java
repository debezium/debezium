/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.schema.debezium;

import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.debezium.metadata.ComponentMetadata;
import io.debezium.schemagenerator.model.debezium.ConnectorDescriptor;
import io.debezium.schemagenerator.schema.DefaultFieldFilter;
import io.debezium.schemagenerator.schema.Schema;
import io.debezium.schemagenerator.schema.SchemaDescriptor;
import io.debezium.schemagenerator.schema.SchemaName;

@SchemaName("debezium")
public class DebeziumDescriptorSchema implements Schema {

    private static final FieldFilter DEFAULT_FILTER = new DefaultFieldFilter();

    private static final SchemaDescriptor DESCRIPTOR = new SchemaDescriptor() {
        @Override
        public String getId() {
            return "debezium";
        }

        @Override
        public String getName() {
            return "Debezium Descriptor";
        }

        @Override
        public String getVersion() {
            return "1.0.0";
        }

        @Override
        public String getDescription() {
            return "Debezium Component Descriptor format";
        }
    };

    private final ObjectMapper objectMapper;

    public DebeziumDescriptorSchema() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    @Override
    public SchemaDescriptor getDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public void configure(Map<String, Object> config) {
        // No configuration needed for now
    }

    @Override
    public FieldFilter getFieldFilter() {
        return DEFAULT_FILTER;
    }

    @Override
    public String getSpec(ComponentMetadata componentMetadata) {
        DebeziumDescriptorSchemaCreator service = new DebeziumDescriptorSchemaCreator(
                componentMetadata, getFieldFilter());

        ConnectorDescriptor descriptor = service.buildDescriptor();

        try {
            return objectMapper.writeValueAsString(descriptor);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize connector descriptor to JSON", e);
        }
    }
}
