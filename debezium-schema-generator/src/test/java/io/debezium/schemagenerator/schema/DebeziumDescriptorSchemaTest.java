/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.util.Set;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

import io.debezium.config.ConfigDefinition;
import io.debezium.config.DependentFieldMatcher;
import io.debezium.config.Field;
import io.debezium.metadata.ConnectorDescriptor;
import io.debezium.metadata.ConnectorMetadata;
import io.debezium.schemagenerator.schema.debezium.DebeziumDescriptorSchema;

/**
 * Unit test for {@link DebeziumDescriptorSchema} that validates the generated
 * JSON output against the DDD-13 JSON Schema.
 */
class DebeziumDescriptorSchemaTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testGeneratedDescriptorMatchesSchema() throws Exception {
        ConnectorMetadata connectorMetadata = createMockConnectorMetadata();

        DebeziumDescriptorSchema schema = new DebeziumDescriptorSchema();
        String descriptorJson = schema.getSpec(connectorMetadata);

        JsonNode descriptorNode = objectMapper.readTree(descriptorJson);

        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);
        InputStream schemaStream = getClass().getClassLoader()
                .getResourceAsStream("debezium-descriptor-schema.json");
        assertThat(schemaStream).isNotNull();

        JsonSchema jsonSchema = factory.getSchema(schemaStream);

        Set<ValidationMessage> validationMessages = jsonSchema.validate(descriptorNode);

        assertThat(validationMessages)
                .withFailMessage(() -> "Validation failed with errors: " +
                        String.join(", ", validationMessages.stream()
                                .map(ValidationMessage::getMessage)
                                .toArray(String[]::new)))
                .isEmpty();
    }

    @Test
    void testDescriptorStructure() throws Exception {
        ConnectorMetadata connectorMetadata = createMockConnectorMetadata();

        DebeziumDescriptorSchema schema = new DebeziumDescriptorSchema();
        String descriptorJson = schema.getSpec(connectorMetadata);

        JsonNode descriptorNode = objectMapper.readTree(descriptorJson);

        assertThat(descriptorNode.has("name")).isTrue();
        assertThat(descriptorNode.has("type")).isTrue();
        assertThat(descriptorNode.has("version")).isTrue();
        assertThat(descriptorNode.has("metadata")).isTrue();
        assertThat(descriptorNode.has("properties")).isTrue();
        assertThat(descriptorNode.has("groups")).isTrue();

        assertThat(descriptorNode.get("name").asText()).isEqualTo("Debezium PostgreSQL Connector");
        assertThat(descriptorNode.get("type").asText()).isEqualTo("source-connector");
        assertThat(descriptorNode.get("version").asText()).isEqualTo("1.0.0-SNAPSHOT");

        JsonNode metadata = descriptorNode.get("metadata");
        assertThat(metadata.has("description")).isTrue();
        assertThat(metadata.get("description").asText()).contains("PostgreSQL");

        JsonNode properties = descriptorNode.get("properties");
        assertThat(properties.isArray()).isTrue();
        assertThat(properties.size()).isGreaterThan(0);

        JsonNode firstProperty = properties.get(0);
        assertThat(firstProperty.has("name")).isTrue();
        assertThat(firstProperty.has("type")).isTrue();
        assertThat(firstProperty.has("display")).isTrue();

        JsonNode display = firstProperty.get("display");
        assertThat(display.has("label")).isTrue();
        assertThat(display.has("description")).isTrue();
        assertThat(display.has("group")).isTrue();
        assertThat(display.has("groupOrder")).isTrue();
        assertThat(display.has("width")).isTrue();
        assertThat(display.has("importance")).isTrue();

        JsonNode groups = descriptorNode.get("groups");
        assertThat(groups.isArray()).isTrue();
        assertThat(groups.size()).isGreaterThan(0);

        JsonNode firstGroup = groups.get(0);
        assertThat(firstGroup.has("name")).isTrue();
        assertThat(firstGroup.has("order")).isTrue();
        assertThat(firstGroup.has("description")).isTrue();
    }

    @Test
    void testValueDependantsStructure() throws Exception {
        // Using ConfigDefinition ensures matchers are resolved just like in real connectors
        ConnectorMetadata connectorMetadata = createMockConnectorWithDependants();

        DebeziumDescriptorSchema schema = new DebeziumDescriptorSchema();
        String descriptorJson = schema.getSpec(connectorMetadata);

        JsonNode descriptorNode = objectMapper.readTree(descriptorJson);
        JsonNode properties = descriptorNode.get("properties");

        JsonNode propertyWithDependants = null;
        for (JsonNode property : properties) {
            if (property.get("name").asText().equals("connection.adapter")) {
                propertyWithDependants = property;
                break;
            }
        }

        assertThat(propertyWithDependants).isNotNull();
        assertThat(propertyWithDependants.has("valueDependants")).isTrue();

        JsonNode valueDependants = propertyWithDependants.get("valueDependants");
        assertThat(valueDependants.isArray()).isTrue();
        assertThat(valueDependants.size()).isGreaterThan(0);

        JsonNode firstDependant = valueDependants.get(0);
        assertThat(firstDependant.has("values")).isTrue();
        assertThat(firstDependant.has("dependants")).isTrue();
        assertThat(firstDependant.get("values").isArray()).isTrue();
        assertThat(firstDependant.get("dependants").isArray()).isTrue();

        assertThat(firstDependant.get("values").get(0).asText()).isEqualTo("LogMiner");
        assertThat(firstDependant.get("dependants").get(0).asText()).isEqualTo("log.mining.buffer.type");
    }

    @Test
    void testInternalPropertiesFiltered() throws Exception {
        ConnectorMetadata connectorMetadata = createMockConnectorWithInternalProperties();

        DebeziumDescriptorSchema schema = new DebeziumDescriptorSchema();
        String descriptorJson = schema.getSpec(connectorMetadata);

        JsonNode descriptorNode = objectMapper.readTree(descriptorJson);
        JsonNode properties = descriptorNode.get("properties");

        for (JsonNode property : properties) {
            String name = property.get("name").asText();
            assertThat(name).doesNotStartWith("internal.");
        }
    }

    @Test
    void testValidationExtraction() throws Exception {
        ConnectorMetadata connectorMetadata = createMockConnectorWithValidations();

        DebeziumDescriptorSchema schema = new DebeziumDescriptorSchema();
        String descriptorJson = schema.getSpec(connectorMetadata);

        JsonNode descriptorNode = objectMapper.readTree(descriptorJson);
        JsonNode properties = descriptorNode.get("properties");

        JsonNode enumProperty = null;
        JsonNode rangeProperty = null;
        JsonNode minProperty = null;

        for (JsonNode property : properties) {
            String name = property.get("name").asText();
            if (name.equals("snapshot.mode")) {
                enumProperty = property;
            }
            else if (name.equals("max.batch.size")) {
                rangeProperty = property;
            }
            else if (name.equals("poll.interval.ms")) {
                minProperty = property;
            }
        }

        assertThat(enumProperty).isNotNull();
        assertThat(enumProperty.has("validation")).isTrue();
        JsonNode enumValidations = enumProperty.get("validation");
        assertThat(enumValidations.isArray()).isTrue();
        assertThat(enumValidations.size()).isGreaterThan(0);
        JsonNode enumValidation = enumValidations.get(0);
        assertThat(enumValidation.get("type").asText()).isEqualTo("enum");
        assertThat(enumValidation.has("values")).isTrue();
        assertThat(enumValidation.get("values").isArray()).isTrue();

        assertThat(rangeProperty).isNotNull();
        assertThat(rangeProperty.has("validation")).isTrue();
        JsonNode rangeValidations = rangeProperty.get("validation");
        assertThat(rangeValidations.size()).isGreaterThan(0);
        JsonNode rangeValidation = rangeValidations.get(0);
        assertThat(rangeValidation.get("type").asText()).isEqualTo("range");
        assertThat(rangeValidation.has("min")).isTrue();
        assertThat(rangeValidation.has("max")).isTrue();

        assertThat(minProperty).isNotNull();
        assertThat(minProperty.has("validation")).isTrue();
        JsonNode minValidations = minProperty.get("validation");
        assertThat(minValidations.size()).isGreaterThan(0);
        JsonNode minValidation = minValidations.get(0);
        assertThat(minValidation.get("type").asText()).isEqualTo("min");
        assertThat(minValidation.has("min")).isTrue();
        assertThat(minValidation.get("min").asInt()).isEqualTo(0);
    }

    private ConnectorMetadata createMockConnectorMetadata() {
        return new ConnectorMetadata() {
            @Override
            public ConnectorDescriptor getConnectorDescriptor() {
                return new ConnectorDescriptor("io.debezium.connector.postgresql.PostgresConnector", "1.0.0-SNAPSHOT");
            }

            @Override
            public Field.Set getConnectorFields() {
                Field topicPrefix = Field.create("topic.prefix")
                        .withDisplayName("Topic prefix")
                        .withType(ConfigDef.Type.STRING)
                        .withWidth(ConfigDef.Width.MEDIUM)
                        .withImportance(ConfigDef.Importance.HIGH)
                        .withDescription("Topic prefix for the connector")
                        .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 0))
                        .required();

                Field databaseHost = Field.create("database.hostname")
                        .withDisplayName("Database hostname")
                        .withType(ConfigDef.Type.STRING)
                        .withWidth(ConfigDef.Width.LONG)
                        .withImportance(ConfigDef.Importance.HIGH)
                        .withDescription("Database hostname to connect to")
                        .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 1));

                return Field.setOf(topicPrefix, databaseHost);
            }
        };
    }

    private ConnectorMetadata createMockConnectorWithDependants() {
        return new ConnectorMetadata() {
            @Override
            public ConnectorDescriptor getConnectorDescriptor() {
                return new ConnectorDescriptor("io.debezium.connector.postgresql.PostgresConnector", "1.0.0-SNAPSHOT");
            }

            @Override
            public Field.Set getConnectorFields() {
                Field adapterField = Field.create("connection.adapter")
                        .withDisplayName("Connection Adapter")
                        .withType(ConfigDef.Type.STRING)
                        .withWidth(ConfigDef.Width.MEDIUM)
                        .withImportance(ConfigDef.Importance.HIGH)
                        .withDescription("The adapter to use")
                        .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 0))
                        .withDependents("LogMiner", DependentFieldMatcher.exact("log.mining.buffer.type"));

                Field bufferType = Field.create("log.mining.buffer.type")
                        .withDisplayName("Log Mining Buffer Type")
                        .withType(ConfigDef.Type.STRING)
                        .withWidth(ConfigDef.Width.MEDIUM)
                        .withImportance(ConfigDef.Importance.LOW)
                        .withDescription("The buffer type")
                        .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 0));

                // Create ConfigDefinition to resolve matchers (just like real connectors do)
                ConfigDefinition config = ConfigDefinition.editor()
                        .name("Test")
                        .connector(adapterField, bufferType)
                        .create();

                return Field.setOf(config.all());
            }
        };
    }

    private ConnectorMetadata createMockConnectorWithInternalProperties() {
        return new ConnectorMetadata() {
            @Override
            public ConnectorDescriptor getConnectorDescriptor() {
                return new ConnectorDescriptor("io.debezium.connector.postgresql.PostgresConnector", "1.0.0-SNAPSHOT");
            }

            @Override
            public Field.Set getConnectorFields() {
                Field normalField = Field.create("normal.property")
                        .withDisplayName("Normal Property")
                        .withType(ConfigDef.Type.STRING)
                        .withWidth(ConfigDef.Width.MEDIUM)
                        .withImportance(ConfigDef.Importance.HIGH)
                        .withDescription("A normal property")
                        .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 0));

                Field internalField = Field.create("internal.secret")
                        .withDisplayName("Internal Secret")
                        .withType(ConfigDef.Type.STRING)
                        .withWidth(ConfigDef.Width.MEDIUM)
                        .withImportance(ConfigDef.Importance.HIGH)
                        .withDescription("Internal property")
                        .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 1));

                return Field.setOf(normalField, internalField);
            }
        };
    }

    private ConnectorMetadata createMockConnectorWithValidations() {
        return new ConnectorMetadata() {
            @Override
            public ConnectorDescriptor getConnectorDescriptor() {
                return new ConnectorDescriptor("io.debezium.connector.postgresql.PostgresConnector", "1.0.0-SNAPSHOT");
            }

            @Override
            public Field.Set getConnectorFields() {
                java.util.Set<String> snapshotModes = new java.util.LinkedHashSet<>();
                snapshotModes.add("always");
                snapshotModes.add("initial");
                snapshotModes.add("never");

                Field enumField = Field.create("snapshot.mode")
                        .withDisplayName("Snapshot mode")
                        .withType(ConfigDef.Type.STRING)
                        .withWidth(ConfigDef.Width.SHORT)
                        .withImportance(ConfigDef.Importance.HIGH)
                        .withDescription("Snapshot mode")
                        .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 0))
                        .withAllowedValues(snapshotModes);

                Field rangeField = Field.create("max.batch.size")
                        .withDisplayName("Max batch size")
                        .withType(ConfigDef.Type.INT)
                        .withWidth(ConfigDef.Width.SHORT)
                        .withImportance(ConfigDef.Importance.MEDIUM)
                        .withDescription("Maximum batch size")
                        .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 0))
                        .withValidation(Field.RangeValidator.between(1, 10000));

                Field minField = Field.create("poll.interval.ms")
                        .withDisplayName("Poll interval")
                        .withType(ConfigDef.Type.LONG)
                        .withWidth(ConfigDef.Width.SHORT)
                        .withImportance(ConfigDef.Importance.LOW)
                        .withDescription("Poll interval in milliseconds")
                        .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 1))
                        .withValidation(Field.RangeValidator.atLeast(0));

                return Field.setOf(enumField, rangeField, minField);
            }
        };
    }
}
