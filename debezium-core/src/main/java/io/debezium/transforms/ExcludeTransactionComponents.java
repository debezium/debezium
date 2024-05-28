/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static io.debezium.config.CommonConnectorConfig.SCHEMA_NAME_ADJUSTMENT_MODE;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import io.debezium.Module;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.pipeline.txmetadata.TransactionStructMaker;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;

public class ExcludeTransactionComponents<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    public static final String COMPONENTS_CONF = "components";

    private static final String TRANSACTION_TOPIC_COMPONENT = "transaction_topic";
    private EnumSet<TransactionMetadataComponent> excludedTransactionMetadataComponents;
    private SchemaNameAdjuster schemaNameAdjuster;

    public enum TransactionMetadataComponent {
        ID("id"),
        DATA_COLLECTION_ORDER("data_collection_order"),
        TOTAL_ORDER("total_order"),
        TRANSACTION_TOPIC("transaction_topic");

        private final String value;

        TransactionMetadataComponent(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    @Override
    public R apply(R record) {
        if (isTransactionTopicMessage(record) && shouldExcludeTransactionTopic()) {
            return null;
        }
        if (shouldExcludeTransactionFields()) {
            Struct value = (Struct) record.value();
            Schema schema = record.valueSchema();
            Schema updatedSchema = updateSchema("", schema);
            Struct newValue = updateStruct("", updatedSchema, value);
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                    updatedSchema, newValue, record.timestamp());
        }
        return record;
    }

    private boolean isTransactionTopicMessage(R record) {
        if (record.keySchema().equals(SchemaFactory.get().transactionKeySchema(schemaNameAdjuster)) &&
                record.valueSchema().equals(SchemaFactory.get().transactionValueSchema(schemaNameAdjuster))) {
            return true;
        }
        return false;
    }

    private boolean shouldExcludeTransactionTopic() {
        return excludedTransactionMetadataComponents.contains(TransactionMetadataComponent.TRANSACTION_TOPIC);
    }

    private boolean shouldExcludeTransactionFields() {
        return excludedTransactionMetadataComponents.contains(TransactionMetadataComponent.ID) ||
                excludedTransactionMetadataComponents.contains(TransactionMetadataComponent.TOTAL_ORDER) ||
                excludedTransactionMetadataComponents.contains(TransactionMetadataComponent.DATA_COLLECTION_ORDER);
    }

    private Schema updateSchema(String fullName, Schema schema) {
        // Create a schema builder
        SchemaBuilder schemaBuilder = SchemaBuilder.struct().version(schema.version()).name(schema.name());
        if (schema.isOptional()) {
            schemaBuilder = schemaBuilder.optional();
        }

        // Iterate over fields in the original schema and add to the schema builder dynamically
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            String currentFullName = !fullName.isEmpty() ? fullName + "." + field.name() : field.name();

            if (field.schema().type() == Schema.Type.STRUCT) {
                // If the field is a nested struct, recursively modify it and add its schema
                Schema updatedNestedSchema = updateSchema(currentFullName, field.schema());
                schemaBuilder.field(field.name(), updatedNestedSchema);
            }
            else {
                if (!shouldExcludeField(currentFullName)) {
                    schemaBuilder.field(field.name(), field.schema());
                }
            }
        }
        return schemaBuilder.build();
    }

    private Struct updateStruct(String fullName, Schema updatedSchema, Struct struct) {
        // Create an updated struct
        Struct updatedStruct = new Struct(updatedSchema);
        for (org.apache.kafka.connect.data.Field field : updatedSchema.fields()) {
            String currentFullName = fullName != "" ? fullName + "." + field.name() : field.name();
            Object fieldValue = struct.get(field.name());
            if (fieldValue instanceof Struct) {
                // If a field is a struct recursively create its nested structs
                Struct nestedStruct = (Struct) fieldValue;
                Struct updatedNestedStruct = updateStruct(currentFullName, field.schema(), nestedStruct);
                updatedStruct.put(field, updatedNestedStruct);
            }
            else {
                if (!shouldExcludeField(currentFullName)) {
                    updatedStruct.put(field, fieldValue);
                }
            }
        }
        return updatedStruct;
    }

    private boolean shouldExcludeField(String fullFieldName) {
        for (TransactionMetadataComponent component : excludedTransactionMetadataComponents) {
            if (fullFieldName.equals(TransactionStructMaker.DEBEZIUM_TRANSACTION_KEY + "." + component.getValue())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        Field.group(config, null, EXCLUDED_TRANSACTION_METADATA_COMPONENTS);
        return config;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> props) {
        final Configuration config = Configuration.from(props);
        SmtManager<R> smtManager = new SmtManager<>(config);
        smtManager.validate(config, Field.setOf(EXCLUDED_TRANSACTION_METADATA_COMPONENTS));

        excludedTransactionMetadataComponents = determineTransactionMetadataComponents(config);
        schemaNameAdjuster = CommonConnectorConfig.SchemaNameAdjustmentMode.parse(config.getString(SCHEMA_NAME_ADJUSTMENT_MODE))
                .createAdjuster();
    }

    private static EnumSet<TransactionMetadataComponent> determineTransactionMetadataComponents(Configuration config) {
        String componentString = config.getString(EXCLUDED_TRANSACTION_METADATA_COMPONENTS);
        return parseTransactionMetadataComponentString(componentString);
    }

    public static EnumSet<TransactionMetadataComponent> parseTransactionMetadataComponentString(String componentString) {
        if (componentString != null) {
            if (componentString.trim().equalsIgnoreCase("none")) {
                return EnumSet.noneOf(TransactionMetadataComponent.class);
            }
            return EnumSet.copyOf(Arrays.stream(componentString.split(","))
                    .map(String::trim)
                    .map(String::toUpperCase)
                    .map(TransactionMetadataComponent::valueOf)
                    .collect(Collectors.toSet()));
        }
        else {
            return EnumSet.noneOf(TransactionMetadataComponent.class);
        }
    }

    public static final Field EXCLUDED_TRANSACTION_METADATA_COMPONENTS = Field.create(COMPONENTS_CONF)
            .withDisplayName("List of transaction metadata components to excluded, defaults to none so includes all components")
            .withType(ConfigDef.Type.LIST)
            .withImportance(ConfigDef.Importance.LOW)
            .withValidation(ExcludeTransactionComponents::validateExcludedTransactionMetadataComponents)
            .withDefault("none")
            .withDescription(
                    "The comma-separated list of transaction metadata components: 'id', 'order', 'transaction_topic'");

    protected static int validateExcludedTransactionMetadataComponents(Configuration config, Field field, Field.ValidationOutput problems) {
        String components = config.getString(field);

        if (components == null || "none".equals(components)) {
            return 0;
        }

        boolean noneSpecified = false;
        boolean operationsSpecified = false;
        for (String component : components.split(",")) {
            switch (component.trim()) {
                case "none":
                    noneSpecified = true;
                    continue;
                case TransactionStructMaker.DEBEZIUM_TRANSACTION_ID_KEY:
                case TransactionStructMaker.DEBEZIUM_TRANSACTION_DATA_COLLECTION_ORDER_KEY:
                case TransactionStructMaker.DEBEZIUM_TRANSACTION_TOTAL_ORDER_KEY:
                case TRANSACTION_TOPIC_COMPONENT:
                    operationsSpecified = true;
                    continue;
                default:
                    problems.accept(field, component, "Invalid component");
                    return 1;
            }
        }

        if (noneSpecified && operationsSpecified) {
            problems.accept(field, "none", "'none' cannot be specified with other skipped operation types");
            return 1;
        }

        return 0;
    }

    @Override
    public String version() {
        return Module.version();
    }

}
