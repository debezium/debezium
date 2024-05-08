/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.deployment;

import static io.debezium.outbox.quarkus.internal.OutboxConstants.OUTBOX_ENTITY_FULLNAME;

import java.time.Instant;
import java.util.UUID;

import org.hibernate.boot.jaxb.hbm.spi.JaxbHbmBasicAttributeType;
import org.hibernate.boot.jaxb.hbm.spi.JaxbHbmColumnType;
import org.hibernate.boot.jaxb.hbm.spi.JaxbHbmGeneratorSpecificationType;
import org.hibernate.boot.jaxb.hbm.spi.JaxbHbmHibernateMapping;
import org.hibernate.boot.jaxb.hbm.spi.JaxbHbmIdentifierGeneratorDefinitionType;
import org.hibernate.boot.jaxb.hbm.spi.JaxbHbmRootEntityType;
import org.hibernate.boot.jaxb.hbm.spi.JaxbHbmSimpleIdType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.outbox.quarkus.internal.JsonNodeAttributeConverter;
import io.debezium.outbox.quarkus.internal.OutboxConstants;

/**
 * Helper class that can produce a JAXB HBM mapping for the OutboxEvent entity.
 *
 * @author Chris Cranford
 */
public class OutboxEventHbmWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(OutboxEventHbmWriter.class);
    private static final String JACKSON_JSONNODE = "com.fasterxml.jackson.databind.JsonNode";

    static JaxbHbmHibernateMapping write(DebeziumOutboxCommonConfig config, OutboxEventEntityBuildItem outboxEventEntityBuildItem) {
        final JaxbHbmHibernateMapping mapping = new JaxbHbmHibernateMapping();

        final JaxbHbmRootEntityType entityType = new JaxbHbmRootEntityType();
        entityType.setEntityName(OUTBOX_ENTITY_FULLNAME);
        entityType.setTable(config.tableName);
        mapping.getClazz().add(entityType);

        // Setup generator
        final JaxbHbmIdentifierGeneratorDefinitionType generatorType = new JaxbHbmIdentifierGeneratorDefinitionType();
        generatorType.setName("uuid2");
        generatorType.setClazz("uuid2");
        mapping.getIdentifierGenerator().add(generatorType);

        // Setup attributes
        entityType.setId(createIdAttribute(config));
        entityType.getAttributes().add(createAggregateTypeAttribute(config));
        entityType.getAttributes().add(createAggregateIdAttribute(config, outboxEventEntityBuildItem));
        entityType.getAttributes().add(createTypeAttribute(config));
        entityType.getAttributes().add(createTimestampAttribute(config));
        entityType.getAttributes().add(createPayloadAttribute(config, outboxEventEntityBuildItem));
        if (config.tracingEnabled) {
            entityType.getAttributes().add(createTracingSpanAttribute(config));
        }

        // Additional fields
        if (config.additionalFields.isPresent()) {
            String[] fields = config.additionalFields.get().split(",");
            for (int fieldIndex = 0; fieldIndex < fields.length; ++fieldIndex) {
                String[] parts = fields[fieldIndex].split(":");
                if (parts.length < 2) {
                    throw new DebeziumException("Expected a column and data type for additional field #" + fieldIndex);
                }
                String fieldName = parts[0];
                String fieldDataType = parts[1];

                String sqlType = null;
                if (parts.length >= 3) {
                    sqlType = parts[2];
                }

                String fieldConverter = null;
                if (parts.length == 4) {
                    fieldConverter = parts[3];
                }

                LOGGER.info("Binding additional field '{}' as '{}' with {} converter.",
                        fieldName, fieldDataType, fieldConverter == null ? "no" : "a");

                entityType.getAttributes().add(createAdditionalField(fieldName, fieldDataType, sqlType, fieldConverter));
            }
        }

        return mapping;
    }

    private static JaxbHbmSimpleIdType createIdAttribute(DebeziumOutboxCommonConfig config) {
        final JaxbHbmSimpleIdType attribute = new JaxbHbmSimpleIdType();
        attribute.setName("id");
        attribute.setTypeAttribute(UUID.class.getName());

        final JaxbHbmColumnType column = new JaxbHbmColumnType();
        column.setName(config.id.name);
        config.id.columnDefinition.ifPresent(column::setSqlType);
        attribute.getColumn().add(column);

        final JaxbHbmGeneratorSpecificationType generator = new JaxbHbmGeneratorSpecificationType();
        generator.setClazz("uuid2");
        attribute.setGenerator(generator);

        return attribute;
    }

    private static JaxbHbmBasicAttributeType createAggregateTypeAttribute(DebeziumOutboxCommonConfig config) {
        final JaxbHbmBasicAttributeType attribute = new JaxbHbmBasicAttributeType();
        attribute.setName("aggregateType");
        attribute.setNotNull(true);
        if (config.aggregateType.converter.isPresent()) {
            attribute.setTypeAttribute("converted::" + config.aggregateType.converter.get());
        }
        else {
            attribute.setTypeAttribute("string");
        }

        final JaxbHbmColumnType column = new JaxbHbmColumnType();
        column.setName(config.aggregateType.name);
        config.aggregateType.columnDefinition.ifPresent(column::setSqlType);
        attribute.getColumnOrFormula().add(column);

        return attribute;
    }

    private static JaxbHbmBasicAttributeType createAggregateIdAttribute(DebeziumOutboxCommonConfig config,
                                                                        OutboxEventEntityBuildItem outboxEventEntityBuildItem) {
        final JaxbHbmBasicAttributeType attribute = new JaxbHbmBasicAttributeType();
        attribute.setName("aggregateId");
        attribute.setNotNull(true);
        if (config.aggregateId.converter.isPresent()) {
            attribute.setTypeAttribute("converted::" + config.aggregateId.converter.get());
        }
        else {
            attribute.setTypeAttribute(outboxEventEntityBuildItem.getAggregateIdType().name().toString());
        }

        final JaxbHbmColumnType column = new JaxbHbmColumnType();
        column.setName(config.aggregateId.name);
        config.aggregateId.columnDefinition.ifPresent(column::setSqlType);
        attribute.getColumnOrFormula().add(column);

        return attribute;
    }

    private static JaxbHbmBasicAttributeType createTypeAttribute(DebeziumOutboxCommonConfig config) {
        final JaxbHbmBasicAttributeType attribute = new JaxbHbmBasicAttributeType();
        attribute.setName("type");
        attribute.setNotNull(true);
        if (config.type.converter.isPresent()) {
            attribute.setTypeAttribute("converted::" + config.type.converter.get());
        }
        else {
            attribute.setTypeAttribute("string");
        }

        final JaxbHbmColumnType column = new JaxbHbmColumnType();
        column.setName(config.type.name);
        config.type.columnDefinition.ifPresent(column::setSqlType);
        attribute.getColumnOrFormula().add(column);

        return attribute;
    }

    private static JaxbHbmBasicAttributeType createTimestampAttribute(DebeziumOutboxCommonConfig config) {
        final JaxbHbmBasicAttributeType attribute = new JaxbHbmBasicAttributeType();
        attribute.setName("timestamp");
        attribute.setNotNull(true);
        if (config.timestamp.converter.isPresent()) {
            attribute.setTypeAttribute("converted::" + config.timestamp.converter.get());
        }
        else {
            // Hibernate 6.x expects full qualified class names for this whereas Hibernate 5.x had
            // a registered type for "Instant" that did the direct mapping without package names.
            attribute.setTypeAttribute(Instant.class.getName());
        }

        final JaxbHbmColumnType column = new JaxbHbmColumnType();
        column.setName(config.timestamp.name);
        config.timestamp.columnDefinition.ifPresent(column::setSqlType);
        attribute.getColumnOrFormula().add(column);

        return attribute;
    }

    private static JaxbHbmBasicAttributeType createPayloadAttribute(DebeziumOutboxCommonConfig config,
                                                                    OutboxEventEntityBuildItem outboxEventEntityBuildItem) {

        final boolean isJacksonJsonNode = isPayloadJacksonJsonNode(outboxEventEntityBuildItem);

        final JaxbHbmBasicAttributeType attribute = new JaxbHbmBasicAttributeType();
        attribute.setName("payload");
        attribute.setNotNull(false);

        if (config.payload.type.isPresent()) {
            LOGGER.info("Using payload type: {}", config.payload.type.get());
            attribute.setTypeAttribute(config.payload.type.get());
        }
        else if (config.payload.converter.isPresent()) {
            LOGGER.info("Using payload attribute converter: {}", config.payload.converter.get());
            attribute.setTypeAttribute("converted::" + config.payload.converter.get());
        }
        else if (isJacksonJsonNode) {
            LOGGER.info("Using payload attribute converter: {}", JsonNodeAttributeConverter.class.getName());
            attribute.setTypeAttribute("converted::" + JsonNodeAttributeConverter.class.getName());
        }
        else {
            String resolvedTypeName = outboxEventEntityBuildItem.getPayloadType().name().toString();
            LOGGER.info("Using payload resolved type: {}", resolvedTypeName);
            attribute.setTypeAttribute(resolvedTypeName);
        }

        final JaxbHbmColumnType column = new JaxbHbmColumnType();
        column.setName(config.payload.name);

        if (config.payload.columnDefinition.isPresent()) {
            column.setSqlType(config.payload.columnDefinition.get());
        }
        else if (isJacksonJsonNode) {
            column.setSqlType("varchar(8000)");
        }

        attribute.getColumnOrFormula().add(column);

        return attribute;
    }

    private static JaxbHbmBasicAttributeType createTracingSpanAttribute(DebeziumOutboxCommonConfig config) {
        final JaxbHbmBasicAttributeType attribute = new JaxbHbmBasicAttributeType();
        attribute.setName(OutboxConstants.TRACING_SPAN_CONTEXT);
        attribute.setNotNull(false);
        attribute.setTypeAttribute("string");

        final JaxbHbmColumnType column = new JaxbHbmColumnType();
        column.setName(config.tracingSpan.name);
        column.setLength(256);

        config.tracingSpan.columnDefinition.ifPresent(column::setSqlType);

        attribute.getColumnOrFormula().add(column);

        return attribute;
    }

    private static JaxbHbmBasicAttributeType createAdditionalField(String name, String dataType, String sqlType, String converter) {
        final JaxbHbmBasicAttributeType attribute = new JaxbHbmBasicAttributeType();
        attribute.setName(name);

        if (converter != null) {
            attribute.setTypeAttribute("converted::" + converter);
        }
        else {
            attribute.setTypeAttribute(dataType);
        }

        final JaxbHbmColumnType column = new JaxbHbmColumnType();
        column.setName(name);
        if (sqlType != null) {
            column.setSqlType(sqlType);
        }

        attribute.getColumnOrFormula().add(column);
        return attribute;
    }

    private static boolean isPayloadJacksonJsonNode(OutboxEventEntityBuildItem outboxEventEntityBuildItem) {
        return outboxEventEntityBuildItem.getPayloadType().name().toString().equals(JACKSON_JSONNODE);
    }
}
