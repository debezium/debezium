/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.deployment;

import static io.debezium.outbox.quarkus.internal.OutboxConstants.OUTBOX_ENTITY_FULLNAME;

import java.util.UUID;

import org.hibernate.boot.jaxb.hbm.spi.JaxbHbmBasicAttributeType;
import org.hibernate.boot.jaxb.hbm.spi.JaxbHbmColumnType;
import org.hibernate.boot.jaxb.hbm.spi.JaxbHbmGeneratorSpecificationType;
import org.hibernate.boot.jaxb.hbm.spi.JaxbHbmHibernateMapping;
import org.hibernate.boot.jaxb.hbm.spi.JaxbHbmIdentifierGeneratorDefinitionType;
import org.hibernate.boot.jaxb.hbm.spi.JaxbHbmRootEntityType;
import org.hibernate.boot.jaxb.hbm.spi.JaxbHbmSimpleIdType;

import io.debezium.outbox.quarkus.internal.JsonNodeAttributeConverter;

/**
 * Helper class that can produce a JAXB HBM mapping for the OutboxEvent entity.
 *
 * @author Chris Cranford
 */
public class OutboxEventHbmWriter {

    private static final String JACKSON_JSONNODE = "com.fasterxml.jackson.databind.JsonNode";

    static JaxbHbmHibernateMapping write(DebeziumOutboxConfig config, OutboxEventEntityBuildItem outboxEventEntityBuildItem) {
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

        return mapping;
    }

    private static JaxbHbmSimpleIdType createIdAttribute(DebeziumOutboxConfig config) {
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

    private static JaxbHbmBasicAttributeType createAggregateTypeAttribute(DebeziumOutboxConfig config) {
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

    private static JaxbHbmBasicAttributeType createAggregateIdAttribute(DebeziumOutboxConfig config,
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

    private static JaxbHbmBasicAttributeType createTypeAttribute(DebeziumOutboxConfig config) {
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

    private static JaxbHbmBasicAttributeType createTimestampAttribute(DebeziumOutboxConfig config) {
        final JaxbHbmBasicAttributeType attribute = new JaxbHbmBasicAttributeType();
        attribute.setName("timestamp");
        attribute.setNotNull(true);
        if (config.timestamp.converter.isPresent()) {
            attribute.setTypeAttribute("converted::" + config.timestamp.converter.get());
        }
        else {
            attribute.setTypeAttribute("Instant");
        }

        final JaxbHbmColumnType column = new JaxbHbmColumnType();
        column.setName(config.timestamp.name);
        config.timestamp.columnDefinition.ifPresent(column::setSqlType);
        attribute.getColumnOrFormula().add(column);

        return attribute;
    }

    private static JaxbHbmBasicAttributeType createPayloadAttribute(DebeziumOutboxConfig config,
                                                                    OutboxEventEntityBuildItem outboxEventEntityBuildItem) {

        final boolean isJacksonJsonNode = isPayloadJacksonJsonNode(outboxEventEntityBuildItem);

        final JaxbHbmBasicAttributeType attribute = new JaxbHbmBasicAttributeType();
        attribute.setName("payload");
        attribute.setNotNull(false);

        if (config.payload.converter.isPresent()) {
            attribute.setTypeAttribute("converted::" + config.payload.converter.get());
        }
        else if (isJacksonJsonNode) {
            attribute.setTypeAttribute("converted::" + JsonNodeAttributeConverter.class.getName());
        }
        else {
            attribute.setTypeAttribute(outboxEventEntityBuildItem.getPayloadType().name().toString());
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

    private static boolean isPayloadJacksonJsonNode(OutboxEventEntityBuildItem outboxEventEntityBuildItem) {
        return outboxEventEntityBuildItem.getPayloadType().name().toString().equals(JACKSON_JSONNODE);
    }
}
