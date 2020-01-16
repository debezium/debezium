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

        // Setup the ID
        final JaxbHbmSimpleIdType idType = new JaxbHbmSimpleIdType();
        idType.setName("id");
        idType.setColumnAttribute(config.idColumnName);
        idType.setTypeAttribute(UUID.class.getName());

        final JaxbHbmGeneratorSpecificationType generatorSpecType = new JaxbHbmGeneratorSpecificationType();
        generatorSpecType.setClazz("uuid2");
        idType.setGenerator(generatorSpecType);

        entityType.setId(idType);

        // Setup the aggregateType
        final JaxbHbmBasicAttributeType aggregateType = new JaxbHbmBasicAttributeType();
        aggregateType.setName("aggregateType");
        aggregateType.setColumnAttribute(config.aggregateTypeColumnName);
        aggregateType.setTypeAttribute("string");
        aggregateType.setNotNull(true);
        entityType.getAttributes().add(aggregateType);

        // Setup the aggregateIdType
        final JaxbHbmBasicAttributeType aggregateIdType = new JaxbHbmBasicAttributeType();
        aggregateIdType.setName("aggregateId");
        aggregateIdType.setColumnAttribute(config.aggregateIdColumnName);
        aggregateIdType.setTypeAttribute(outboxEventEntityBuildItem.getAggregateIdType().name().toString());
        aggregateIdType.setNotNull(true);
        entityType.getAttributes().add(aggregateIdType);

        // Setup the typeType
        final JaxbHbmBasicAttributeType typeType = new JaxbHbmBasicAttributeType();
        typeType.setName("type");
        typeType.setColumnAttribute(config.typeColumnName);
        typeType.setTypeAttribute("string");
        typeType.setNotNull(true);
        entityType.getAttributes().add(typeType);

        // Setup the timestampType
        final JaxbHbmBasicAttributeType timestampType = new JaxbHbmBasicAttributeType();
        timestampType.setName("timestamp");
        timestampType.setColumnAttribute(config.timestampColumnName);
        timestampType.setTypeAttribute("Instant");
        timestampType.setNotNull(true);
        entityType.getAttributes().add(timestampType);

        // Setup the payloadType
        final JaxbHbmBasicAttributeType payloadType = new JaxbHbmBasicAttributeType();
        payloadType.setName("payload");

        // todo: this needs some more testing with varied data types
        final String payloadClassType = outboxEventEntityBuildItem.getPayloadType().name().toString();
        if (payloadClassType.equals("com.fasterxml.jackson.databind.JsonNode")) {
            payloadType.setTypeAttribute("converted::" + JsonNodeAttributeConverter.class.getName());

            final JaxbHbmColumnType columnType = new JaxbHbmColumnType();
            columnType.setName(config.payloadColumnName);
            columnType.setSqlType("varchar(8000)");
            payloadType.getColumnOrFormula().add(columnType);
        }
        else {
            payloadType.setColumnAttribute(config.payloadColumnName);
            payloadType.setTypeAttribute(outboxEventEntityBuildItem.getPayloadType().name().toString());
        }
        entityType.getAttributes().add(payloadType);

        return mapping;
    }
}
