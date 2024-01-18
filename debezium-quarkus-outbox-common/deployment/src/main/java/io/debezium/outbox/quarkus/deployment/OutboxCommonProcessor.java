/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.deployment;

import static io.debezium.outbox.quarkus.internal.OutboxConstants.OUTBOX_ENTITY_HBMXML;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Marshaller;

import org.hibernate.boot.jaxb.hbm.spi.JaxbHbmHibernateMapping;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.ParameterizedType;
import org.jboss.jandex.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

import io.debezium.outbox.quarkus.ExportedEvent;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.GeneratedResourceBuildItem;
import io.quarkus.hibernate.orm.deployment.PersistenceUnitDescriptorBuildItem;
import io.quarkus.hibernate.orm.deployment.integration.HibernateOrmIntegrationStaticConfiguredBuildItem;

/**
 * Quarkus common deployment processor for the Debezium "outbox" extension.
 *
 * @author Chris Cranford
 */
public abstract class OutboxCommonProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(OutboxCommonProcessor.class);

    protected static final String DEBEZIUM_OUTBOX = "debezium-outbox";

    protected abstract DebeziumOutboxCommonConfig getConfig();

    @BuildStep
    public FeatureBuildItem feature() {
        return new FeatureBuildItem(DEBEZIUM_OUTBOX);
    }

    @BuildStep
    public void produceOutboxBuildItem(CombinedIndexBuildItem index,
                                       List<PersistenceUnitDescriptorBuildItem> persistenceUnitDescriptorBuildItems,
                                       BuildProducer<OutboxEventEntityBuildItem> outboxEventEntityProducer,
                                       BuildProducer<HibernateOrmIntegrationStaticConfiguredBuildItem> integrationConfiguredProducer) {
        final DotName exportedEvent = DotName.createSimple(ExportedEvent.class.getName());

        Type aggregateIdType = Type.create(DotName.createSimple(String.class.getName()), Type.Kind.CLASS);
        Type payloadType = Type.create(DotName.createSimple(JsonNode.class.getName()), Type.Kind.CLASS);

        boolean parameterizedTypesDetected = false;
        for (ClassInfo classInfo : index.getIndex().getAllKnownImplementors(exportedEvent)) {
            LOGGER.info("Found ExportedEvent type: {}", classInfo.name());
            for (Type interfaceType : classInfo.interfaceTypes()) {
                if (interfaceType.name().equals(exportedEvent)) {
                    if (interfaceType.kind().equals(Type.Kind.PARAMETERIZED_TYPE)) {
                        final ParameterizedType pType = interfaceType.asParameterizedType();
                        if (pType.arguments().size() != 2) {
                            throw new IllegalArgumentException(
                                    String.format(
                                            "Expected 2 parameterized types for class %s using interface ExportedEvent",
                                            classInfo.name()));
                        }

                        final Type pTypeAggregateType = pType.arguments().get(0);
                        final Type pTypePayloadType = pType.arguments().get(1);
                        LOGGER.debug(" * Implements ExportedEvent with generic parameters:");
                        LOGGER.debug("     AggregateId: {}", pTypeAggregateType.name().toString());
                        LOGGER.debug("     Payload: {}", pTypePayloadType.name().toString());

                        if (parameterizedTypesDetected) {
                            if (!pTypeAggregateType.equals(aggregateIdType)) {
                                throw new IllegalStateException(
                                        String.format(
                                                "Class %s implements ExportedEvent and expected aggregate-id parameter type " +
                                                        "to be %s but was %s. All ExportedEvent implementors must use the same parameter types.",
                                                classInfo.name(),
                                                aggregateIdType.name(),
                                                pTypeAggregateType.name()));
                            }
                            if (!pTypePayloadType.equals(payloadType)) {
                                throw new IllegalStateException(
                                        String.format(
                                                "Class %s implements ExportedEvent and expected payload parameter type to be " +
                                                        "%s but was %s. All ExportedEvent implementors must use the same parameter types.",
                                                classInfo.name(),
                                                payloadType.name(),
                                                pTypePayloadType.name()));
                            }
                        }
                        else {
                            aggregateIdType = pTypeAggregateType;
                            payloadType = pTypePayloadType;
                            parameterizedTypesDetected = true;
                        }
                    }
                    else {
                        LOGGER.debug(" * Implements ExportedEvent without parameters, using:");
                        LOGGER.debug("     AggregateId: {}", aggregateIdType.name().toString());
                        LOGGER.debug("     Payload: {}", payloadType.name().toString());
                    }
                }
            }
        }

        LOGGER.info("Binding Aggregate Id as '{}'.", aggregateIdType.name().toString());
        LOGGER.info("Binding Payload as '{}'.", payloadType.name().toString());

        outboxEventEntityProducer.produce(new OutboxEventEntityBuildItem(aggregateIdType, payloadType));

        // We enable outbox events in *all* persistence units.
        for (PersistenceUnitDescriptorBuildItem puDescriptor : persistenceUnitDescriptorBuildItems) {
            integrationConfiguredProducer.produce(
                    new HibernateOrmIntegrationStaticConfiguredBuildItem(DEBEZIUM_OUTBOX,
                            puDescriptor.getPersistenceUnitName())
                            .setXmlMappingRequired(true));
        }
    }

    protected void generateHbmMapping(OutboxEventEntityBuildItem outboxBuildItem,
                                      BuildProducer<GeneratedResourceBuildItem> generatedResourcesProducer) {
        try {
            try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                final JaxbHbmHibernateMapping jaxbMapping = OutboxEventHbmWriter.write(getConfig(), outboxBuildItem);

                final JAXBContext context = JAXBContext.newInstance(JaxbHbmHibernateMapping.class);
                final Marshaller marshaller = context.createMarshaller();
                marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

                final PrintWriter writer = new PrintWriter(os);
                marshaller.marshal(jaxbMapping, writer);

                LOGGER.debug("Outbox entity HBM mapping:\n{}", new String(os.toByteArray()));
                generatedResourcesProducer.produce(new GeneratedResourceBuildItem(OUTBOX_ENTITY_HBMXML, os.toByteArray()));
            }
        }
        catch (JAXBException | IOException e) {
            throw new IllegalStateException("Failed to produce Outbox HBM mapping", e);
        }
    }
}
