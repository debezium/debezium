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

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.hibernate.boot.jaxb.hbm.spi.JaxbHbmHibernateMapping;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.ParameterizedType;
import org.jboss.jandex.Type;
import org.jboss.logging.Logger;

import com.fasterxml.jackson.databind.JsonNode;

import io.debezium.outbox.quarkus.ExportedEvent;
import io.debezium.outbox.quarkus.internal.DebeziumTracerEventDispatcher;
import io.debezium.outbox.quarkus.internal.DefaultEventDispatcher;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.Capabilities;
import io.quarkus.deployment.Capability;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.GeneratedResourceBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.hibernate.orm.deployment.PersistenceUnitDescriptorBuildItem;
import io.quarkus.hibernate.orm.deployment.integration.HibernateOrmIntegrationStaticConfiguredBuildItem;

/**
 * Quarkus deployment processor for the Debezium "outbox" extension.
 *
 * @author Chris Cranford
 */
public final class OutboxProcessor {

    private static final Logger LOGGER = Logger.getLogger(OutboxProcessor.class);

    private static final String DEBEZIUM_OUTBOX = "debezium-outbox";

    /**
     * Debezium Outbox configuration
     */
    DebeziumOutboxConfig debeziumOutboxConfig;

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
            LOGGER.infof("Found ExportedEvent type: %s", classInfo.name());
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
                        LOGGER.debugf("     AggregateId: %s", pTypeAggregateType.name().toString());
                        LOGGER.debugf("     Payload: %s", pTypePayloadType.name().toString());

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
                        LOGGER.debugf("     AggregateId: %s", aggregateIdType.name().toString());
                        LOGGER.debugf("     Payload: %s", payloadType.name().toString());
                    }
                }
            }
        }

        LOGGER.infof("Binding Aggregate Id as '%s'.", aggregateIdType.name().toString());
        LOGGER.infof("Binding Payload as '%s'.", payloadType.name().toString());

        outboxEventEntityProducer.produce(new OutboxEventEntityBuildItem(aggregateIdType, payloadType));

        // We enable outbox events in *all* persistence units.
        for (PersistenceUnitDescriptorBuildItem puDescriptor : persistenceUnitDescriptorBuildItems) {
            integrationConfiguredProducer.produce(
                    new HibernateOrmIntegrationStaticConfiguredBuildItem(DEBEZIUM_OUTBOX,
                            puDescriptor.getPersistenceUnitName())
                            .setXmlMappingRequired(true));
        }
    }

    @BuildStep
    public void build(OutboxEventEntityBuildItem outboxBuildItem,
                      BuildProducer<AdditionalBeanBuildItem> additionalBeanProducer,
                      BuildProducer<GeneratedResourceBuildItem> generatedResourcesProducer,
                      BuildProducer<ReflectiveClassBuildItem> reflectiveClassProducer,
                      Capabilities capabilities) {
        if (debeziumOutboxConfig.tracingEnabled && capabilities.isPresent(Capability.OPENTRACING)) {
            additionalBeanProducer.produce(AdditionalBeanBuildItem.unremovableOf(DebeziumTracerEventDispatcher.class));
        }
        else {
            additionalBeanProducer.produce(AdditionalBeanBuildItem.unremovableOf(DefaultEventDispatcher.class));
        }
        generateHbmMapping(outboxBuildItem, generatedResourcesProducer);
    }

    private void generateHbmMapping(OutboxEventEntityBuildItem outboxBuildItem,
                                    BuildProducer<GeneratedResourceBuildItem> generatedResourcesProducer) {
        try {
            try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                final JaxbHbmHibernateMapping jaxbMapping = OutboxEventHbmWriter.write(debeziumOutboxConfig, outboxBuildItem);

                final JAXBContext context = JAXBContext.newInstance(JaxbHbmHibernateMapping.class);
                final Marshaller marshaller = context.createMarshaller();
                marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

                final PrintWriter writer = new PrintWriter(os);
                marshaller.marshal(jaxbMapping, writer);

                LOGGER.debugf("Outbox entity HBM mapping:\n%s", new String(os.toByteArray()));
                generatedResourcesProducer.produce(new GeneratedResourceBuildItem(OUTBOX_ENTITY_HBMXML, os.toByteArray()));
            }
        }
        catch (JAXBException | IOException e) {
            throw new IllegalStateException("Failed to produce Outbox HBM mapping", e);
        }
    }
}
