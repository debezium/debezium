/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.internal;

import static io.debezium.outbox.quarkus.internal.OutboxConstants.OUTBOX_ENTITY_HBMXML;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.hibernate.boot.jaxb.Origin;
import org.hibernate.boot.jaxb.SourceType;
import org.hibernate.boot.jaxb.hbm.spi.JaxbHbmHibernateMapping;
import org.hibernate.boot.jaxb.internal.MappingBinder;
import org.hibernate.boot.jaxb.spi.Binding;
import org.hibernate.boot.model.source.internal.hbm.MappingDocument;
import org.hibernate.boot.spi.AdditionalJaxbMappingProducer;
import org.hibernate.boot.spi.MetadataBuildingContext;
import org.hibernate.boot.spi.MetadataImplementor;
import org.jboss.jandex.IndexView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;

/**
 * An {@link AdditionalJaxbMappingProducer} implementation that provides Hibernate ORM
 * with a HBM XML mapping for an map-mode entity configuration for the OutboxEvent
 * entity data type.
 *
 * @author Chris Cranford
 */
public class AdditionalJaxbMappingProducerImpl implements AdditionalJaxbMappingProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(AdditionalJaxbMappingProducerImpl.class);

    @Override
    public Collection<MappingDocument> produceAdditionalMappings(MetadataImplementor metadata,
                                                                 IndexView jandexIndex,
                                                                 MappingBinder mappingBinder,
                                                                 MetadataBuildingContext buildingContext) {
        final Origin origin = new Origin(SourceType.FILE, OUTBOX_ENTITY_HBMXML);

        try (InputStream stream = getOutboxHbmXmlStream()) {
            if (stream == null) {
                LOGGER.error("Failed to locate OutboxEvent.hbm.xml on classpath");
                return Collections.emptyList();
            }

            try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                final Writer writer = new BufferedWriter(new OutputStreamWriter(baos, StandardCharsets.UTF_8));
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        writer.write(line);
                    }
                    writer.flush();
                }

                try (ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray())) {
                    try (BufferedInputStream bis = new BufferedInputStream(bais)) {
                        final Binding<?> jaxbBinding = mappingBinder.bind(bis, origin);
                        final JaxbHbmHibernateMapping mapping = (JaxbHbmHibernateMapping) jaxbBinding.getRoot();

                        logOutboxMapping(mapping);

                        LOGGER.info("Contributed XML mapping for entity: {}", mapping.getClazz().get(0).getEntityName());
                        return Collections.singletonList(new MappingDocument(mapping, origin, buildingContext));
                    }
                }
            }
        }
        catch (IOException e) {
            throw new DebeziumException("Failed to read OutboxEvent.hbm.xml", e);
        }
    }

    private InputStream getOutboxHbmXmlStream() {
        // Attempt to load the XML using the current context class loader, needed for quarkus:dev
        final ClassLoader currentThreadClassLoader = Thread.currentThread().getContextClassLoader();
        final InputStream stream = currentThreadClassLoader.getResourceAsStream("/" + OUTBOX_ENTITY_HBMXML);
        if (stream != null) {
            return stream;
        }

        // Attempt to load the XML using the current class loader
        return getClass().getResourceAsStream("/" + OUTBOX_ENTITY_HBMXML);
    }

    private void logOutboxMapping(JaxbHbmHibernateMapping mapping) {
        try {
            JAXBContext context = JAXBContext.newInstance(JaxbHbmHibernateMapping.class);

            Marshaller marshaller = context.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

            final StringWriter writer = new StringWriter();
            marshaller.marshal(mapping, writer);

            LOGGER.debug("Debezium Outbox XML Mapping:\n{}", writer);
        }
        catch (JAXBException e) {
            throw new RuntimeException("Failed to marshal Debezium Outbox XML mapping", e);
        }
    }
}
