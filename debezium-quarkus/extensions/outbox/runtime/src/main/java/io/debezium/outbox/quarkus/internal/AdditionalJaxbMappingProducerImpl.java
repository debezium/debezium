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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;

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

/**
 * An {@link AdditionalJaxbMappingProducer} implementation that provides Hibernate ORM
 * with a HBM XML mapping for an map-mode entity configuration for the OutboxEvent
 * entity data type.
 *
 * @author Chris Cranford
 */
public class AdditionalJaxbMappingProducerImpl implements AdditionalJaxbMappingProducer {
    @Override
    public Collection<MappingDocument> produceAdditionalMappings(MetadataImplementor metadata,
                                                                 IndexView jandexIndex,
                                                                 MappingBinder mappingBinder,
                                                                 MetadataBuildingContext buildingContext) {
        final Origin origin = new Origin(SourceType.FILE, OUTBOX_ENTITY_HBMXML);
        try {
            final InputStream stream = getClass().getResourceAsStream("/" + OUTBOX_ENTITY_HBMXML);
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
                        return Collections.singletonList(new MappingDocument(mapping, origin, buildingContext));
                    }
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to submit OutboxEvent.hbm.xml mapping to Hibernate ORM", e);
        }
    }
}
