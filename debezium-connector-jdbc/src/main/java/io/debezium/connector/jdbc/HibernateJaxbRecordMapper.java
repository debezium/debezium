/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Locale;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.hibernate.boot.jaxb.hbm.spi.JaxbHbmBasicAttributeType;
import org.hibernate.boot.jaxb.hbm.spi.JaxbHbmGeneratorSpecificationType;
import org.hibernate.boot.jaxb.hbm.spi.JaxbHbmHibernateMapping;
import org.hibernate.boot.jaxb.hbm.spi.JaxbHbmRootEntityType;
import org.hibernate.boot.jaxb.hbm.spi.JaxbHbmSimpleIdType;
import org.hibernate.type.BooleanType;
import org.hibernate.type.ByteType;
import org.hibernate.type.DateType;
import org.hibernate.type.FloatType;
import org.hibernate.type.IntegerType;
import org.hibernate.type.LongType;
import org.hibernate.type.StringType;
import org.hibernate.type.TimeType;
import org.hibernate.type.TimestampType;

import io.debezium.data.Bits;
import io.debezium.time.Date;
import io.debezium.time.MicroDuration;
import io.debezium.time.ZonedTime;
import io.debezium.time.ZonedTimestamp;

/** Provides functionality to convert {@link Schema} to a proper {@link JaxbHbmHibernateMapping}
 *  to save records in database
 *
 * @author Hossein Torabi
 */
public class HibernateJaxbRecordMapper {
    private static final String DEBEZIUM_DATATYPE_PREFIX = "io.debezium";

    public static final String RESOURCES_PATH = HibernateJaxbRecordMapper.class.getResource("/").getPath();
    public static final String HIBERNATE_XML_SUFFIX = "hbm.xml";
    public static final String DEBEZIUM_ID = "dbz_id";

    /** Makes {@link JaxbHbmHibernateMapping} from record {@link Schema}
     *  with table name and entity name of the topic
     *
     * @param topic topic name to use naming the entity
     * @param schema schema of a whole {@link SinkRecord}
     */
    public static JaxbHbmHibernateMapping getJaxbHbmHibernateMapping(String topic, Schema schema) {
        JaxbHbmHibernateMapping mapping = new JaxbHbmHibernateMapping();

        JaxbHbmRootEntityType entityType = new JaxbHbmRootEntityType();
        entityType.setEntityName(topic);
        entityType.setTable(topic.replace(".", "_"));
        mapping.getClazz().add(entityType);

        entityType.setId(getHibernateIdType());

        schema.fields().forEach((Field field) -> {
            JaxbHbmBasicAttributeType attribute = new JaxbHbmBasicAttributeType();

            attribute.setName(field.name());
            attribute.setNotNull(!field.schema().isOptional());
            attribute.setTypeAttribute(getHibernateAttributeType(field.schema()));
            entityType.getAttributes().add(attribute);
        });

        return mapping;
    }

    private static JaxbHbmSimpleIdType getHibernateIdType() {
        JaxbHbmSimpleIdType id = new JaxbHbmSimpleIdType();
        id.setTypeAttribute(Long.class.getName());

        JaxbHbmGeneratorSpecificationType generatorSpecificationType = new JaxbHbmGeneratorSpecificationType();
        generatorSpecificationType.setClazz("increment");
        id.setGenerator(generatorSpecificationType);
        id.setName(DEBEZIUM_ID);

        return id;

    }

    /**convert {@link Schema} to a DataType that could be used on hibernate based on schema of a Field
     *
     * @param schema schema of a Field {@link Field}
     * @return Hibernate Attribute type from  {@link Schema} of a {@link Field}
     */
    public static String getHibernateAttributeType(Schema schema) {
        String schemaType = schema.type().name().toLowerCase(Locale.ROOT);
        // make priority to debezium data types to detect better schema
        if (schema.name() != null && schema.name().startsWith(DEBEZIUM_DATATYPE_PREFIX)) {
            schemaType = schema.name();
        }

        switch (schemaType) {
            case "int8":
            case "int16":
            case "int32":
                return IntegerType.INSTANCE.getName();
            case "int64":
            case MicroDuration.SCHEMA_NAME:
                return LongType.INSTANCE.getName();
            case "float32":
            case "float64":
                return FloatType.INSTANCE.getName();
            case "boolean":
                return BooleanType.INSTANCE.getName();
            case Bits.LOGICAL_NAME:
                return ByteType.INSTANCE.getName();
            case ZonedTimestamp.SCHEMA_NAME:
                return TimestampType.INSTANCE.getName();
            case ZonedTime.SCHEMA_NAME:
                return TimeType.INSTANCE.getName();
            case Date.SCHEMA_NAME:
                return DateType.INSTANCE.getName();
            default:
                return StringType.INSTANCE.getName();
        }
    }

    /** Persists a {@link JaxbHbmHibernateMapping} to resource path
     * @param mapping JaxbMapping Object created by {@link HibernateJaxbRecordMapper#getJaxbHbmHibernateMapping}
     *
     *
     * @throws JAXBException
     * @throws FileNotFoundException
     */
    public static void persistJaxbHbmHibernateMapping(String topic, JaxbHbmHibernateMapping mapping) throws JAXBException, FileNotFoundException {
        JAXBContext context = JAXBContext.newInstance(JaxbHbmHibernateMapping.class);
        Marshaller marshaller = context.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        marshaller.marshal(
                mapping,
                new FileOutputStream(String.format("%s/%s.%s", RESOURCES_PATH, topic, HIBERNATE_XML_SUFFIX)));
    }

}
