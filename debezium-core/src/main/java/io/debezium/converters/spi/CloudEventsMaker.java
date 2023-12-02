/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.converters.spi;

import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.TimeZone;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.util.Collect;

/**
 * An abstract class that builds CloudEvents attributes using fields of change records provided by
 * {@link RecordParser RecordParser}.
 */
public abstract class CloudEventsMaker {

    private static final String SCHEMA_URL_PATH = "/schemas/ids/";

    /**
     * The constants for the names of CloudEvents attributes.
     */
    public static final class FieldName {

        /**
         * CloudEvents context attributes (REQUIRED)
         */
        public static final String ID = "id";
        public static final String SOURCE = "source";
        public static final String SPECVERSION = "specversion";
        public static final String TYPE = "type";

        /**
         * CloudEvents context attributes (OPTIONAL)
         */
        public static final String DATACONTENTTYPE = "datacontenttype";
        public static final String DATASCHEMA = "dataschema";

        // TODO DBZ-1701 not used
        public static final String SUBJECT = "subject";
        public static final String TIME = "time";

        /**
         * Event data
         */
        public static final String DATA = "data";

        /**
         * Schema and payload within event data
         */
        public static final String SCHEMA_FIELD_NAME = "schema";
        public static final String PAYLOAD_FIELD_NAME = "payload";
    }

    public static final String CLOUDEVENTS_SPECVERSION = "1.0";

    private final SerializerType dataContentType;
    private final String dataSchemaUriBase;
    private final Schema ceDataAttributeSchema;

    protected final RecordParser recordParser;

    static final Map<SerializerType, String> CONTENT_TYPE_NAME_MAP = Collect.hashMapOf(
            SerializerType.JSON, "application/json",
            SerializerType.AVRO, "application/avro");

    protected CloudEventsMaker(RecordParser parser, SerializerType contentType, String dataSchemaUriBase) {
        this.recordParser = parser;
        this.dataContentType = contentType;
        this.dataSchemaUriBase = dataSchemaUriBase;
        this.ceDataAttributeSchema = recordParser.dataSchema();
    }

    /**
     * Construct the id of CloudEvents envelope.
     *
     * @return the id of CloudEvents envelope
     */
    public abstract String ceId();

    /**
     * Construct the source field of CloudEvents envelope, e.g. "/debezium/postgres/dbserver1".
     *
     * @return the source field of CloudEvents envelope
     */
    public String ceSource(String logicalName) {
        return "/debezium/" + recordParser.connectorType() + "/" + logicalName;
    }

    /**
     * Get the version of CloudEvents specification.
     *
     * @return the version of CloudEvents specification
     */
    public String ceSpecversion() {
        return CLOUDEVENTS_SPECVERSION;
    }

    /**
     * Construct the type field of CloudEvents envelope.
     *
     * @return the type field of CloudEvents envelope
     */
    public String ceType() {
        return "io.debezium.connector." + recordParser.connectorType() + ".DataChangeEvent";
    }

    /**
     * Get the data content type of CloudEvents envelope.
     *
     * @return the data content type of CloudEvents envelope
     */
    public String ceDatacontenttype() {
        return CONTENT_TYPE_NAME_MAP.get(dataContentType);
    }

    /**
     * Get the data schema url of CloudEvents envelope.
     *
     * @return the data schema url of CloudEvents envelope
     */
    // TODO DBZ-1701: the exported path should be configurable, e.g. it could be a
    // proxy URL for external consumers or even just be omitted altogether
    public String ceDataschemaUri(String schemaId) {
        return dataSchemaUriBase + SCHEMA_URL_PATH + schemaId;
    }

    /**
     * Get the timestamp of CloudEvents envelope using the format defined in RFC 3339.
     *
     * @return the timestamp of CloudEvents envelope
     */
    public String ceTime() {
        long time = (long) recordParser.getMetadata(AbstractSourceInfo.TIMESTAMP_KEY);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        return formatter.format(time);
    }

    /**
     * Construct the schema of the data attribute of CloudEvents.
     *
     * @return the schema of the data attribute of CloudEvents
     */
    public Schema ceDataAttributeSchema() {
        return ceDataAttributeSchema;
    }

    /**
     * Construct the value of the data attribute of CloudEvents.
     *
     * @return the value of the data attribute of CloudEvents
     */
    public Struct ceDataAttribute() {
        return recordParser.data();
    }

    /**
     * Construct the name of the schema of CloudEvents envelope.
     *
     * @return the name of the schema of CloudEvents envelope
     */
    public String ceEnvelopeSchemaName() {
        return recordParser.getMetadata(AbstractSourceInfo.SERVER_NAME_KEY) + "."
                + recordParser.getMetadata(AbstractSourceInfo.DATABASE_NAME_KEY) + "."
                + "CloudEvents.Envelope";
    }
}
