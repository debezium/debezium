/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.converters;

import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.TimeZone;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.util.Collect;

/**
 * An abstract class that builds CloudEvents attributes using fields of change records provided by
 * {@link RecordParser RecordParser}. Callers {@link #create(RecordParser, SerializerType, String)} create} a concrete
 * CloudEventsMaker for a specific connector type.
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

    /**
     * Create a concrete CloudEvents maker using the outputs of a record parser. Also need to specify the data content
     * type (that is the serialization format of the data attribute).
     *
     * @param parser the parser of a change record
     * @param contentType the data content type of CloudEvents
     * @param dataSchemaUriBase the URI of the schema in case of Avro; may be null
     * @return a concrete CloudEvents maker
     */
    public static CloudEventsMaker create(RecordParser parser, SerializerType contentType, String dataSchemaUriBase) {
        switch (parser.connectorType()) {
            case "mysql":
                return new MysqlCloudEventsMaker(parser, contentType, dataSchemaUriBase);
            case "postgresql":
                return new PostgresCloudEventsMaker(parser, contentType, dataSchemaUriBase);
            case "mongodb":
                return new MongodbCloudEventsMaker(parser, contentType, dataSchemaUriBase);
            case "sqlserver":
                return new SqlserverCloudEventsMaker(parser, contentType, dataSchemaUriBase);
            default:
                throw new DataException("No usable CloudEvents converters for connector type \"" + parser.connectorType() + "\"");
        }
    }

    /**
     * Create a concrete CloudEvents maker using the outputs of a record parser. Also need to specify the data content
     * type (that is the serialization format of the data attribute) and the url of data schema registry when using Avro
     * as the data content type.
     *
     * @param parser the parser of a change record
     * @param contentType the data content type of CloudEvents
     *
     * @return a concrete CloudEvents maker
     */
    public static CloudEventsMaker create(RecordParser parser, SerializerType contentType) {
        return create(parser, contentType, null);
    }

    private CloudEventsMaker(RecordParser parser, SerializerType contentType, String dataSchemaUriBase) {
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
        return "io.debezium." + recordParser.connectorType() + ".datachangeevent";
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

    /**
     * CloudEvents maker for records produced by MySQL connector.
     */
    public static final class MysqlCloudEventsMaker extends CloudEventsMaker {
        MysqlCloudEventsMaker(RecordParser parser, SerializerType contentType, String dataSchemaUriBase) {
            super(parser, contentType, dataSchemaUriBase);
        }

        @Override
        public String ceId() {
            return "name:" + recordParser.getMetadata(AbstractSourceInfo.SERVER_NAME_KEY)
                    + ";file:" + recordParser.getMetadata(RecordParser.MysqlRecordParser.BINLOG_FILENAME_OFFSET_KEY)
                    + ";pos:" + recordParser.getMetadata(RecordParser.MysqlRecordParser.BINLOG_POSITION_OFFSET_KEY);
        }
    }

    /**
     * CloudEvents maker for records produced by PostgreSQL connector.
     */
    public static final class PostgresCloudEventsMaker extends CloudEventsMaker {
        PostgresCloudEventsMaker(RecordParser parser, SerializerType contentType, String dataSchemaUriBase) {
            super(parser, contentType, dataSchemaUriBase);
        }

        @Override
        public String ceId() {
            return "name:" + recordParser.getMetadata(AbstractSourceInfo.SERVER_NAME_KEY)
                    + ";lsn:" + recordParser.getMetadata(RecordParser.PostgresRecordParser.LSN_KEY).toString()
                    + ";txId:" + recordParser.getMetadata(RecordParser.PostgresRecordParser.TXID_KEY).toString();
        }
    }

    /**
     * CloudEvents maker for records produced by MongoDB connector.
     */
    public static final class MongodbCloudEventsMaker extends CloudEventsMaker {
        MongodbCloudEventsMaker(RecordParser parser, SerializerType contentType, String dataSchemaUriBase) {
            super(parser, contentType, dataSchemaUriBase);
        }

        @Override
        public String ceId() {
            return "name:" + recordParser.getMetadata(AbstractSourceInfo.SERVER_NAME_KEY)
                    + ";h:" + recordParser.getMetadata(RecordParser.MongodbRecordParser.OPERATION_ID);
        }
    }

    /**
     * CloudEvents maker for records produced by SQL Server connector.
     */
    public static final class SqlserverCloudEventsMaker extends CloudEventsMaker {
        SqlserverCloudEventsMaker(RecordParser parser, SerializerType contentType, String dataSchemaUriBase) {
            super(parser, contentType, dataSchemaUriBase);
        }

        @Override
        public String ceId() {
            return "name:" + recordParser.getMetadata(AbstractSourceInfo.SERVER_NAME_KEY)
                    + ";change_lsn:" + recordParser.getMetadata(RecordParser.SqlserverRecordParser.CHANGE_LSN_KEY)
                    + ";commit_lsn:" + recordParser.getMetadata(RecordParser.SqlserverRecordParser.COMMIT_LSN_KEY)
                    + ";event_serial_no:" + recordParser.getMetadata(RecordParser.SqlserverRecordParser.EVENT_SERIAL_NO_KEY);
        }
    }
}
