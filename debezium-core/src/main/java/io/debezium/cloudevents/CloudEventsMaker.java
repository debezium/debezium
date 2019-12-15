/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.cloudevents;

import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.TimeZone;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import io.debezium.util.Collect;

/**
 * An abstract class that builds CloudEvents attributes using fields of change records provided by
 * {@link RecordParser RecordParser}. Callers {@link #create(RecordParser, SerializerType, String)} create} a concrete
 * CloudEventsMaker for a specific connector type.
 */
public abstract class CloudEventsMaker {

    /**
     * The constants for the names of CloudEvents attributes.
     */
    public static final class FieldName {
        public static final String ID = "id";
        public static final String SOURCE = "source";
        public static final String SPECVERSION = "specversion";
        public static final String TYPE = "type";
        public static final String DATACONTENTTYPE = "datacontenttype";
        public static final String DATASCHEMA = "dataschema";
        public static final String TIME = "time";
        public static final String EXTRAINFO = "extrainfo";
        public static final String DATA = "data";
        public static final String SCHEMA_FIELD_NAME = "schema";
        public static final String PAYLOAD_FIELD_NAME = "payload";
    }

    public static final String CLOUDEVENTS_SPECVERSION = "1.0";

    private static SerializerType dataContentType;
    private static String dataSchemaUrl;
    private static Schema ceExtrainfoSchema;
    private static Schema ceDataAttributeSchema;

    RecordParser recordParser;

    static final Map<SerializerType, String> CONTENT_TYPE_NAME_MAP = Collect.hashMapOf(
            SerializerType.JSON, "application/json",
            SerializerType.AVRO, "avro/binary");

    /**
     * Create a concrete CloudEvents maker using the outputs of a record parser. Also need to specify the data content
     * type (that is the serialization format of the data attribute).
     *
     * @param parser the parser of a change record
     * @param contentType the data content type of CloudEvents
     * @return a concrete CloudEvents maker
     */
    public static CloudEventsMaker create(RecordParser parser, SerializerType contentType) {
        dataContentType = contentType;
        ceExtrainfoSchema = ceDataAttributeSchema = null;

        switch (parser.connectorType()) {
            case "mysql":
                return new MysqlCloudEventsMaker(parser);
            case "postgresql":
                return new PostgresCloudEventsMaker(parser);
            case "mongodb":
                return new MongodbCloudEventsMaker(parser);
            case "sqlserver":
                return new SqlserverCloudEventsMaker(parser);
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
     * @param schemaUrl the url of data schema registry; may be null
     * @return a concrete CloudEvents maker
     */
    public static CloudEventsMaker create(RecordParser parser, SerializerType contentType, String schemaUrl) {
        dataSchemaUrl = schemaUrl;
        return create(parser, contentType);
    }

    CloudEventsMaker(RecordParser parser) {
        recordParser = parser;
    }

    /**
     * Construct the id of CloudEvents envelope.
     *
     * @return the id of CloudEvents envelope
     */
    public abstract String ceId();

    /**
     * Construct the source field of CloudEvents envelope.
     *
     * @return the source field of CloudEvents envelope
     */
    public String ceSource() {
        return "/debezium/" + recordParser.connectorType();
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
    public String ceDataschema() {
        return dataSchemaUrl;
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
     * Construct the schema of the extrainfo field of CloudEvents envelope.
     *
     * @return the schema of the extrainfo field of CloudEvents envelope
     */
    public Schema ceExtrainfoSchema() {
        if (ceExtrainfoSchema != null) {
            return ceExtrainfoSchema;
        }
        ceExtrainfoSchema = SchemaBuilder.struct()
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP, Schema.STRING_SCHEMA)
                .field(Envelope.FieldName.SOURCE, recordParser.source().schema());
        return ceExtrainfoSchema;
    }

    /**
     * Construct the value of the extrainfo field of CloudEvents envelope.
     *
     * @return the value of extrainfo field of CloudEvents envelope
     */
    public Struct ceExtrainfo() {
        return new Struct(ceExtrainfoSchema())
                .put(Envelope.FieldName.OPERATION, recordParser.op())
                .put(Envelope.FieldName.TIMESTAMP, recordParser.ts_ms())
                .put(Envelope.FieldName.SOURCE, recordParser.source());
    }

    /**
     * Construct the schema of the data attribute of CloudEvents.
     *
     * @return the schema of the data attribute of CloudEvents
     */
    public Schema ceDataAttributeSchema() {
        if (ceDataAttributeSchema != null) {
            return ceDataAttributeSchema;
        }

        SchemaBuilder builder = SchemaBuilder.struct().name(ceDataAttributeSchemaName());
        if (recordParser.beforeSchema() != null) {
            builder.field(Envelope.FieldName.BEFORE, recordParser.beforeSchema());
        }
        if (recordParser.afterSchema() != null) {
            builder.field(Envelope.FieldName.AFTER, recordParser.afterSchema());
        }
        ceDataAttributeSchema = builder.build();
        return ceDataAttributeSchema;
    }

    /**
     * Construct the value of the data attribute of CloudEvents.
     *
     * @return the value of the data attribute of CloudEvents
     */
    public Struct ceDataAttribute() {
        Struct data = new Struct(ceDataAttributeSchema());
        if (recordParser.before() != null) {
            data.put(Envelope.FieldName.BEFORE, recordParser.before());
        }
        if (recordParser.after() != null) {
            data.put(Envelope.FieldName.AFTER, recordParser.after());
        }
        return data;
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
     * Construct the name of the schema of the data attribute of CloudEvents.
     *
     * @return the name of the schema of the data attribute of CloudEvents
     */
    public String ceDataAttributeSchemaName() {
        return "io.debezium.connector." + recordParser.connectorType() + ".Data";
    }

    /**
     * CloudEvents maker for records produced by MySQL connector.
     */
    public static final class MysqlCloudEventsMaker extends CloudEventsMaker {
        MysqlCloudEventsMaker(RecordParser parser) {
            super(parser);
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
        PostgresCloudEventsMaker(RecordParser parser) {
            super(parser);
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
        MongodbCloudEventsMaker(RecordParser parser) {
            super(parser);
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
        SqlserverCloudEventsMaker(RecordParser parser) {
            super(parser);
        }

        @Override
        public String ceId() {
            return "name:" + recordParser.getMetadata(AbstractSourceInfo.SERVER_NAME_KEY)
                    + ";change_lsn:" + recordParser.getMetadata(RecordParser.SqlserverRecordParser.CHANGE_LSN_KEY)
                    + ";commit_lsn:" + recordParser.getMetadata(RecordParser.SqlserverRecordParser.COMMIT_LSN_KEY);
        }
    }
}
