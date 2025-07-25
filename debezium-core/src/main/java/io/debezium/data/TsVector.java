package io.debezium.data;

import org.apache.kafka.connect.data.SchemaBuilder;


/**
 * @author Pranav Tiwari
 * A semantic type for a PostgreSQL tsvector value.
 */
public class TsVector {

    public static final String LOGICAL_NAME = "io.debezium.data.Tsvector";

    /**
     * Returns a {@link SchemaBuilder} for a Tsvector field.
     *
     * @return the schema builder
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.string()
                .name(LOGICAL_NAME)
                .version(1);
    }

    /**
     * Returns a {@link SchemaBuilder} for a Tsvector field.
     *
     * @param schema the schema builder
     * @return the schema builder
     */
    public static SchemaBuilder builder(SchemaBuilder schema) {
        return schema.name(LOGICAL_NAME).version(1);
    }
}