/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.docling;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * Defines the Kafka Connect {@link Schema} functionality associated with a <a href="https://docling-project.github.io/docling/">Docling</a> document and related utility functions.
 */
public class DoclingDocument {

    public static final String SCHEMA_NAME = "io.debezium.ai.docling.DoclingDocument";
    public static final String TYPE_FIELD_NAME = "type";
    public static final String CONTENT_FIELD_NAME = "content";

    /**
     * Returns a {@link SchemaBuilder} for a {@link DoclingDocument}.
     * The resulting schema will describe type of Docling document and its content.
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.struct()
                .name(SCHEMA_NAME)
                .field(TYPE_FIELD_NAME, Schema.STRING_SCHEMA)
                .field(CONTENT_FIELD_NAME, Schema.STRING_SCHEMA)
                .version(1);
    }

    /**
     * Returns a Schema for a {@link DoclingDocument}.
     */
    public static Schema schema() {
        return builder().build();
    }

    /**
     * Return a new Struct for Docling document.
     */
    public static Struct doclingContent(String type, String content) {
        return new Struct(DoclingDocument.schema())
                .put("type", type)
                .put("content", content);
    }
}
