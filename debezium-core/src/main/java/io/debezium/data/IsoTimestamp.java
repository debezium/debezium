/*
 * Copyright 2015 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * An ISO8601 timestamp that includes the time zone. The logical values for this schema type are represented with the standard
 * {@link OffsetDateTime}, which represents an instant with the offset from UTC/Greenwich. {@link OffsetDateTime} has a
 * well-defined ordering and thus is more suitable to be persisted; the {@link java.time.ZonedDateTime} has built-in support for
 * time zone rules (e.g., daylight savings time and other anomalies) and therefore does not have a well-defined ordering.
 * Typically, an {@link OffsetDateTime} that is read from a persisted state can be converted to a {@link java.time.ZonedDateTime}
 * or other time representations for a specific time zone with proper time zone handling, including handling daylight savings
 * time.
 * <p>
 * The encoded representation is the UFT-8 byte representation of an ISO8601 string.
 * 
 * @author Randall Hauch
 */
public class IsoTimestamp {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
    private static final Charset CHARSET = StandardCharsets.UTF_8;

    public static final String LOGICAL_NAME = "io.debezium.data.IsoTimestamp";

    /**
     * Returns a {@link SchemaBuilder} for an IsoTimestamp. You can use the resulting SchemaBuilder
     * to set additional schema settings such as required/optional, default value, and documentation.
     * 
     * @return the schema builder
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.bytes()
                            .name(LOGICAL_NAME)
                            .version(1);
    }

    /**
     * Returns a Schema for an IsoTimestamp but with all other default Schema settings.
     * 
     * @return the schema
     * @see #builder()
     */
    public static Schema schema() {
        return builder().build();
    }

    /**
     * Convert a value from its logical format ({@link OffsetDateTime}) to it's encoded format.
     * 
     * @param schema the schema
     * @param value the logical value
     * @return the encoded value
     */
    public static byte[] fromLogical(Schema schema, OffsetDateTime value) {
        return value.format(FORMATTER).getBytes(CHARSET);
    }

    /**
     * Convert a value from its encoded format into its logical format ({@link OffsetDateTime}).
     * 
     * @param schema the schema
     * @param value the encoded value
     * @return the logical value
     */
    public static OffsetDateTime toLogical(Schema schema, byte[] value) {
        return OffsetDateTime.parse(new String(value, CHARSET), FORMATTER);
    }
}
