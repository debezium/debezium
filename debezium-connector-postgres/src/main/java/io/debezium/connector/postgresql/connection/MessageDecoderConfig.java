/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresSchema;

/**
 * Configuration parameter object for a {@link MessageDecoder}
 *
 * @author Chris Cranford
 */
public class MessageDecoderConfig {

    private final Configuration configuration;
    private final PostgresSchema schema;
    private final String publicationName;

    public MessageDecoderConfig(Configuration configuration, PostgresSchema schema, String publicationName) {
        this.configuration = configuration;
        this.schema = schema;
        this.publicationName = publicationName;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public PostgresSchema getSchema() {
        return schema;
    }

    public String getPublicationName() {
        return publicationName;
    }
}
