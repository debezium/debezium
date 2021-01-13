/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
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
    private final boolean exportedSnapshot;
    private final boolean doSnapshot;
    private final PostgresConnectorConfig.TruncateHandlingMode truncateHandlingMode;

    public MessageDecoderConfig(Configuration configuration, PostgresSchema schema, String publicationName, boolean exportedSnapshot, boolean doSnapshot,
                                PostgresConnectorConfig.TruncateHandlingMode truncateHandlingMode) {
        this.configuration = configuration;
        this.schema = schema;
        this.publicationName = publicationName;
        this.exportedSnapshot = exportedSnapshot;
        this.doSnapshot = doSnapshot;
        this.truncateHandlingMode = truncateHandlingMode;
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

    public boolean exportedSnapshot() {
        return exportedSnapshot;
    }

    public boolean doSnapshot() {
        return doSnapshot;
    }

    public PostgresConnectorConfig.TruncateHandlingMode getTruncateHandlingMode() {
        return truncateHandlingMode;
    }

}
