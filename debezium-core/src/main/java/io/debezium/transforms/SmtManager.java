/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope;

/**
 * A class used by all Debezium supplied SMTs to centralize common logic.
 *
 * @param <R> the subtype of {@link ConnectRecord} on which the transformation will operate
 * @author Jiri Pechanec
 */
public class SmtManager<R extends ConnectRecord<R>> {

    private static final String RECORD_ENVELOPE_KEY_SCHEMA_NAME_SUFFIX = ".Key";

    private static final Logger LOGGER = LoggerFactory.getLogger(SmtManager.class);

    public SmtManager(Configuration config) {
    }

    public boolean isValidEnvelope(final R record) {
        if (record.valueSchema() == null ||
                record.valueSchema().name() == null ||
                !Envelope.isEnvelopeSchema(record.valueSchema())) {
            LOGGER.debug("Expected Envelope for transformation, passing it unchanged");
            return false;
        }
        return true;
    }

    public boolean isValidKey(final R record) {
        if (record.keySchema() == null ||
                record.keySchema().name() == null ||
                !record.keySchema().name().endsWith(RECORD_ENVELOPE_KEY_SCHEMA_NAME_SUFFIX)) {
            LOGGER.debug("Expected Key Schema for transformation, passing it unchanged. Message key: \"{}\"", record.key());
            return false;
        }
        return true;
    }
}
