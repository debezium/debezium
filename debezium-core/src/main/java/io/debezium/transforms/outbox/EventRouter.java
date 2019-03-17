/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.outbox;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * Debezium Outbox Transform Event Router
 *
 * @author Renato mefi (gh@mefi.in)
 */
public class EventRouter<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventRouter.class);

    private EventRouterConfigDefinition.InvalidOperationBehavior invalidOperationBehavior;

    @Override
    public R apply(R r) {
        // Ignoring tombstones
        if (r.value() == null) {
            LOGGER.info("Tombstone message {} ignored", r.key());
            return null;
        }

        Struct value = requireStruct(r.value(), "b");
        String op = value.getString(Envelope.FieldName.OPERATION);

        // Skipping deletes
        if (op.equals(Envelope.Operation.DELETE.code())) {
            LOGGER.info("Delete message {} ignored", r.key());
            return null;
        }

        // Dealing with unexpected update operations
        if (op.equals(Envelope.Operation.UPDATE.code())) {
            handleUnexpectedOperation(r);
            return null;
        }

        return r;
    }

    private void handleUnexpectedOperation(R r) {
        switch (invalidOperationBehavior) {
            case SKIP_AND_WARN:
                LOGGER.warn("Unexpected update message received {} and ignored", r.key());
                break;
            case SKIP_AND_ERROR:
                LOGGER.error("Unexpected update message received {} and ignored", r.key());
                break;
            case FATAL:
                throw new IllegalStateException(String.format("Unexpected update message received %s, fail.", r.key()));
        }
    }

    @Override
    public ConfigDef config() {
        return EventRouterConfigDefinition.configDef();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configMap) {
        final Configuration config = Configuration.from(configMap);

        invalidOperationBehavior = EventRouterConfigDefinition.InvalidOperationBehavior.parse(
                config.getString(EventRouterConfigDefinition.OPERATION_INVALID_BEHAVIOR)
        );
    }
}
