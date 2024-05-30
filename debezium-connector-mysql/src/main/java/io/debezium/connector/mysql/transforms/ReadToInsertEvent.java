/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.transforms;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.Module;
import io.debezium.data.Envelope;
import io.debezium.transforms.SmtManager;

/**
 * This SMT allows the MySql connector to emit snapshot events as "c" operation type (CREATE) by changing the 'op' field of the records
 * from "r" (default) to "c".
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Anisha Mohanty
 */
public class ReadToInsertEvent<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadToInsertEvent.class);

    private SmtManager<R> smtManager;

    @Override
    public R apply(R record) {
        if (record.value() == null || !smtManager.isValidEnvelope(record)) {
            return record;
        }

        Struct originalValueStruct = (Struct) record.value();
        Struct updatedValueStruct;
        String operation = originalValueStruct.getString(Envelope.FieldName.OPERATION);

        if (operation.equals(Envelope.Operation.READ.code())) {
            updatedValueStruct = originalValueStruct.put("op", Envelope.Operation.CREATE.code());
        }
        else {
            return record;
        }

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                updatedValueStruct,
                record.timestamp());
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> props) {
        final Configuration config = Configuration.from(props);
        smtManager = new SmtManager<>(config);
    }

    @Override
    public String version() {
        return Module.version();
    }
}
