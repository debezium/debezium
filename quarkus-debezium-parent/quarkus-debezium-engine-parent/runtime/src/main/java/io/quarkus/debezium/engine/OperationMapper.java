/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.data.Envelope.Operation;
import io.debezium.engine.ChangeEvent;
import io.debezium.runtime.CapturingEvent;
import io.debezium.runtime.CapturingEvent.*;

public class OperationMapper {

    public static final String NOT_AVAILABLE = "NOT_AVAILABLE";

    public static CapturingEvent<SourceRecord> from(ChangeEvent<SourceRecord, SourceRecord> record) {
        Struct payload = (Struct) record.value().value();

        return switch (Operation.forCode(payload.getString("op"))) {
            case READ -> new Read<>(record.value(),
                    record.destination(),
                    NOT_AVAILABLE,
                    record.headers());
            case CREATE -> new Create<>(record.value(),
                    record.destination(),
                    NOT_AVAILABLE,
                    record.headers());
            case UPDATE -> new Update<>(
                    record.value(),
                    record.destination(),
                    NOT_AVAILABLE,
                    record.headers());

            case DELETE -> new Delete<>(
                    record.value(),
                    record.destination(),
                    NOT_AVAILABLE,
                    record.headers());
            case TRUNCATE -> new Truncate<>(
                    record.value(),
                    record.destination(),
                    NOT_AVAILABLE,
                    record.headers());
            case MESSAGE -> new Message<>(
                    record.value(),
                    record.destination(),
                    NOT_AVAILABLE,
                    record.headers());
        };
    }
}
