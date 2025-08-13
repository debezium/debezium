/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import static io.debezium.data.Envelope.FieldName.OPERATION;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.data.Envelope.Operation;
import io.debezium.engine.ChangeEvent;
import io.debezium.runtime.CapturingEvent;
import io.debezium.runtime.CapturingEvent.Create;
import io.debezium.runtime.CapturingEvent.Delete;
import io.debezium.runtime.CapturingEvent.Message;
import io.debezium.runtime.CapturingEvent.Read;
import io.debezium.runtime.CapturingEvent.Truncate;
import io.debezium.runtime.CapturingEvent.Update;

public class OperationMapper {

    public static final String NOT_AVAILABLE = "NOT_AVAILABLE";

    public static CapturingEvent<SourceRecord> from(ChangeEvent<SourceRecord, SourceRecord> record) {
        Struct payload = (Struct) record.value().value();

        if (payload == null) {
            return new Message<>(
                    record.value(),
                    record.destination(),
                    NOT_AVAILABLE,
                    record.headers());
        }

        if (payload.schema() == null) {
            return new Message<>(
                    record.value(),
                    record.destination(),
                    NOT_AVAILABLE,
                    record.headers());
        }

        if (payload.schema().field(OPERATION) == null) {
            return new Message<>(
                    record.value(),
                    record.destination(),
                    NOT_AVAILABLE,
                    record.headers());
        }

        return switch (Operation.forCode(payload.getString(OPERATION))) {
            case READ -> new Read<>(
                    record.value(),
                    record.destination(),
                    NOT_AVAILABLE,
                    record.headers());
            case CREATE -> new Create<>(
                    record.value(),
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
