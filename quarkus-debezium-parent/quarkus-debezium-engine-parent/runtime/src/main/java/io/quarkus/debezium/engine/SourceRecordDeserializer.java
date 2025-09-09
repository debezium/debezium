/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import static io.debezium.data.Envelope.FieldName.AFTER;
import static io.debezium.data.Envelope.FieldName.BEFORE;
import static org.apache.kafka.connect.json.JsonConverterConfig.SCHEMAS_ENABLE_CONFIG;

import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;

import io.debezium.data.Envelope;
import io.debezium.runtime.CapturingEvent;
import io.debezium.runtime.CapturingEvent.Create;
import io.debezium.runtime.CapturingEvent.Delete;
import io.debezium.runtime.CapturingEvent.Message;
import io.debezium.runtime.CapturingEvent.Read;
import io.debezium.runtime.CapturingEvent.Truncate;
import io.debezium.runtime.CapturingEvent.Update;
import io.quarkus.debezium.engine.deserializer.Deserializer;

public class SourceRecordDeserializer<T> implements CapturingEventDeserializer<T, SourceRecord> {

    private final Deserializer<T> deserializer;
    private final Converter converter;

    public SourceRecordDeserializer(Deserializer<T> deserializer, Converter converter) {
        this.deserializer = deserializer;
        this.converter = converter;
        this.converter.configure(Map.of(SCHEMAS_ENABLE_CONFIG, true), false);
    }

    @Override
    public CapturingEvent<T> deserialize(CapturingEvent<SourceRecord> event) {
        byte[] data = converter.fromConnectData(
                event.record().topic(),
                event.record().valueSchema(),
                event.record().value());

        if (!Envelope.isEnvelopeSchema(event.record().valueSchema())) {
            return new Message<>(
                    deserializer.deserialize(data, null),
                    event.destination(),
                    event.source(),
                    event.headers(),
                    event.group());
        }

        return switch (event) {
            case Create<SourceRecord> record -> new Create<>(
                    deserializer.deserialize(data, AFTER),
                    record.destination(),
                    record.source(),
                    record.headers(),
                    event.group());
            case Delete<SourceRecord> record -> new Delete<>(
                    deserializer.deserialize(data, BEFORE),
                    record.destination(),
                    record.source(),
                    record.headers(),
                    event.group());
            case Message<SourceRecord> record -> new Message<>(
                    deserializer.deserialize(data, AFTER),
                    record.destination(),
                    record.source(),
                    record.headers(),
                    event.group());
            case Read<SourceRecord> record -> new Read<>(
                    deserializer.deserialize(data, AFTER),
                    record.destination(),
                    record.source(),
                    record.headers(),
                    event.group());
            case Truncate<SourceRecord> record -> new Truncate<>(
                    deserializer.deserialize(data, AFTER),
                    record.destination(),
                    record.source(),
                    record.headers(),
                    event.group());
            case CapturingEvent.Update<SourceRecord> record -> new Update<>(
                    deserializer.deserialize(data, AFTER),
                    record.destination(),
                    record.source(),
                    record.headers(),
                    event.group());
        };
    }
}
