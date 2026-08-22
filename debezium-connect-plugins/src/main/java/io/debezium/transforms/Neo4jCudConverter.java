/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.Module;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.data.Envelope;
import io.debezium.metadata.ConfigDescriptor;
import io.debezium.transforms.neo4j.CudEvent;
import io.debezium.transforms.neo4j.CudEventFactory;
import io.debezium.transforms.neo4j.CudEventSerializer;
import io.debezium.transforms.neo4j.Neo4jCudConverterConfig;
import io.debezium.transforms.neo4j.Neo4jCudConverterConfig.OutputMode;

/**
 * A Kafka Connect SMT that converts Debezium change events into Neo4j CUD format,
 * enabling CDC pipelines from relational databases to Neo4j via the Neo4j Kafka sink connector.
 *
 * @param <R> the subtype of {@link ConnectRecord} on which the transformation will operate
 * @see <a href="https://neo4j.com/docs/kafka/current/sink/cud-file-format/">Neo4j CUD format</a>
 */
public class Neo4jCudConverter<R extends ConnectRecord<R>> implements Transformation<R>, Versioned, ConfigDescriptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jCudConverter.class);

    private SmtManager<R> smtManager;
    private Neo4jCudConverterConfig converterConfig;
    private CudEventFactory eventFactory;
    private CudEventSerializer serializer;

    @Override
    public void configure(Map<String, ?> props) {
        final var config = Configuration.from(props);
        this.smtManager = new SmtManager<>(config);
        this.smtManager.validate(config, Neo4jCudConverterConfig.ALL_FIELDS);
        this.converterConfig = Neo4jCudConverterConfig.from(config, props);
        this.eventFactory = new CudEventFactory(converterConfig);
        this.serializer = new CudEventSerializer();
    }

    @Override
    public R apply(R record) {
        if (record.value() == null) {
            return handleTombstone(record);
        }
        if (isNotValidMessage(record)) {
            return record;
        }

        return handleMessage(record);
    }

    @Override
    public ConfigDef config() {
        final var config = new ConfigDef();
        Field.group(config, null,
                Neo4jCudConverterConfig.NODE_MODE,
                Neo4jCudConverterConfig.NODE_LABELS,
                Neo4jCudConverterConfig.NODE_ID_PROPERTIES,
                Neo4jCudConverterConfig.NODE_PROPERTIES_INCLUDE,
                Neo4jCudConverterConfig.NODE_PROPERTIES_EXCLUDE,
                Neo4jCudConverterConfig.NODE_DELETE_DETACH,
                Neo4jCudConverterConfig.OUTPUT_MODE,
                Neo4jCudConverterConfig.TOMBSTONES_ENABLED);
        return config;
    }

    @Override
    public void close() {
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public Field.Set getConfigFields() {
        return Neo4jCudConverterConfig.ALL_FIELDS;
    }

    private R handleMessage(R record) {
        final var message = (Struct) record.value();
        final var op = resolveOperation(message);
        if (op == null) {
            return record;
        }

        final var data = resolveData(message, op);
        final var cudOp = op == Envelope.Operation.DELETE ? CudEvent.Operation.DELETE : CudEvent.Operation.MERGE;
        final var events = eventFactory.buildEvents(data, cudOp);

        final var serializedContent = converterConfig.outputMode() == OutputMode.ARRAY
                ? serializer.serializeArray(events)
                : serializer.serializeSingle(events.get(0));

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                Schema.STRING_SCHEMA,
                serializedContent,
                record.timestamp(),
                record.headers());
    }

    private boolean isNotValidMessage(R record) {
        return !smtManager.isValidEnvelope(record);
    }

    private R handleTombstone(R record) {
        if (converterConfig.tombstonesEnabled()) {
            LOGGER.trace("Passing through tombstone record");
            return record;
        }
        return null;
    }

    private Envelope.Operation resolveOperation(Struct envelope) {
        final var op = Envelope.Operation.forCode(envelope.getString(Envelope.FieldName.OPERATION));

        if (op == Envelope.Operation.TRUNCATE || op == Envelope.Operation.MESSAGE) {
            LOGGER.debug("Skipping non-DML operation: {}", envelope.getString(Envelope.FieldName.OPERATION));
            return null;
        }
        return op;
    }

    private Struct resolveData(Struct envelope, Envelope.Operation op) {
        if (op == Envelope.Operation.DELETE) {
            return envelope.getStruct(Envelope.FieldName.BEFORE);
        }
        return envelope.getStruct(Envelope.FieldName.AFTER);
    }

}
