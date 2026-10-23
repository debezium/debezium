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
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.Module;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.metadata.ConfigDescriptor;
import io.debezium.transforms.neo4j.DebeziumEnvelopeFactory;
import io.debezium.transforms.neo4j.Neo4jCdcEvent;
import io.debezium.transforms.neo4j.Neo4jDebeziumConverterConfig;
import io.debezium.transforms.neo4j.Neo4jDebeziumConverterConfig.FieldMissingBehavior;

/**
 * A Kafka Connect SMT that converts Neo4j CDC change events into the Debezium envelope format, enabling
 * graph-to-relational CDC pipelines:.
 * <p>
 * The SMT is stateless and zero-config by default: Neo4j CDC events are self-describing,
 * so it derives the full mapping from each event, with optional overrides.
 *
 * @param <R> the subtype of {@link ConnectRecord} on which the transformation will operate
 */
public class Neo4jDebeziumConverter<R extends ConnectRecord<R>> implements Transformation<R>, Versioned, ConfigDescriptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jDebeziumConverter.class);

    private SmtManager<R> smtManager;
    private Neo4jDebeziumConverterConfig converterConfig;
    private DebeziumEnvelopeFactory debeziumEnvelopeFactory;

    @Override
    public void configure(Map<String, ?> props) {
        final var config = Configuration.from(props);
        this.smtManager = new SmtManager<>(config);
        this.smtManager.validate(config, Neo4jDebeziumConverterConfig.ALL_FIELDS);
        this.converterConfig = Neo4jDebeziumConverterConfig.from(config, props);
        this.debeziumEnvelopeFactory = new DebeziumEnvelopeFactory(converterConfig);
    }

    @Override
    public R apply(R record) {
        if (record.value() == null) {
            return handleTombstone(record);
        }

        final var recognition = Neo4jCdcEvent.from(record.value());
        if (!recognition.recognized()) {
            LOGGER.debug("Passing record through unchanged: {}", recognition.skipReason());
            return record;
        }

        final var built = debeziumEnvelopeFactory.build(recognition.event());
        if (built.isSkipped()) {
            // The event was skipped per field.missing.behavior (fail would have thrown in the factory, so here it is warn or ignore).
            // Log the returned reason at the matching level and drop it, rather than pass a raw Neo4j CDC event through to the JDBC sink.
            logSkipped(built.skipReason());
            return null;
        }

        final var emitted = built.record();
        return record.newRecord(
                emitted.table(),
                record.kafkaPartition(),
                emitted.keySchema(),
                emitted.key(),
                emitted.valueSchema(),
                emitted.value(),
                record.timestamp(),
                record.headers());
    }

    @Override
    public ConfigDef config() {
        final var config = new ConfigDef();
        Field.group(config, null,
                Neo4jDebeziumConverterConfig.TABLE_NAMING,
                Neo4jDebeziumConverterConfig.COLUMN_NAMING,
                Neo4jDebeziumConverterConfig.RELATIONSHIP_FK_NAMING,
                Neo4jDebeziumConverterConfig.FIELD_MISSING_BEHAVIOR,
                Neo4jDebeziumConverterConfig.TOMBSTONES_ENABLED);
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
        return Neo4jDebeziumConverterConfig.ALL_FIELDS;
    }

    private void logSkipped(String reason) {
        if (converterConfig.fieldMissingBehavior() == FieldMissingBehavior.WARN) {
            LOGGER.warn("Dropping Neo4j CDC event: {} (field.missing.behavior=warn)", reason);
        }
        else {
            LOGGER.debug("Dropping Neo4j CDC event: {} (field.missing.behavior=ignore)", reason);
        }
    }

    private R handleTombstone(R record) {
        if (converterConfig.tombstonesEnabled()) {
            LOGGER.trace("Passing through tombstone record");
            return record;
        }
        return null;
    }
}
