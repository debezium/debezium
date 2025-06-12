/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.transforms.timescaledb;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.common.annotation.VisibleForTesting;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.postgresql.Module;
import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.data.Envelope;
import io.debezium.relational.TableId;
import io.debezium.transforms.SmtManager;

/**
 * Single message transform that modifies records coming from TimescaleDB.
 * It operates on chunks and aggregate's materialized hypertables.
 * <ul>
 * <li>chunk source info block and topic name is replaced with appropriate hypertable name</li>
 * <li>chunks belonging to hypertables acting as materialized aggregates have topic name and source info block updated with aggregate name</li>
 * <li>headers are added with original chunk schema/table name and for aggregates with original materialized hypertable schema/table name</li>
 * </ul>
 * @author Jiri Pechanec
 *
 * @param <R>
 */
public class TimescaleDb<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimescaleDb.class);

    public static final String HEADER_CHUNK_TABLE = "__debezium_timescaledb_chunk_table";
    public static final String HEADER_CHUNK_SCHEMA = "__debezium_timescaledb_chunk_schema";
    public static final String HEADER_HYPERTABLE_TABLE = "__debezium_timescaledb_hypertable_table";
    public static final String HEADER_HYPERTABLE_SCHEMA = "__debezium_timescaledb_hypertable_schema";

    private SmtManager<R> smtManager;
    private TimescaleDbMetadata metadata;
    private String topicPrefix;

    @Override
    public void configure(Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        smtManager = new SmtManager<>(config);

        if (metadata == null) {
            metadata = new QueryInformationSchemaMetadata(config);
        }
        topicPrefix = config.getString(TimescaleDbConfigDefinition.TARGET_TOPIC_PREFIX_FIELD) + ".";
    }

    @Override
    public R apply(R record) {
        if (!smtManager.isValidEnvelope(record)) {
            return record;
        }

        final var source = ((Struct) record.value()).getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            LOGGER.debug("Incoming record has an empty source info block {}", record);
            return record;
        }

        final var schemaName = source.getString(SourceInfo.SCHEMA_NAME_KEY);
        final var tableName = source.getString(SourceInfo.TABLE_NAME_KEY);

        if (schemaName == null || tableName == null) {
            LOGGER.debug("Incoming record has an empty schema '{}' or table '{}' name", schemaName, tableName);
            return record;
        }

        if (!metadata.isTimescaleDbSchema(schemaName)) {
            LOGGER.trace("Record for schema '{}' is not intended for TimescaleDB processing", schemaName);
            return record;
        }

        final var chunkId = new TableId(null, schemaName, tableName);
        final var hypertableId = metadata.hypertableId(chunkId);
        if (hypertableId.isEmpty()) {
            LOGGER.warn("Unable to find hypertable for chunk '{}'", chunkId);
            return record;
        }

        final var aggregateId = metadata.aggregateId(hypertableId.get());
        // If the data are coming from aggregate then we should use its metadata, otherwise hypertable metadata are used
        if (aggregateId.isPresent()) {
            LOGGER.trace("Changing metadata for aggregate from '{}' to '{}'", chunkId, aggregateId);
        }
        else {
            LOGGER.trace("Changing metadata for hypertable from '{}' to '{}'", chunkId, aggregateId);
        }

        final var newId = aggregateId.orElse(hypertableId.get());

        source.put(SourceInfo.SCHEMA_NAME_KEY, newId.schema());
        source.put(SourceInfo.TABLE_NAME_KEY, newId.table());

        return record.newRecord(
                getNewTopicName(newId),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                record.value(),
                record.timestamp(),
                addHeaders(record.headers(), chunkId, hypertableId.get(), aggregateId));
    }

    private String getNewTopicName(TableId newId) {
        return topicPrefix + newId.schema() + "." + newId.table();
    }

    private Headers addHeaders(Headers headers, TableId chunkId, TableId hypertableId, Optional<TableId> aggregateId) {
        headers.addString(HEADER_CHUNK_TABLE, chunkId.table());
        headers.addString(HEADER_CHUNK_SCHEMA, chunkId.schema());

        if (aggregateId.isPresent()) {
            headers.addString(HEADER_HYPERTABLE_TABLE, hypertableId.table());
            headers.addString(HEADER_HYPERTABLE_SCHEMA, hypertableId.schema());
        }
        return headers;
    }

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        Field.group(config, null, TimescaleDbConfigDefinition.SCHEMA_LIST_NAMES_FIELD, TimescaleDbConfigDefinition.TARGET_TOPIC_PREFIX_FIELD);
        return config;
    }

    @Override
    public void close() {
        try {
            metadata.close();
        }
        catch (IOException e) {
            LOGGER.warn("Exception while closing the metadata manager", e);
        }
    }

    @Override
    public String version() {
        return Module.version();
    }

    @VisibleForTesting
    void setMetadata(TimescaleDbMetadata metadata) {
        this.metadata = metadata;
    }
}
