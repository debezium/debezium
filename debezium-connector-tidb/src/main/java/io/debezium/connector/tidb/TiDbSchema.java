/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.data.Envelope;
import io.debezium.relational.TableId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * The schemas of the TiDB tables captured by the connector.
 * <p>
 * The connector does not have direct access to table metadata; the per-table key and row schemas
 * are learned (and refreshed when they change) from the typed TiCDC messages as they are
 * consumed, so this schema is not historized. Once snapshot support through TiDB's
 * MySQL-compatible endpoint is added, schemas discovered via JDBC will pre-populate this
 * registry.
 *
 * @author Aviral Srivastava
 */
@ThreadSafe
public class TiDbSchema implements DatabaseSchema<TableId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TiDbSchema.class);

    private final TopicNamingStrategy<TableId> topicNamingStrategy;
    private final Schema sourceSchema;
    private final SchemaNameAdjuster adjuster;
    private final ConcurrentMap<TableId, TiDbTableSchema> tables = new ConcurrentHashMap<>();

    public TiDbSchema(TopicNamingStrategy<TableId> topicNamingStrategy, Schema sourceSchema, SchemaNameAdjuster schemaNameAdjuster) {
        this.topicNamingStrategy = topicNamingStrategy;
        this.sourceSchema = sourceSchema;
        this.adjuster = schemaNameAdjuster;
    }

    @Override
    public void close() {
    }

    @Override
    public TiDbTableSchema schemaFor(TableId tableId) {
        return tables.get(tableId);
    }

    /**
     * Registers the schema of the given table, replacing a previously registered schema if the
     * key or row schema advertised by TiCDC has changed.
     *
     * @return the up-to-date schema of the table
     */
    public TiDbTableSchema refresh(TableId tableId, Schema keySchema, Schema rowSchema) {
        return tables.compute(tableId, (id, existing) -> {
            if (existing != null && existing.isCompatibleWith(keySchema, rowSchema)) {
                return existing;
            }
            if (existing != null) {
                LOGGER.info("Schema of table {} has changed, rebuilding envelope schema", id);
            }
            final String topicName = topicNamingStrategy.dataChangeTopic(id);
            final Envelope envelope = Envelope.defineSchema()
                    .withName(adjuster.adjust(Envelope.schemaName(topicName)))
                    .withRecord(rowSchema)
                    .withSource(sourceSchema)
                    .build();
            return new TiDbTableSchema(id, keySchema, rowSchema, envelope);
        });
    }

    @Override
    public boolean tableInformationComplete() {
        // Table schemas are learned dynamically from the TiCDC stream, nothing is recovered upfront
        return false;
    }

    @Override
    public void assureNonEmptySchema() {
        if (tables.isEmpty()) {
            LOGGER.warn(DatabaseSchema.NO_CAPTURED_DATA_COLLECTIONS_WARNING);
        }
    }

    @Override
    public boolean isHistorized() {
        return false;
    }

    public Set<TableId> tableIds() {
        return tables.keySet();
    }
}
