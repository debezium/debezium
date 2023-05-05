/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal.actions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.document.Array;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.TableId;
import io.debezium.relational.history.JsonTableChangeSerializer;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChangeType;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.spi.schema.DataCollectionId;

public class SchemaChanges<P extends Partition> implements SignalAction<P> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaChanges.class);

    public static final String NAME = "schema-changes";

    public static final String FIELD_CHANGES = "changes";
    public static final String FIELD_DATABASE = "database";
    public static final String FIELD_SCHEMA = "schema";

    private final JsonTableChangeSerializer serializer;
    private final boolean useCatalogBeforeSchema;
    private final EventDispatcher<P, TableId> dispatcher;

    @SuppressWarnings("unchecked")
    public SchemaChanges(EventDispatcher<P, ? extends DataCollectionId> dispatcher, boolean useCatalogBeforeSchema) {
        serializer = new JsonTableChangeSerializer();
        this.useCatalogBeforeSchema = useCatalogBeforeSchema;
        this.dispatcher = (EventDispatcher<P, TableId>) dispatcher;
    }

    @Override
    public boolean arrived(SignalPayload<P> signalPayload) throws InterruptedException {
        final Array changes = signalPayload.data.getArray(FIELD_CHANGES);
        final String database = signalPayload.data.getString(FIELD_DATABASE);
        final String schema = signalPayload.data.getString(FIELD_SCHEMA);

        if (changes == null || changes.isEmpty()) {
            LOGGER.warn("Table changes signal '{}' has arrived but the requested field '{}' is missing from data", signalPayload, FIELD_CHANGES);
            return false;
        }
        if (database == null || database.isEmpty()) {
            LOGGER.warn("Table changes signal '{}' has arrived but the requested field '{}' is missing from data", signalPayload, FIELD_DATABASE);
            return false;
        }
        for (TableChanges.TableChange tableChange : serializer.deserialize(changes, useCatalogBeforeSchema)) {
            if (dispatcher.getHistorizedSchema() != null) {
                LOGGER.info("Executing schema change for table '{}' requested by signal '{}'", tableChange.getId(), signalPayload.id);
                dispatcher.dispatchSchemaChangeEvent(signalPayload.partition, tableChange.getId(), emitter -> {
                    emitter.schemaChangeEvent(SchemaChangeEvent.ofTableChange(
                            tableChange,
                            signalPayload.partition.getSourcePartition(),
                            signalPayload.offsetContext.getOffset(),
                            signalPayload.offsetContext.getSourceInfo(),
                            database,
                            schema));
                });
            }
            else if (dispatcher.getSchema() instanceof RelationalDatabaseSchema) {
                LOGGER.info("Executing schema change for table '{}' requested by signal '{}'", tableChange.getId(), signalPayload.id);
                final RelationalDatabaseSchema databaseSchema = (RelationalDatabaseSchema) dispatcher.getSchema();
                if (tableChange.getType() == TableChangeType.CREATE || tableChange.getType() == TableChangeType.ALTER) {
                    databaseSchema.refresh(tableChange.getTable());
                }
            }
        }
        return true;
    }
}
