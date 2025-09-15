/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.openlineage;

import static io.debezium.openlineage.dataset.DatasetMetadata.DatasetType.OUTPUT;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.Module;
import io.debezium.config.Configuration;
import io.debezium.connector.common.DebeziumTaskState;
import io.debezium.openlineage.ConnectorContext;
import io.debezium.openlineage.DebeziumOpenLineageEmitter;
import io.debezium.openlineage.dataset.DatasetDataExtractor;
import io.debezium.openlineage.dataset.DatasetMetadata;
import io.debezium.transforms.SmtManager;
import io.debezium.util.BoundedConcurrentHashMap;

public class OpenLineage<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenLineage.class);

    private static final int CACHE_SIZE = 64;

    private ZonedDateTime lastEmissionTime;
    private final BoundedConcurrentHashMap<String, Boolean> recentlySeenTopics = new BoundedConcurrentHashMap<>(CACHE_SIZE);
    private final BoundedConcurrentHashMap<Schema, Boolean> recentlySeenSchemas = new BoundedConcurrentHashMap<>(CACHE_SIZE);
    private SmtManager<R> smtManager;
    private DatasetDataExtractor datasetDataExtractor;

    @Override
    public ConfigDef config() {

        return new ConfigDef();
    }

    @Override
    public void configure(Map<String, ?> props) {

        final Configuration config = Configuration.from(props);
        smtManager = new SmtManager<>(config);
        datasetDataExtractor = new DatasetDataExtractor();
    }

    @Override
    public R apply(R record) {

        if (smtManager.isValidRecordForLineage(record)) {
            return record;
        }

        if (recentlySeenTopics.put(record.topic(), true) == null || recentlySeenSchemas.put(record.valueSchema(), true) == null) {

            if (recentlySeenSchemas.put(record.valueSchema(), true) == null) {

                List<DatasetMetadata.FieldDefinition> fieldDefinitions = datasetDataExtractor.extract(record);

                ConnectorContext connectorContext = ConnectorContext.from(record.headers());
                DebeziumOpenLineageEmitter.emit(connectorContext, DebeziumTaskState.RUNNING, List.of(new DatasetMetadata(record.topic(), OUTPUT, fieldDefinitions)));

                lastEmissionTime = ZonedDateTime.now();
            }

            LOGGER.debug("Emitting running event for output dataset {}", record.topic());

            return record;
        }

        return record;
    }

    @Override
    public void close() {
    }

    @Override
    public String version() {
        return Module.version();
    }
}
