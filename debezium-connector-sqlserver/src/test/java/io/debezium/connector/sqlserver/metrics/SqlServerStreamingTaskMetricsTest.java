/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.connector.sqlserver.SqlServerPartition;
import io.debezium.doc.FixFor;
import io.debezium.embedded.util.MetricsHelper;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;

/**
 * Unit tests for {@link SqlServerStreamingTaskMetrics}.
 */
public class SqlServerStreamingTaskMetricsTest {

    private final ChangeEventQueueMetrics queueMetrics = new ChangeEventQueueMetrics() {
        @Override
        public int totalCapacity() {
            return 0;
        }

        @Override
        public int remainingCapacity() {
            return 0;
        }

        @Override
        public long maxQueueSizeInBytes() {
            return 0;
        }

        @Override
        public long currentQueueSizeInBytes() {
            return 0;
        }
    };

    private final EventMetadataProvider metadataProvider = new EventMetadataProvider() {
        @Override
        public Instant getEventTimestamp(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            return null;
        }

        @Override
        public Map<String, String> getEventSourcePosition(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            return null;
        }

        @Override
        public String getTransactionId(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            return null;
        }
    };

    @Test
    @FixFor("debezium/dbz#586")
    public void shouldExposeOnlyPartitionSpecificCapturedTablesViaJmxInMultiPartitionMode() {
        final CdcSourceTaskContext<SqlServerConnectorConfig> taskContext = createTaskContext("server1", "0", "db1,db2");
        final SqlServerPartition partition1 = new SqlServerPartition("server1", "db1");
        final SqlServerPartition partition2 = new SqlServerPartition("server1", "db2");

        final List<TableId> allTables = List.of(
                new TableId("db1", "dbo", "tableA"),
                new TableId("db1", "dbo", "tableB"),
                new TableId("db2", "dbo", "tableA"),
                new TableId("db2", "dbo", "tableC"));

        final SqlServerStreamingTaskMetrics taskMetrics = new SqlServerStreamingTaskMetrics(
                taskContext,
                queueMetrics,
                metadataProvider,
                List.of(partition1, partition2),
                () -> allTables);

        try {
            taskMetrics.register();

            final String[] db1Tables = MetricsHelper.getStreamingMetric("sql_server", "server1", "streaming", "0", "db1", "CapturedTables");
            final String[] db2Tables = MetricsHelper.getStreamingMetric("sql_server", "server1", "streaming", "0", "db2", "CapturedTables");

            assertThat(db1Tables).containsExactlyInAnyOrder("db1.dbo.tableA", "db1.dbo.tableB");
            assertThat(db2Tables).containsExactlyInAnyOrder("db2.dbo.tableA", "db2.dbo.tableC");
        }
        finally {
            taskMetrics.unregister();
        }
    }

    @Test
    @FixFor("debezium/dbz#586")
    public void shouldFilterCapturedTablesToPartitionInMultiPartitionMode() {
        final SqlServerPartition partition1 = new SqlServerPartition("server1", "db1");
        final SqlServerPartition partition2 = new SqlServerPartition("server1", "db2");

        final List<TableId> allTables = List.of(
                new TableId("db1", "dbo", "tableA"),
                new TableId("db1", "dbo", "tableB"),
                new TableId("db2", "dbo", "tableA"),
                new TableId("db2", "dbo", "tableC"));

        final CapturedTablesSupplier supplier1 = SqlServerStreamingTaskMetrics.scopedTo(() -> allTables, partition1);
        final CapturedTablesSupplier supplier2 = SqlServerStreamingTaskMetrics.scopedTo(() -> allTables, partition2);

        assertThat(supplier1.getCapturedTables())
                .extracting(DataCollectionId::toString)
                .containsExactlyInAnyOrder("db1.dbo.tableA", "db1.dbo.tableB");
        assertThat(supplier2.getCapturedTables())
                .extracting(DataCollectionId::toString)
                .containsExactlyInAnyOrder("db2.dbo.tableA", "db2.dbo.tableC");
    }

    @Test
    @FixFor("debezium/dbz#586")
    public void shouldExposeCapturedTablesInSinglePartitionMode() {
        final SqlServerPartition partition = new SqlServerPartition("server1", "db1", false);

        final List<TableId> tables = List.of(
                new TableId("db1", "dbo", "tableA"),
                new TableId("db1", "dbo", "tableB"),
                new TableId("otherDb", "dbo", "tableC"));

        final CapturedTablesSupplier supplier = SqlServerStreamingTaskMetrics.scopedTo(() -> tables, partition);

        assertThat(supplier.getCapturedTables())
                .extracting(DataCollectionId::toString)
                .containsExactlyInAnyOrder("db1.dbo.tableA", "db1.dbo.tableB");
    }

    @Test
    @FixFor("debezium/dbz#586")
    public void shouldHandleCaseInsensitiveDatabaseNames() {
        final SqlServerPartition partition = new SqlServerPartition("server1", "DB1");

        final List<TableId> tables = List.of(
                new TableId("db1", "dbo", "tableA"),
                new TableId("db2", "dbo", "tableB"));

        final CapturedTablesSupplier supplier = SqlServerStreamingTaskMetrics.scopedTo(() -> tables, partition);

        assertThat(supplier.getCapturedTables())
                .extracting(DataCollectionId::toString)
                .containsExactlyInAnyOrder("db1.dbo.tableA");
    }

    @Test
    @FixFor("debezium/dbz#586")
    public void shouldHandleNullOrEmptyCapturedTablesSupplier() {
        final SqlServerPartition partition = new SqlServerPartition("server1", "db1");

        final CapturedTablesSupplier nullSupplier = SqlServerStreamingTaskMetrics.scopedTo(null, partition);
        assertThat(nullSupplier.getCapturedTables()).isEmpty();

        final CapturedTablesSupplier emptySupplier = SqlServerStreamingTaskMetrics.scopedTo(Collections::emptyList, partition);
        assertThat(emptySupplier.getCapturedTables()).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#586")
    public void shouldIgnoreNonTableIdOrNullCatalog() {
        final SqlServerPartition partition = new SqlServerPartition("server1", "db1");

        final DataCollectionId nonTableId = new DataCollectionId() {
            @Override
            public String identifier() {
                return "custom_id";
            }

            @Override
            public List<String> parts() {
                return Collections.emptyList();
            }

            @Override
            public List<String> databaseParts() {
                return Collections.emptyList();
            }

            @Override
            public List<String> schemaParts() {
                return Collections.emptyList();
            }
        };

        final List<DataCollectionId> tables = List.of(
                new TableId(null, "dbo", "tableNoCatalog"),
                new TableId("db1", "dbo", "tableA"),
                nonTableId);

        final CapturedTablesSupplier supplier = SqlServerStreamingTaskMetrics.scopedTo(() -> tables, partition);

        assertThat(supplier.getCapturedTables())
                .extracting(DataCollectionId::toString)
                .containsExactlyInAnyOrder("db1.dbo.tableA");
    }

    private CdcSourceTaskContext<SqlServerConnectorConfig> createTaskContext(String serverName, String taskId, String databaseNames) {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, serverName)
                .with(SqlServerConnectorConfig.HOSTNAME, "localhost")
                .with(SqlServerConnectorConfig.USER, "debezium")
                .with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, "localhost:9092")
                .with(KafkaSchemaHistory.TOPIC, "history")
                .with(SqlServerConnectorConfig.DATABASE_NAMES, databaseNames)
                .build();
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(config);
        return new CdcSourceTaskContext<>(config, connectorConfig, taskId, Collections.emptyMap());
    }
}
