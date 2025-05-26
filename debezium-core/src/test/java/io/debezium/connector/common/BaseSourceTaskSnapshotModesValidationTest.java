/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.junit.Before;
import org.junit.Test;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.doc.FixFor;
import io.debezium.function.LogPositionValidator;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.HistorizedDatabaseSchema;
import io.debezium.spi.snapshot.Snapshotter;

public class BaseSourceTaskSnapshotModesValidationTest {

    private final MyBaseSourceTask baseSourceTask = new MyBaseSourceTask();

    @Before
    public void setup() {
        baseSourceTask.initialize(mock(SourceTaskContext.class));
    }

    @Test
    @FixFor("DBZ-7780")
    public void whenSnapshotModePermitsSchemaOrDataAndSnapshotIsNotCompletedOnConnectorRestartsValidateMustPass() {

        CommonConnectorConfig commonConnectorConfig = mock(CommonConnectorConfig.class);
        LogPositionValidator logPositionValidator = (partition, offsetContext, config) -> true;

        Partition partition = mock(Partition.class);
        OffsetContext offset = mock(OffsetContext.class);
        when(offset.isInitialSnapshotRunning()).thenReturn(true);

        Offsets previousOffsets = Offsets.of(partition, offset);
        DatabaseSchema databaseSchema = mock(DatabaseSchema.class);
        Snapshotter snapshotter = mock(Snapshotter.class);
        when(snapshotter.shouldSnapshotData(true, true)).thenReturn(false);
        when(snapshotter.shouldSnapshotSchema(true, true)).thenReturn(true);

        assertThatCode(() -> baseSourceTask.validateSchemaHistory(commonConnectorConfig, logPositionValidator, previousOffsets, databaseSchema, snapshotter))
                .doesNotThrowAnyException();

    }

    @Test
    public void whenSnapshotModeNotPermitsSchemaAndDataAndSnapshotIsNotCompletedOnConnectorRestartsExceptionWillBeThrown() {

        CommonConnectorConfig commonConnectorConfig = mock(CommonConnectorConfig.class);
        LogPositionValidator logPositionValidator = (partition, offsetContext, config) -> true;

        Partition partition = mock(Partition.class);
        OffsetContext offset = mock(OffsetContext.class);
        when(offset.isInitialSnapshotRunning()).thenReturn(true);

        Offsets previousOffsets = Offsets.of(partition, offset);
        DatabaseSchema databaseSchema = mock(DatabaseSchema.class);
        Snapshotter snapshotter = mock(Snapshotter.class);
        when(snapshotter.shouldSnapshotData(true, true)).thenReturn(false);
        when(snapshotter.shouldSnapshotSchema(true, true)).thenReturn(false);

        assertThatCode(() -> baseSourceTask.validateSchemaHistory(commonConnectorConfig, logPositionValidator, previousOffsets, databaseSchema, snapshotter))
                .isInstanceOf(DebeziumException.class)
                .hasMessage("The connector previously stopped while taking a snapshot, but now the connector is configured "
                        + "to never allow snapshots. Reconfigure the connector to use snapshots initially or when needed.");

    }

    @Test
    public void whenNoOffsetExistsAndSnapshotPermitsSchemaRecoveryAnExceptionWillBeThrown() {

        CommonConnectorConfig commonConnectorConfig = mock(CommonConnectorConfig.class);
        LogPositionValidator logPositionValidator = (partition, offsetContext, config) -> true;

        Partition partition = mock(Partition.class);

        Offsets previousOffsets = Offsets.of(partition, null);
        DatabaseSchema databaseSchema = mock(DatabaseSchema.class);
        Snapshotter snapshotter = mock(Snapshotter.class);

        when(snapshotter.shouldSnapshotOnSchemaError()).thenReturn(true);

        assertThatThrownBy(() -> baseSourceTask.validateSchemaHistory(commonConnectorConfig, logPositionValidator, previousOffsets, databaseSchema, snapshotter))
                .isInstanceOf(DebeziumException.class)
                .hasMessage("Could not find existing redo log information while attempting schema only recovery snapshot");

    }

    @Test
    public void whenNoOffsetExistsAndDatabaseIsHistorizedThenSchemaStorageIsInitialized() {

        CommonConnectorConfig commonConnectorConfig = mock(CommonConnectorConfig.class);
        LogPositionValidator logPositionValidator = (partition, offsetContext, config) -> true;

        Partition partition = mock(Partition.class);
        Offsets previousOffsets = Offsets.of(partition, null);

        HistorizedDatabaseSchema databaseSchema = mock(HistorizedDatabaseSchema.class);
        when(databaseSchema.isHistorized()).thenReturn(true);

        Snapshotter snapshotter = mock(Snapshotter.class);

        baseSourceTask.validateSchemaHistory(commonConnectorConfig, logPositionValidator, previousOffsets, databaseSchema, snapshotter);

        verify(databaseSchema).initializeStorage();
    }

    @Test
    public void whenCompletedSnapshotExistsAndHistoryNotExistsAndSnapshotOnSchemaErrorThenSchemaStorageIsInitialized() {

        CommonConnectorConfig commonConnectorConfig = mock(CommonConnectorConfig.class);
        LogPositionValidator logPositionValidator = (partition, offsetContext, config) -> true;

        Partition partition = mock(Partition.class);
        OffsetContext offset = mock(OffsetContext.class);
        when(offset.isInitialSnapshotRunning()).thenReturn(false);

        Offsets previousOffsets = Offsets.of(partition, offset);
        HistorizedDatabaseSchema databaseSchema = mock(HistorizedDatabaseSchema.class);
        when(databaseSchema.isHistorized()).thenReturn(true);
        Snapshotter snapshotter = mock(Snapshotter.class);
        when(snapshotter.shouldSnapshotOnSchemaError()).thenReturn(true);

        baseSourceTask.validateSchemaHistory(commonConnectorConfig, logPositionValidator, previousOffsets, databaseSchema, snapshotter);

        verify(databaseSchema).initializeStorage();

    }

    @Test
    public void whenCompletedSnapshotExistsAndHistoryNotExistsAndSnapshotOnSchemaErrorIsFalseThenAnExceptionWillBeThrown() {

        CommonConnectorConfig commonConnectorConfig = mock(CommonConnectorConfig.class);
        LogPositionValidator logPositionValidator = (partition, offsetContext, config) -> true;

        Partition partition = mock(Partition.class);
        OffsetContext offset = mock(OffsetContext.class);
        when(offset.isInitialSnapshotRunning()).thenReturn(false);

        Offsets previousOffsets = Offsets.of(partition, offset);
        HistorizedDatabaseSchema databaseSchema = mock(HistorizedDatabaseSchema.class);
        when(databaseSchema.isHistorized()).thenReturn(true);
        Snapshotter snapshotter = mock(Snapshotter.class);
        when(snapshotter.shouldSnapshotOnSchemaError()).thenReturn(false);

        assertThatThrownBy(() -> baseSourceTask.validateSchemaHistory(commonConnectorConfig, logPositionValidator, previousOffsets, databaseSchema, snapshotter))
                .isInstanceOf(DebeziumException.class)
                .hasMessage("The db history topic is missing. You may attempt to recover it by reconfiguring the connector to recovery.");

    }

    @Test
    public void whenCompletedSnapshotExistsAndStoredOffsetPositionIsPresentOnDbLogThenSchemaWillBeRecovered() {

        CommonConnectorConfig commonConnectorConfig = mock(CommonConnectorConfig.class);
        when(commonConnectorConfig.isLogPositionCheckEnabled()).thenReturn(true);
        LogPositionValidator logPositionValidator = (partition, offsetContext, config) -> true;

        Partition partition = mock(Partition.class);
        OffsetContext offset = mock(OffsetContext.class);
        when(offset.isInitialSnapshotRunning()).thenReturn(false);

        Offsets previousOffsets = Offsets.of(partition, offset);
        HistorizedDatabaseSchema databaseSchema = mock(HistorizedDatabaseSchema.class);
        when(databaseSchema.isHistorized()).thenReturn(true);
        when(databaseSchema.historyExists()).thenReturn(true);
        Snapshotter snapshotter = mock(Snapshotter.class);

        baseSourceTask.validateSchemaHistory(commonConnectorConfig, logPositionValidator, previousOffsets, databaseSchema, snapshotter);

    }

    @Test
    public void whenCompletedSnapshotExistsAndStoredOffsetPositionIsNotPresentOnDbLogThenAWarnShouldBeLogged() {

        LogInterceptor logInterceptor = new LogInterceptor(BaseSourceTask.class);

        CommonConnectorConfig commonConnectorConfig = mock(CommonConnectorConfig.class);
        when(commonConnectorConfig.isLogPositionCheckEnabled()).thenReturn(true);
        LogPositionValidator logPositionValidator = (partition, offsetContext, config) -> false;

        Partition partition = mock(Partition.class);
        OffsetContext offset = mock(OffsetContext.class);
        when(offset.isInitialSnapshotRunning()).thenReturn(false);

        Offsets previousOffsets = Offsets.of(partition, offset);
        HistorizedDatabaseSchema databaseSchema = mock(HistorizedDatabaseSchema.class);
        when(databaseSchema.isHistorized()).thenReturn(true);
        when(databaseSchema.historyExists()).thenReturn(true);
        Snapshotter snapshotter = mock(Snapshotter.class);

        baseSourceTask.validateSchemaHistory(commonConnectorConfig, logPositionValidator, previousOffsets, databaseSchema, snapshotter);

        assertThat(logInterceptor.containsWarnMessage("The connector is trying to read redo log starting at " + offset + ", but this is no longer "
                + "available on the server. Reconfigure the connector to use a snapshot when needed if you want to recover. " +
                "If not the connector will streaming from the last available position in the log")).isTrue();

    }

    @Test
    public void whenCompletedSnapshotExistsAndStoredOffsetPositionIsNotPresentOnDbLogAndSnapshotOnDataErrorThenOffsetWillBeReset() {

        CommonConnectorConfig commonConnectorConfig = mock(CommonConnectorConfig.class);
        when(commonConnectorConfig.isLogPositionCheckEnabled()).thenReturn(true);
        LogPositionValidator logPositionValidator = (partition, offsetContext, config) -> false;

        Partition partition = mock(Partition.class);
        OffsetContext offset = mock(OffsetContext.class);
        when(offset.isInitialSnapshotRunning()).thenReturn(false);

        Offsets previousOffsets = Offsets.of(partition, offset);
        HistorizedDatabaseSchema databaseSchema = mock(HistorizedDatabaseSchema.class);
        when(databaseSchema.isHistorized()).thenReturn(true);
        when(databaseSchema.historyExists()).thenReturn(true);
        Snapshotter snapshotter = mock(Snapshotter.class);
        when(snapshotter.shouldSnapshotOnDataError()).thenReturn(true);

        baseSourceTask.validateSchemaHistory(commonConnectorConfig, logPositionValidator, previousOffsets, databaseSchema, snapshotter);

        assertThat(previousOffsets.getTheOnlyOffset()).isNull();

    }

    public static class MyBaseSourceTask extends BaseSourceTask<Partition, OffsetContext> {
        final List<SourceRecord> records = new ArrayList<>();
        final AtomicInteger startCount = new AtomicInteger();
        final AtomicInteger stopCount = new AtomicInteger();

        @SuppressWarnings("unchecked")
        final ChangeEventSourceCoordinator<Partition, OffsetContext> coordinator = mock(ChangeEventSourceCoordinator.class);

        @Override
        protected ChangeEventSourceCoordinator<Partition, OffsetContext> start(Configuration config) {
            startCount.incrementAndGet();
            return coordinator;
        }

        @Override
        protected List<SourceRecord> doPoll() {
            return records;
        }

        @Override
        protected void resetErrorHandlerRetriesIfNeeded(List<SourceRecord> records) {
            // do nothing as we don't have a coordinator mocked
        }

        @Override
        protected void doStop() {
            stopCount.incrementAndGet();
        }

        @Override
        protected Iterable<Field> getAllConfigurationFields() {
            return List.of(Field.create("f1"));
        }

        @Override
        public String version() {
            return "1.0";
        }
    }
}
