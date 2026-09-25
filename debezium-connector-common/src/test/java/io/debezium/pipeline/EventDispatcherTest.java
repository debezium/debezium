/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.StreamSupport;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.heartbeat.Heartbeat.ScheduledHeartbeat;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.signal.channels.SourceSignalChannel;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.ChangeEventCreator;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.txmetadata.TransactionStructMaker;
import io.debezium.pipeline.txmetadata.spi.TransactionMetadataFactory;
import io.debezium.processors.PostProcessorRegistry;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.SnapshotChangeRecordEmitter;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.schema.DataCollectionFilters;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.service.spi.ServiceRegistry;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;

@ExtendWith(MockitoExtension.class)
public class EventDispatcherTest {

    @Mock
    private Partition partition;

    @Mock
    private DataCollectionId dataCollectionId;

    @Mock
    private RelationalDatabaseConnectorConfig config;

    @Mock
    private TopicNamingStrategy<DataCollectionId> topicNamingStrategy;

    @Mock
    private DatabaseSchema<DataCollectionId> databaseSchema;

    @Mock
    private ChangeEventQueue<DataChangeEvent> changeEventQueue;

    @Mock
    private DataCollectionFilters.DataCollectionFilter<DataCollectionId> dataCollectionFilters;

    @Mock
    private ChangeEventCreator changeEventCreator;

    @Mock
    private EventMetadataProvider eventMetadataProvider;

    @Mock
    private SchemaNameAdjuster schemaNameAdjuster;

    @Mock
    private SignalProcessor<Partition, ?> signalProcessor;

    @Mock
    private CdcSourceTaskContext cdcSourceTaskContext;

    @Mock
    private TransactionStructMaker transactionStructMaker;

    @Mock
    private TransactionMetadataFactory transactionMetadataFactory;

    @Mock
    private SourceSignalChannel sourceSignalChannel;

    @Mock
    private SourceInfoStructMaker<AbstractSourceInfo> sourceInfoStructMaker;

    @Mock
    private Schema schema;

    @Mock
    private ServiceRegistry serviceRegistry;

    @Mock
    private PostProcessorRegistry postProcessorRegistry;

    @Mock
    private TableSchema dataCollectionSchema;

    @Mock
    private OffsetContext offsetContext;

    @Mock
    private Envelope envelope;

    @Mock
    private Struct struct;

    @Mock
    private DataChangeEventListener<Partition> dataChangeEventListener;

    @Captor
    ArgumentCaptor<SourceRecord> sourceRecordCaptor;

    private EventDispatcher<Partition, DataCollectionId> dispatcher;
    private static ConnectHeaders connectHeaders;

    @AfterEach
    public void tearDown() throws Exception {
        if (connectHeaders != null) {
            connectHeaders.clear();
        }
    }

    @Test
    public void dispatchEventWithExtendedHeaders() throws InterruptedException {

        DebeziumHeaderProducer debeziumHeaderProducer = new DebeziumHeaderProducer(cdcSourceTaskContext);
        when(dataCollectionSchema.getEnvelopeSchema()).thenReturn(envelope);
        when(envelope.read(any(), any(), any())).thenReturn(struct);
        when(databaseSchema.schemaFor(any())).thenReturn(dataCollectionSchema);
        when(config.getServiceRegistry()).thenReturn(serviceRegistry);
        when(serviceRegistry.tryGetService(PostProcessorRegistry.class)).thenReturn(postProcessorRegistry);
        when(config.getSourceInfoStructMaker()).thenReturn(sourceInfoStructMaker);
        when(sourceInfoStructMaker.schema()).thenReturn(schema);
        when(config.supportsOperationFiltering()).thenReturn(true);
        when(signalProcessor.getSignalChannel(any())).thenReturn(sourceSignalChannel);
        when(config.getTransactionMetadataFactory()).thenReturn(transactionMetadataFactory);
        when(config.getTransactionMetadataFactory().getTransactionStructMaker()).thenReturn(transactionStructMaker);

        when(config.isExtendedHeadersEnabled()).thenReturn(true);
        when(cdcSourceTaskContext.getTaskId()).thenReturn("0");
        when(cdcSourceTaskContext.getConnectorLogicalName()).thenReturn("test");
        when(cdcSourceTaskContext.getConnectorPluginName()).thenReturn("plugin");
        when(cdcSourceTaskContext.getRunId()).thenReturn(UUID.fromString("671711f6-5517-4c69-806b-87c1b034ee7b"));

        dispatcher = new EventDispatcher<>(config, topicNamingStrategy, databaseSchema, changeEventQueue, dataCollectionFilters, changeEventCreator,
                eventMetadataProvider,
                schemaNameAdjuster, signalProcessor, debeziumHeaderProducer);

        Object[] data = new Object[]{ "col1", "col2" };
        SnapshotChangeRecordEmitter<Partition> changeRecordEmitter = new SnapshotChangeRecordEmitter<>(partition, offsetContext, data, Clock.SYSTEM, config);

        dispatcher.dispatchSnapshotEvent(partition, dataCollectionId, changeRecordEmitter, new PartitionSnapshotReceiver());

        assertThat(connectHeaders).isNotEmpty();
    }

    @Test
    @FixFor("DBZ-9422")
    public void dispatchIncrementalSnapshotShouldNotProduceDuplicateHeaders() throws InterruptedException {

        DebeziumHeaderProducer debeziumHeaderProducer = new DebeziumHeaderProducer(cdcSourceTaskContext);
        when(dataCollectionSchema.getEnvelopeSchema()).thenReturn(envelope);
        when(envelope.read(any(), any(), any())).thenReturn(struct);
        when(databaseSchema.schemaFor(any())).thenReturn(dataCollectionSchema);
        when(config.getServiceRegistry()).thenReturn(serviceRegistry);
        when(serviceRegistry.tryGetService(PostProcessorRegistry.class)).thenReturn(postProcessorRegistry);
        when(config.getSourceInfoStructMaker()).thenReturn(sourceInfoStructMaker);
        when(sourceInfoStructMaker.schema()).thenReturn(schema);
        when(config.supportsOperationFiltering()).thenReturn(true);
        when(signalProcessor.getSignalChannel(any())).thenReturn(sourceSignalChannel);
        when(config.getTransactionMetadataFactory()).thenReturn(transactionMetadataFactory);
        when(config.getTransactionMetadataFactory().getTransactionStructMaker()).thenReturn(transactionStructMaker);

        when(config.isExtendedHeadersEnabled()).thenReturn(true);
        when(cdcSourceTaskContext.getTaskId()).thenReturn("0");
        when(cdcSourceTaskContext.getConnectorLogicalName()).thenReturn("test");
        when(cdcSourceTaskContext.getConnectorPluginName()).thenReturn("plugin");
        when(cdcSourceTaskContext.getRunId()).thenReturn(UUID.fromString("671711f6-5517-4c69-806b-87c1b034ee7b"));

        dispatcher = new EventDispatcher<>(config, topicNamingStrategy, databaseSchema, changeEventQueue, dataCollectionFilters, changeEventCreator,
                eventMetadataProvider,
                schemaNameAdjuster, signalProcessor, debeziumHeaderProducer);

        Object[] data = new Object[]{ "col1", "col2" };
        SnapshotChangeRecordEmitter<Partition> changeRecordEmitter = new SnapshotChangeRecordEmitter<>(partition, offsetContext, data, Clock.SYSTEM, config);

        EventDispatcher.SnapshotReceiver<Partition> incrementalSnapshotChangeEventReceiver = dispatcher.getIncrementalSnapshotChangeEventReceiver(
                dataChangeEventListener);
        dispatcher.dispatchSnapshotEvent(partition, dataCollectionId, changeRecordEmitter, incrementalSnapshotChangeEventReceiver);

        verify(changeEventCreator).createDataChangeEvent(sourceRecordCaptor.capture());

        List<String> listOfHeaders = StreamSupport.stream(sourceRecordCaptor.getValue().headers().spliterator(), false)
                .map(Header::key)
                .toList();

        assertEquals(listOfHeaders.stream().distinct().count(), listOfHeaders.size(), "Header must not be duplicated");
    }

    @Test
    public void dispatchEventWithoutExtendedHeaders() throws InterruptedException {

        DebeziumHeaderProducer debeziumHeaderProducer = new DebeziumHeaderProducer(cdcSourceTaskContext);
        when(dataCollectionSchema.getEnvelopeSchema()).thenReturn(envelope);
        when(envelope.read(any(), any(), any())).thenReturn(struct);
        when(databaseSchema.schemaFor(any())).thenReturn(dataCollectionSchema);
        when(config.getServiceRegistry()).thenReturn(serviceRegistry);
        when(serviceRegistry.tryGetService(PostProcessorRegistry.class)).thenReturn(postProcessorRegistry);
        when(config.getSourceInfoStructMaker()).thenReturn(sourceInfoStructMaker);
        when(sourceInfoStructMaker.schema()).thenReturn(schema);
        when(config.supportsOperationFiltering()).thenReturn(true);
        when(signalProcessor.getSignalChannel(any())).thenReturn(sourceSignalChannel);
        when(config.getTransactionMetadataFactory()).thenReturn(transactionMetadataFactory);
        when(config.getTransactionMetadataFactory().getTransactionStructMaker()).thenReturn(transactionStructMaker);

        when(config.isExtendedHeadersEnabled()).thenReturn(false);

        dispatcher = new EventDispatcher<>(config, topicNamingStrategy, databaseSchema, changeEventQueue, dataCollectionFilters, changeEventCreator,
                eventMetadataProvider,
                schemaNameAdjuster, signalProcessor, debeziumHeaderProducer);

        Object[] data = new Object[]{ "col1", "col2" };
        SnapshotChangeRecordEmitter<Partition> changeRecordEmitter = new SnapshotChangeRecordEmitter<>(partition, offsetContext, data, Clock.SYSTEM, config);

        dispatcher.dispatchSnapshotEvent(partition, dataCollectionId, changeRecordEmitter, new PartitionSnapshotReceiver());

        assertThat(connectHeaders).isNull();
    }

    @Test
    @FixFor("debezium/dbz#2549")
    public void shouldAdvanceOffsetOnlyAfterDeleteTombstone() throws InterruptedException {
        initializeStreamingDispatcher();

        final Map<String, Object> offsetBeforeEvent = Map.of("position", 1L);
        final Map<String, Object> currentOffset = Map.of("position", 2L);
        doReturn(offsetBeforeEvent).when(offsetContext).getOffsetForIncompleteEvent();
        doReturn(currentOffset).when(offsetContext).getOffset();

        dispatcher.dispatchDataChangeEvent(partition, dataCollectionId,
                changeRecordEmitter((schema, receiver) -> receiver.changeRecord(
                        partition, schema, Envelope.Operation.DELETE, "key", struct, offsetContext, new ConnectHeaders(), true),
                        Envelope.Operation.DELETE));

        verify(changeEventCreator, times(2)).createDataChangeEvent(sourceRecordCaptor.capture());
        assertThat(sourceRecordCaptor.getAllValues())
                .extracting(SourceRecord::sourceOffset)
                .containsExactly(offsetBeforeEvent, currentOffset);
        assertThat(sourceRecordCaptor.getAllValues())
                .extracting(SourceRecord::value)
                .containsExactly(struct, null);
    }

    @Test
    @FixFor("debezium/dbz#2549")
    public void shouldAdvanceOffsetOnlyAfterPrimaryKeyUpdateRecords() throws InterruptedException {
        initializeStreamingDispatcher(true);

        final Map<String, Object> offsetBeforeEvent = Map.of("position", 1L);
        final Map<String, Object> currentOffset = Map.of("position", 2L);
        doReturn(offsetBeforeEvent).when(offsetContext).getOffsetForIncompleteEvent();
        doReturn(currentOffset).when(offsetContext).getOffset();

        dispatcher.dispatchDataChangeEvent(partition, dataCollectionId,
                changeRecordEmitter((schema, receiver) -> {
                    receiver.changeRecord(partition, schema, Envelope.Operation.DELETE, "old-key", struct, offsetContext, new ConnectHeaders(), false);
                    receiver.changeRecord(partition, schema, Envelope.Operation.CREATE, "new-key", struct, offsetContext, new ConnectHeaders(), true);
                }, Envelope.Operation.UPDATE));

        verify(changeEventCreator, times(3)).createDataChangeEvent(sourceRecordCaptor.capture());
        assertThat(sourceRecordCaptor.getAllValues())
                .extracting(SourceRecord::sourceOffset)
                .containsExactly(offsetBeforeEvent, offsetBeforeEvent, currentOffset);
        assertThat(sourceRecordCaptor.getAllValues())
                .extracting(SourceRecord::key)
                .containsExactly("old-key", "old-key", "new-key");
        assertThat(sourceRecordCaptor.getAllValues())
                .extracting(SourceRecord::value)
                .containsExactly(struct, null, struct);
    }

    @Test
    @FixFor("debezium/dbz#2549")
    public void shouldAdvanceOffsetOnlyAfterPrimaryKeyUpdateWhenTombstonesAreDisabled() throws InterruptedException {
        initializeStreamingDispatcher(false);

        final Map<String, Object> offsetBeforeEvent = Map.of("position", 1L);
        final Map<String, Object> currentOffset = Map.of("position", 2L);
        doReturn(offsetBeforeEvent).when(offsetContext).getOffsetForIncompleteEvent();
        doReturn(currentOffset).when(offsetContext).getOffset();

        dispatcher.dispatchDataChangeEvent(partition, dataCollectionId,
                changeRecordEmitter((schema, receiver) -> {
                    receiver.changeRecord(partition, schema, Envelope.Operation.DELETE, "old-key", struct, offsetContext, new ConnectHeaders(), false);
                    receiver.changeRecord(partition, schema, Envelope.Operation.CREATE, "new-key", struct, offsetContext, new ConnectHeaders(), true);
                }, Envelope.Operation.UPDATE));

        verify(changeEventCreator, times(2)).createDataChangeEvent(sourceRecordCaptor.capture());
        assertThat(sourceRecordCaptor.getAllValues())
                .extracting(SourceRecord::sourceOffset)
                .containsExactly(offsetBeforeEvent, currentOffset);
        assertThat(sourceRecordCaptor.getAllValues())
                .extracting(SourceRecord::key)
                .containsExactly("old-key", "new-key");
        assertThat(sourceRecordCaptor.getAllValues())
                .extracting(SourceRecord::value)
                .containsExactly(struct, struct);
    }

    private void initializeStreamingDispatcher() {
        initializeStreamingDispatcher(true);
    }

    private void initializeStreamingDispatcher(boolean emitTombstonesOnDelete) {
        final TableId tableId = new TableId(null, null, "table");
        when(dataCollectionSchema.getEnvelopeSchema()).thenReturn(envelope);
        when(dataCollectionSchema.keySchema()).thenReturn(schema);
        when(dataCollectionSchema.id()).thenReturn(tableId);
        when(envelope.schema()).thenReturn(schema);
        when(databaseSchema.schemaFor(dataCollectionId)).thenReturn(dataCollectionSchema);
        when(databaseSchema.isHistorized()).thenReturn(false);
        when(dataCollectionFilters.isIncluded(dataCollectionId)).thenReturn(true);
        when(partition.getSourcePartition()).thenReturn(Map.of("server", "test"));
        when(topicNamingStrategy.dataChangeTopic(tableId)).thenReturn("server.table");
        when(topicNamingStrategy.transactionTopic()).thenReturn("server.transaction");
        when(config.isEmitTombstoneOnDelete()).thenReturn(emitTombstonesOnDelete);
        when(config.getSkippedOperations()).thenReturn(EnumSet.noneOf(Envelope.Operation.class));
        when(config.supportsOperationFiltering()).thenReturn(true);
        when(config.getServiceRegistry()).thenReturn(serviceRegistry);
        when(serviceRegistry.tryGetService(PostProcessorRegistry.class)).thenReturn(postProcessorRegistry);
        when(config.getSourceInfoStructMaker()).thenReturn(sourceInfoStructMaker);
        when(sourceInfoStructMaker.schema()).thenReturn(schema);
        when(config.getTransactionMetadataFactory()).thenReturn(transactionMetadataFactory);
        when(transactionMetadataFactory.getTransactionStructMaker()).thenReturn(transactionStructMaker);
        when(changeEventCreator.createDataChangeEvent(any())).thenAnswer(invocation -> new DataChangeEvent(invocation.getArgument(0)));

        dispatcher = new EventDispatcher<>(config, topicNamingStrategy, databaseSchema, changeEventQueue, dataCollectionFilters, changeEventCreator,
                eventMetadataProvider, ScheduledHeartbeat.NOOP_HEARTBEAT, SchemaNameAdjuster.NO_OP, null, null);
    }

    private ChangeRecordEmitter<Partition> changeRecordEmitter(RecordEmission emission, Envelope.Operation operation) {
        return new ChangeRecordEmitter<>() {
            @Override
            public void emitChangeRecords(DataCollectionSchema schema, Receiver<Partition> receiver) throws InterruptedException {
                emission.emit(schema, receiver);
            }

            @Override
            public Partition getPartition() {
                return partition;
            }

            @Override
            public OffsetContext getOffset() {
                return offsetContext;
            }

            @Override
            public Envelope.Operation getOperation() {
                return operation;
            }
        };
    }

    @FunctionalInterface
    private interface RecordEmission {
        void emit(DataCollectionSchema schema, ChangeRecordEmitter.Receiver<Partition> receiver) throws InterruptedException;
    }

    private static class PartitionSnapshotReceiver implements EventDispatcher.SnapshotReceiver<Partition> {

        @Override
        public void completeSnapshot() throws InterruptedException {

        }

        @Override
        public void changeRecord(Partition partition, DataCollectionSchema schema, Envelope.Operation operation, Object key, Struct value, OffsetContext offset,
                                 ConnectHeaders headers)
                throws InterruptedException {

            connectHeaders = headers;
        }
    }
}
