/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.stream.StreamSupport;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.signal.channels.SourceSignalChannel;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.ChangeEventCreator;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.txmetadata.TransactionStructMaker;
import io.debezium.pipeline.txmetadata.spi.TransactionMetadataFactory;
import io.debezium.processors.PostProcessorRegistry;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.SnapshotChangeRecordEmitter;
import io.debezium.relational.TableSchema;
import io.debezium.schema.DataCollectionFilters;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.service.spi.ServiceRegistry;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;

@RunWith(MockitoJUnitRunner.class)
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

    @After
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

        Assert.assertEquals("Header must not be duplicated", listOfHeaders.stream().distinct().count(), listOfHeaders.size());
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
