/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.txmetadata;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.EnumSet;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;
import org.mockito.Mockito;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.function.BlockingConsumer;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;

public class TransactionMonitorTest {

    @Test
    public void shouldSendTransactionTopicBeginMessage() throws InterruptedException {
        CommonConnectorConfig config = mock(CommonConnectorConfig.class);
        when(config.shouldProvideTransactionMetadata()).thenReturn(true);
        when(config.getExcludedTransactionMetadataComponents()).thenReturn(
                EnumSet.of(CommonConnectorConfig.TransactionMetadataComponent.ID));
        when(config.getTransactionMetadataFactory()).thenReturn(new DefaultTransactionMetadataFactory(Configuration.empty()));

        String expectedId = "tx_id";
        TransactionInfo transactionInfo = new DefaultTransactionInfo(expectedId);

        BlockingConsumer<SourceRecord> sender = mock(BlockingConsumer.class);

        EventMetadataProvider metadataProvider = mock(EventMetadataProvider.class);
        when(metadataProvider.getTransactionId(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(expectedId);
        when(metadataProvider.getTransactionInfo(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(transactionInfo);
        when(metadataProvider.getEventTimestamp(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(Instant.EPOCH);

        TransactionMonitor monitor = new TransactionMonitor(config, metadataProvider, null, sender, null);

        Partition partition = Mockito.mock(Partition.class);
        DataCollectionId dataCollectionId = Mockito.mock(DataCollectionId.class);
        Struct struct = mock(Struct.class);

        OffsetContext offsetContext = mock(OffsetContext.class);
        TransactionContext transactionContext = new TransactionContext();
        when(offsetContext.getTransactionContext()).thenReturn(transactionContext);

        monitor.dataEvent(partition, dataCollectionId, offsetContext, "key", struct);
        verify(sender, Mockito.times(1)).accept(Mockito.any());
    }

    @Test
    public void shouldNotSendTransactionTopicBeginMessage() throws InterruptedException {
        CommonConnectorConfig config = mock(CommonConnectorConfig.class);
        when(config.shouldProvideTransactionMetadata()).thenReturn(true);
        when(config.getExcludedTransactionMetadataComponents()).thenReturn(
                EnumSet.of(CommonConnectorConfig.TransactionMetadataComponent.TRANSACTION_TOPIC));
        when(config.getTransactionMetadataFactory()).thenReturn(new DefaultTransactionMetadataFactory(Configuration.empty()));

        String expectedId = "tx_id";
        TransactionInfo transactionInfo = new DefaultTransactionInfo(expectedId);

        BlockingConsumer<SourceRecord> sender = mock(BlockingConsumer.class);

        EventMetadataProvider metadataProvider = mock(EventMetadataProvider.class);
        when(metadataProvider.getTransactionId(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(expectedId);
        when(metadataProvider.getTransactionInfo(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(transactionInfo);
        when(metadataProvider.getEventTimestamp(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(Instant.EPOCH);

        TransactionMonitor monitor = new TransactionMonitor(config, metadataProvider, null, sender, null);

        Partition partition = Mockito.mock(Partition.class);
        DataCollectionId dataCollectionId = Mockito.mock(DataCollectionId.class);
        Struct struct = mock(Struct.class);

        OffsetContext offsetContext = mock(OffsetContext.class);
        TransactionContext transactionContext = new TransactionContext();
        when(offsetContext.getTransactionContext()).thenReturn(transactionContext);

        monitor.dataEvent(partition, dataCollectionId, offsetContext, "key", struct);
        verify(sender, never()).accept(Mockito.any());
    }

    @Test
    public void shouldNotSendTransactionTopicEndMessage() throws InterruptedException {
        CommonConnectorConfig config = mock(CommonConnectorConfig.class);
        when(config.shouldProvideTransactionMetadata()).thenReturn(true);
        when(config.getExcludedTransactionMetadataComponents()).thenReturn(
                EnumSet.of(CommonConnectorConfig.TransactionMetadataComponent.TRANSACTION_TOPIC));
        when(config.getTransactionMetadataFactory()).thenReturn(new DefaultTransactionMetadataFactory(Configuration.empty()));

        String txId1 = "tx_id1";
        String txId2 = "tx_id2";
        TransactionInfo transactionInfo = new DefaultTransactionInfo(txId1);

        BlockingConsumer<SourceRecord> sender = mock(BlockingConsumer.class);

        EventMetadataProvider metadataProvider = mock(EventMetadataProvider.class);
        when(metadataProvider.getTransactionId(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(txId1);
        when(metadataProvider.getTransactionInfo(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(transactionInfo);
        when(metadataProvider.getEventTimestamp(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(Instant.EPOCH);

        TransactionMonitor monitor = new TransactionMonitor(config, metadataProvider, null, sender, null);

        Partition partition = Mockito.mock(Partition.class);
        DataCollectionId dataCollectionId = Mockito.mock(DataCollectionId.class);
        Struct struct = mock(Struct.class);

        OffsetContext offsetContext = mock(OffsetContext.class);
        TransactionContext transactionContext = new TransactionContext();
        when(offsetContext.getTransactionContext()).thenReturn(transactionContext);
        transactionContext.setTransactionId(txId2);

        monitor.dataEvent(partition, dataCollectionId, offsetContext, "key", struct);
        verify(sender, never()).accept(Mockito.any());
    }

}
