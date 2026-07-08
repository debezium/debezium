/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.txmetadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.txmetadata.spi.TransactionMetadataFactory;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaNameAdjuster;

@ExtendWith(MockitoExtension.class)
public class TransactionMonitorTest {

    private static final String TOPIC_NAME = "server.transaction";
    private static final TableId TABLE_A = new TableId("db", null, "a");
    private static final TableId TABLE_B = new TableId("db", null, "b");

    @Mock
    private CommonConnectorConfig connectorConfig;

    @Mock
    private EventMetadataProvider eventMetadataProvider;

    @Mock
    private TransactionMetadataFactory transactionMetadataFactory;

    @Mock
    private Partition partition;

    @Mock
    private OffsetContext offsetContext;

    private List<SourceRecord> sentRecords;
    private TransactionMonitor monitor;

    @BeforeEach
    public void beforeEach() {
        sentRecords = new ArrayList<>();
        when(connectorConfig.getTransactionMetadataFactory()).thenReturn(transactionMetadataFactory);
        when(transactionMetadataFactory.getTransactionStructMaker()).thenReturn(new DefaultTransactionStructMaker(Configuration.empty()));
        monitor = new TransactionMonitor(connectorConfig, eventMetadataProvider, SchemaNameAdjuster.NO_OP, sentRecords::add, TOPIC_NAME);
    }

    @Test
    @FixFor("debezium/dbz#633")
    public void shouldContinueEventCountersWhenRestoredTransactionIsStartedAgain() throws Exception {
        when(connectorConfig.shouldProvideTransactionMetadata()).thenReturn(true);

        // Transaction context restored on restart from offsets that were committed in the middle of
        // transaction tx-1, after three events for table a and two events for table b
        final TransactionContext preRestartContext = new TransactionContext();
        preRestartContext.beginTransaction(new DefaultTransactionInfo("tx-1"));
        preRestartContext.event(TABLE_A);
        preRestartContext.event(TABLE_A);
        preRestartContext.event(TABLE_A);
        preRestartContext.event(TABLE_B);
        preRestartContext.event(TABLE_B);
        final TransactionContext transactionContext = TransactionContext.load(preRestartContext.store(new HashMap<>()));
        when(offsetContext.getTransactionContext()).thenReturn(transactionContext);

        // The connector replays the start of the restored transaction after restart
        monitor.transactionStartedEvent(partition, new DefaultTransactionInfo("tx-1"), offsetContext, Instant.ofEpochMilli(1_000));

        assertThat(sentRecords).as("No duplicate BEGIN is emitted for the restored transaction").isEmpty();
        assertThat(transactionContext.getTransactionId()).isEqualTo("tx-1");
        assertThat(transactionContext.event(TABLE_A)).as("Event counters continue where the restored offsets ended").isEqualTo(4);
        assertThat(transactionContext.getTotalEventCount()).isEqualTo(6);

        when(partition.getSourcePartition()).thenReturn(Map.of());
        when(offsetContext.getOffset()).thenReturn(Map.of());
        monitor.transactionCommittedEvent(partition, offsetContext, Instant.ofEpochMilli(2_000));

        assertThat(sentRecords).hasSize(1);
        final Struct end = (Struct) sentRecords.get(0).value();
        assertThat(end.getString(TransactionStructMaker.DEBEZIUM_TRANSACTION_STATUS_KEY)).isEqualTo(TransactionStatus.END.name());
        assertThat(end.getString(TransactionStructMaker.DEBEZIUM_TRANSACTION_ID_KEY)).isEqualTo("tx-1");
        assertThat(end.getInt64(TransactionStructMaker.DEBEZIUM_TRANSACTION_EVENT_COUNT_KEY)).isEqualTo(6L);
        final List<Struct> dataCollections = end.getArray(TransactionStructMaker.DEBEZIUM_TRANSACTION_DATA_COLLECTIONS_KEY);
        assertThat(dataCollections)
                .extracting(
                        collection -> collection.getString(TransactionStructMaker.DEBEZIUM_TRANSACTION_COLLECTION_KEY),
                        collection -> collection.getInt64(TransactionStructMaker.DEBEZIUM_TRANSACTION_EVENT_COUNT_KEY))
                .containsExactlyInAnyOrder(tuple("db.a", 4L), tuple("db.b", 2L));
    }

    @Test
    @FixFor("debezium/dbz#633")
    public void shouldBeginNewTransactionWhenStartEventHasDifferentTransactionId() throws Exception {
        when(connectorConfig.shouldProvideTransactionMetadata()).thenReturn(true);
        when(partition.getSourcePartition()).thenReturn(Map.of());
        when(offsetContext.getOffset()).thenReturn(Map.of());

        final TransactionContext transactionContext = new TransactionContext();
        transactionContext.beginTransaction(new DefaultTransactionInfo("tx-1"));
        transactionContext.event(TABLE_A);
        when(offsetContext.getTransactionContext()).thenReturn(transactionContext);

        monitor.transactionStartedEvent(partition, new DefaultTransactionInfo("tx-2"), offsetContext, Instant.ofEpochMilli(1_000));

        assertThat(sentRecords).hasSize(1);
        final Struct begin = (Struct) sentRecords.get(0).value();
        assertThat(begin.getString(TransactionStructMaker.DEBEZIUM_TRANSACTION_STATUS_KEY)).isEqualTo(TransactionStatus.BEGIN.name());
        assertThat(begin.getString(TransactionStructMaker.DEBEZIUM_TRANSACTION_ID_KEY)).isEqualTo("tx-2");
        assertThat(transactionContext.getTransactionId()).isEqualTo("tx-2");
        assertThat(transactionContext.getTotalEventCount()).isZero();
    }

    @Test
    public void shouldBeginTransactionWhenNoTransactionIsInProgress() throws Exception {
        when(connectorConfig.shouldProvideTransactionMetadata()).thenReturn(true);
        when(partition.getSourcePartition()).thenReturn(Map.of());
        when(offsetContext.getOffset()).thenReturn(Map.of());

        final TransactionContext transactionContext = new TransactionContext();
        when(offsetContext.getTransactionContext()).thenReturn(transactionContext);

        monitor.transactionStartedEvent(partition, new DefaultTransactionInfo("tx-1"), offsetContext, Instant.ofEpochMilli(1_000));

        assertThat(sentRecords).hasSize(1);
        final Struct begin = (Struct) sentRecords.get(0).value();
        assertThat(begin.getString(TransactionStructMaker.DEBEZIUM_TRANSACTION_STATUS_KEY)).isEqualTo(TransactionStatus.BEGIN.name());
        assertThat(begin.getString(TransactionStructMaker.DEBEZIUM_TRANSACTION_ID_KEY)).isEqualTo("tx-1");
        assertThat(transactionContext.getTransactionId()).isEqualTo("tx-1");
    }
}
