/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.connector.tidb.ticdc.TiCdcTestMessages;
import io.debezium.data.Envelope;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;

/**
 * Tests of the streaming pipeline: TiCDC messages consumed from a (mock) Kafka consumer all the
 * way through the event dispatcher to the emitted {@link SourceRecord}s.
 *
 * @author Aviral Srivastava
 */
public class TiCdcStreamingChangeEventSourceTest {

    private static final String TICDC_TOPIC = "ticdc-inventory";
    private static final TopicPartition TICDC_PARTITION = new TopicPartition(TICDC_TOPIC, 0);

    /**
     * A self-contained harness around the streaming source with a mock consumer and a real
     * dispatcher and queue.
     */
    private static class StreamingHarness {

        final TiDbConnectorConfig connectorConfig;
        final TiDbSchema schema;
        final ChangeEventQueue<DataChangeEvent> queue;
        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.NONE);
        final TiCdcStreamingChangeEventSource source;
        final AtomicBoolean running = new AtomicBoolean(true);

        StreamingHarness(Configuration config) {
            this.connectorConfig = new TiDbConnectorConfig(config);
            @SuppressWarnings("unchecked")
            final TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(TiDbConnectorConfig.TOPIC_NAMING_STRATEGY);
            final SchemaNameAdjuster adjuster = connectorConfig.schemaNameAdjuster();

            this.schema = new TiDbSchema(topicNamingStrategy, connectorConfig.getSourceInfoStructMaker().schema(), adjuster);
            this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                    .pollInterval(connectorConfig.getPollInterval())
                    .maxBatchSize(connectorConfig.getMaxBatchSize())
                    .maxQueueSize(connectorConfig.getMaxQueueSize())
                    .loggingContextSupplier(() -> LoggingContext.forConnector("TiDB", "tidb_server", "streaming-test"))
                    .build();
            final TiDbErrorHandler errorHandler = new TiDbErrorHandler(connectorConfig, queue, null);
            final TiDbTaskContext taskContext = new TiDbTaskContext(config, connectorConfig);

            final EventDispatcher<TiDbPartition, TableId> dispatcher = new EventDispatcher<>(
                    connectorConfig,
                    topicNamingStrategy,
                    schema,
                    queue,
                    connectorConfig.getTableFilters().dataCollectionFilter(),
                    DataChangeEvent::new,
                    new TiDbEventMetadataProvider(),
                    adjuster,
                    new DebeziumHeaderProducer(taskContext));

            this.source = new TiCdcStreamingChangeEventSource(connectorConfig, dispatcher, errorHandler, Clock.system(), schema) {
                @Override
                protected Consumer<byte[], byte[]> createConsumer() {
                    return consumer;
                }
            };

            consumer.updatePartitions(TICDC_TOPIC, List.of(new PartitionInfo(TICDC_TOPIC, 0, null, null, null)));
            consumer.updateBeginningOffsets(Map.of(TICDC_PARTITION, 0L));
            consumer.updateEndOffsets(Map.of(TICDC_PARTITION, 0L));
        }

        List<SourceRecord> run(TiDbOffsetContext offsetContext, List<ConsumerRecord<byte[], byte[]>> messages) throws InterruptedException {
            consumer.schedulePollTask(() -> messages.forEach(consumer::addRecord));
            consumer.schedulePollTask(() -> running.set(false));

            source.init(offsetContext);
            source.execute(new StoppableContext(running), new TiDbPartition(connectorConfig.getLogicalName()), source.getOffsetContext());

            return queue.poll().stream()
                    .map(DataChangeEvent::getRecord)
                    .collect(Collectors.toList());
        }
    }

    private static class StoppableContext implements ChangeEventSourceContext {

        private final AtomicBoolean running;

        StoppableContext(AtomicBoolean running) {
            this.running = running;
        }

        @Override
        public boolean isPaused() {
            return false;
        }

        @Override
        public boolean isRunning() {
            return running.get();
        }

        @Override
        public void resumeStreaming() {
        }

        @Override
        public void waitSnapshotCompletion() {
        }

        @Override
        public void streamingPaused() {
        }

        @Override
        public void waitStreamingPaused() {
        }
    }

    private static Configuration.Builder config() {
        return Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "tidb_server")
                .with(TiDbConnectorConfig.TICDC_BOOTSTRAP_SERVERS, "localhost:9092")
                .with(TiDbConnectorConfig.TICDC_TOPICS, TICDC_TOPIC)
                .with(CommonConnectorConfig.POLL_INTERVAL_MS, 10);
    }

    private static ConsumerRecord<byte[], byte[]> record(long offset, byte[] key, byte[] value) {
        return new ConsumerRecord<>(TICDC_TOPIC, 0, offset, key, value);
    }

    @Test
    public void shouldStreamChangeEventsWithTiDbSourceInfo() throws InterruptedException {
        final StreamingHarness harness = new StreamingHarness(config().build());

        final List<SourceRecord> records = harness.run(null, List.of(
                record(0, TiCdcTestMessages.keyMessage("inventory", "products", 17),
                        TiCdcTestMessages.createMessage("inventory", "products", 17, "scooter", 446245805252059137L)),
                record(1, TiCdcTestMessages.keyMessage("inventory", "products", 17),
                        TiCdcTestMessages.updateMessage("inventory", "products", 17, "scooter", "e-scooter", 446245805252059138L))));

        assertThat(records).hasSize(2);

        final SourceRecord create = records.get(0);
        assertThat(create.topic()).isEqualTo("tidb_server.inventory.products");
        assertThat(((Struct) create.key()).getInt64("id")).isEqualTo(17L);

        final Struct createValue = (Struct) create.value();
        assertThat(createValue.getString(Envelope.FieldName.OPERATION)).isEqualTo(Envelope.Operation.CREATE.code());
        assertThat(createValue.getStruct(Envelope.FieldName.AFTER).getString("name")).isEqualTo("scooter");

        final Struct createSource = createValue.getStruct(Envelope.FieldName.SOURCE);
        assertThat(createSource.getString("db")).isEqualTo("inventory");
        assertThat(createSource.getString("table")).isEqualTo("products");
        assertThat(createSource.getInt64(SourceInfo.COMMIT_TS_KEY)).isEqualTo(446245805252059137L);
        assertThat(createSource.getString(SourceInfo.CLUSTER_ID_KEY)).isEqualTo("tidb-cluster-1");
        assertThat(createSource.getString("connector")).isEqualTo("tidb");
        assertThat(createSource.getString("name")).isEqualTo("tidb_server");

        final SourceRecord update = records.get(1);
        final Struct updateValue = (Struct) update.value();
        assertThat(updateValue.getString(Envelope.FieldName.OPERATION)).isEqualTo(Envelope.Operation.UPDATE.code());
        assertThat(updateValue.getStruct(Envelope.FieldName.BEFORE).getString("name")).isEqualTo("scooter");
        assertThat(updateValue.getStruct(Envelope.FieldName.AFTER).getString("name")).isEqualTo("e-scooter");

        // The offsets track both the TSO commit timestamp and the TiCDC stream position
        @SuppressWarnings("unchecked")
        final Map<String, Object> sourceOffset = (Map<String, Object>) update.sourceOffset();
        assertThat(sourceOffset)
                .containsEntry(TiDbOffsetContext.COMMIT_TS_KEY, 446245805252059138L)
                .containsEntry(TiDbOffsetContext.TICDC_OFFSETS_KEY, TICDC_TOPIC + ":0=2");
        @SuppressWarnings("unchecked")
        final Map<String, Object> sourcePartition = (Map<String, Object>) update.sourcePartition();
        assertThat(sourcePartition).containsEntry("server", "tidb_server");
    }

    @Test
    public void shouldResumeFromStoredStreamPosition() throws InterruptedException {
        final StreamingHarness harness = new StreamingHarness(config().build());

        final TiDbOffsetContext storedOffset = new TiDbOffsetContext.Loader(harness.connectorConfig)
                .load(Map.of(
                        TiDbOffsetContext.COMMIT_TS_KEY, 446245805252059137L,
                        TiDbOffsetContext.TICDC_OFFSETS_KEY, TICDC_TOPIC + ":0=1"));

        final List<SourceRecord> records = harness.run(storedOffset, List.of(
                record(0, null, TiCdcTestMessages.createMessage("inventory", "products", 1, "already-processed", 100L)),
                record(1, null, TiCdcTestMessages.createMessage("inventory", "products", 2, "new-event", 200L))));

        assertThat(records).hasSize(1);
        final Struct value = (Struct) records.get(0).value();
        assertThat(value.getStruct(Envelope.FieldName.AFTER).getString("name")).isEqualTo("new-event");
    }

    @Test
    public void shouldApplyTableIncludeListToTiCdcStream() throws InterruptedException {
        final StreamingHarness harness = new StreamingHarness(config()
                .with("table.include.list", "inventory\\.products")
                .build());

        final List<SourceRecord> records = harness.run(null, List.of(
                record(0, null, TiCdcTestMessages.createMessage("inventory", "products", 1, "kept", 100L)),
                record(1, null, TiCdcTestMessages.createMessage("inventory", "orders", 2, "filtered", 200L))));

        assertThat(records).hasSize(1);
        final Struct value = (Struct) records.get(0).value();
        assertThat(value.getStruct(Envelope.FieldName.AFTER).getString("name")).isEqualTo("kept");
    }

    @Test
    public void shouldStartFromLatestWhenConfigured() throws InterruptedException {
        final StreamingHarness harness = new StreamingHarness(config()
                .with(TiDbConnectorConfig.TICDC_INITIAL_OFFSET, "latest")
                .build());
        // Two messages already sit in the topic; with 'latest' they must not be re-read
        harness.consumer.updateEndOffsets(Map.of(TICDC_PARTITION, 2L));

        final List<SourceRecord> records = harness.run(null, List.of(
                record(0, null, TiCdcTestMessages.createMessage("inventory", "products", 1, "old-1", 100L)),
                record(1, null, TiCdcTestMessages.createMessage("inventory", "products", 2, "old-2", 200L)),
                record(2, null, TiCdcTestMessages.createMessage("inventory", "products", 3, "new", 300L))));

        assertThat(records).hasSize(1);
        final Struct value = (Struct) records.get(0).value();
        assertThat(value.getStruct(Envelope.FieldName.AFTER).getString("name")).isEqualTo("new");
    }

    @Test
    public void shouldEmitDeleteWithTombstone() throws InterruptedException {
        final StreamingHarness harness = new StreamingHarness(config().build());

        final List<SourceRecord> records = harness.run(null, List.of(
                record(0, TiCdcTestMessages.keyMessage("inventory", "products", 17),
                        TiCdcTestMessages.deleteMessage("inventory", "products", 17, "scooter", 300L))));

        // Delete record followed by tombstone (default tombstones.on.delete=true)
        assertThat(records).hasSize(2);
        final Struct deleteValue = (Struct) records.get(0).value();
        assertThat(deleteValue.getString(Envelope.FieldName.OPERATION)).isEqualTo(Envelope.Operation.DELETE.code());
        assertThat(deleteValue.getStruct(Envelope.FieldName.BEFORE).getString("name")).isEqualTo("scooter");
        assertThat(records.get(1).value()).isNull();
    }
}
