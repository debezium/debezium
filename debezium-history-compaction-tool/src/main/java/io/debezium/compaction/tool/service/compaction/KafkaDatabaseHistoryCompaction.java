/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.compaction.tool.service.compaction;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.document.Array;
import io.debezium.document.DocumentReader;
import io.debezium.function.Predicates;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryException;
import io.debezium.relational.history.JsonTableChangeSerializer;
import io.debezium.relational.history.TableChanges;
import io.debezium.text.MultipleParsingExceptions;
import io.debezium.text.ParsingException;
import io.debezium.util.Collect;

public class KafkaDatabaseHistoryCompaction {

    public static final String CONFIGURATION_FIELD_PREFIX_STRING = "database.history.";
    public static final Field RECOVERY_POLL_INTERVAL_MS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING
            + "kafka.recovery.poll.interval.ms")
            .withDisplayName("Poll interval during database history recovery (ms)")
            .withType(ConfigDef.Type.INT)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The number of milliseconds to wait while polling for persisted data during recovery.")
            .withDefault(100)
            .withValidation(Field::isNonNegativeInteger);
    public static final Field RECOVERY_POLL_ATTEMPTS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "kafka.recovery.attempts")
            .withDisplayName("Max attempts to recovery database history")
            .withType(ConfigDef.Type.INT)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The number of attempts in a row that no data are returned from Kafka before recover completes. "
                    + "The maximum amount of time to wait after receiving no data is (recovery.attempts) x (recovery.poll.interval.ms).")
            .withDefault(100)
            .withValidation(Field::isInteger);
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDatabaseHistoryCompaction.class);
    private static final String CONSUMER_PREFIX = CONFIGURATION_FIELD_PREFIX_STRING + "consumer.";
    private static final String PRODUCER_PREFIX = CONFIGURATION_FIELD_PREFIX_STRING + "producer.";
    /**
     * The one and only partition of the history topic.
     */
    private static final Integer PARTITION = 0;
    private static final int UNLIMITED_VALUE = -1;

    private final DocumentReader reader = DocumentReader.defaultReader();
    private final String compactedHistoryTopic;
    private final String bootstrapServers;
    private final String historyTopic;
    private final boolean preferDdl = false;
    private final TableChanges.TableChangesSerializer<Array> tableChangesSerializer = new JsonTableChangeSerializer();
    protected Configuration config;
    private Configuration consumerConfig;
    private Configuration producerConfig;
    private volatile KafkaProducer<String, String> producer;
    private int maxRecoveryAttempts;
    private Duration pollInterval;
    private HistoryRecordComparator comparator = HistoryRecordComparator.INSTANCE;
    private boolean skipUnparseableDDL;
    private Function<String, Optional<Pattern>> ddlFilter = (x -> Optional.empty());
    private DatabaseHistoryListener listener = DatabaseHistoryListener.NOOP;
    private boolean useCatalogBeforeSchema;

    public KafkaDatabaseHistoryCompaction(String bootstrapServers, String historyTopic, String compactedHistoryTopic) {
        this.bootstrapServers = bootstrapServers;
        this.historyTopic = historyTopic;
        this.compactedHistoryTopic = compactedHistoryTopic;
    }

    public void configure(Configuration config, HistoryRecordComparator comparator, DatabaseHistoryListener listener, boolean useCatalogBeforeSchema) {
        this.config = config;
        this.comparator = comparator != null ? comparator : HistoryRecordComparator.INSTANCE;
        this.skipUnparseableDDL = config.getBoolean(DatabaseHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS);

        final String ddlFilter = config.getString(DatabaseHistory.DDL_FILTER);
        this.ddlFilter = (ddlFilter != null) ? Predicates.matchedBy(ddlFilter) : this.ddlFilter;
        this.listener = listener;
        this.useCatalogBeforeSchema = useCatalogBeforeSchema;
        this.pollInterval = Duration.ofMillis(config.getInteger(RECOVERY_POLL_INTERVAL_MS));
        this.maxRecoveryAttempts = config.getInteger(RECOVERY_POLL_ATTEMPTS);

        String dbHistoryName = config.getString(DatabaseHistory.NAME, UUID.randomUUID().toString());
        this.consumerConfig = config.subset(CONSUMER_PREFIX, true).edit()
                .withDefault(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
                .withDefault(ConsumerConfig.CLIENT_ID_CONFIG, dbHistoryName)
                .withDefault(ConsumerConfig.GROUP_ID_CONFIG, dbHistoryName)
                .withDefault(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1)
                .withDefault(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
                .withDefault(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 50000)
                .withDefault(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        OffsetResetStrategy.EARLIEST.toString().toLowerCase())
                .withDefault(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .withDefault(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .build();
        this.producerConfig = config.subset(PRODUCER_PREFIX, true).edit()
                .withDefault(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
                .withDefault(ProducerConfig.CLIENT_ID_CONFIG, dbHistoryName)
                .withDefault(ProducerConfig.ACKS_CONFIG, 1)
                .withDefault(ProducerConfig.RETRIES_CONFIG, 1)
                // okay
                .withDefault(ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 32) // 32KB
                .withDefault(ProducerConfig.LINGER_MS_CONFIG, 0)
                .withDefault(ProducerConfig.BUFFER_MEMORY_CONFIG, 1024 * 1024) // 1MB
                .withDefault(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .withDefault(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .withDefault(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000)
                .build();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("KafkaDatabaseHistoryCompaction Consumer config: {}", consumerConfig.withMaskedPasswords());
            LOGGER.info("KafkaDatabaseHistoryCompaction Producer config: {}", producerConfig.withMaskedPasswords());
        }
    }

    public synchronized void start() {
        LOGGER.debug("KafkaDatabaseHistoryCompaction start() method");
        listener.started();
        if (this.producer == null) {
            this.producer = new KafkaProducer<>(this.producerConfig.asProperties());
        }
    }

    public final void record(Map<String, ?> source, Map<String, ?> position, Tables schema, DdlParser parser)
            throws DatabaseHistoryException {

        Tables compactedSchema = recover(source, position, schema, parser);
        LOGGER.info("No of compacted history records: {}", compactedSchema.tableIds().size());
        compactedSchema.tableIds().forEach(tableId -> {
            final Table table = compactedSchema.forTable(tableId);

            TableChanges tableChanges = new TableChanges();

            tableChanges.create(new Table() {
                @Override
                public TableId id() {
                    return tableId;
                }

                @Override
                public List<String> primaryKeyColumnNames() {
                    return table.primaryKeyColumnNames();
                }

                @Override
                public List<String> retrieveColumnNames() {
                    return table.retrieveColumnNames();
                }

                @Override
                public List<Column> columns() {
                    return table.columns();
                }

                @Override
                public Column columnWithName(String s) {
                    return table.columnWithName(s);
                }

                @Override
                public String defaultCharsetName() {
                    return table.defaultCharsetName();
                }

                @Override
                public String comment() {
                    return table.comment();
                }

                @Override
                public TableEditor edit() {
                    return table.edit();
                }
            });

            HistoryRecord compactedRecord = new HistoryRecord(source, position, "", tableId.schema(), null, tableChanges, null);
            storeRecord(compactedRecord);
            listener.onChangeApplied(compactedRecord);
        });
    }

    public final Tables recover(Map<String, ?> source, Map<String, ?> position, Tables schema, DdlParser ddlParser) {
        LOGGER.debug("Recovering DDL history for source partition {} and position {}", source, position);
        listener.recoveryStarted();
        HistoryRecord stopPoint = new HistoryRecord(source, position, "inventory", null, null, null, null);

        AtomicInteger counter = new AtomicInteger();
        recoverRecords(new Consumer<HistoryRecord>() {
            @Override
            public void accept(HistoryRecord recovered) {
                // LOGGER.debug("recovered record: {}", recovered);
                listener.onChangeFromHistory(recovered);
                LOGGER.debug("recovered record position: {} and stop point position: {}", recovered.position(), stopPoint.position());
                if (comparator.isAtOrBefore(recovered, stopPoint)) {
                    // use positions to compact the history records rest of the records will be appended to the new history topic
                    Array tableChanges = recovered.tableChanges();
                    String ddl = recovered.ddl();
                    if (!preferDdl && tableChanges != null && !tableChanges.isEmpty()) {
                        TableChanges changes = tableChangesSerializer.deserialize(tableChanges, useCatalogBeforeSchema);
                        for (TableChanges.TableChange entry : changes) {
                            if (entry.getType() == TableChanges.TableChangeType.CREATE || entry.getType() == TableChanges.TableChangeType.ALTER) {
                                schema.overwriteTable(entry.getTable());
                            }
                            // DROP
                            else {
                                schema.removeTable(entry.getId());
                            }
                        }
                        listener.onChangeApplied(recovered);
                    }
                    else if (ddl != null && ddlParser != null) {
                        if (recovered.databaseName() != null) {
                            ddlParser.setCurrentDatabase(recovered.databaseName()); // may be null
                        }
                        if (recovered.schemaName() != null) {
                            ddlParser.setCurrentSchema(recovered.schemaName()); // may be null
                        }
                        Optional<Pattern> filteredBy = ddlFilter.apply(ddl);
                        if (filteredBy.isPresent()) {
                            LOGGER.info("a DDL '{}' was filtered out of processing by regular expression '{}", ddl, filteredBy.get());
                            return;
                        }
                        try {
                            LOGGER.debug("Applying: {}", ddl);
                            ddlParser.parse(ddl, schema);
                            listener.onChangeApplied(recovered);
                        }
                        catch (final ParsingException | MultipleParsingExceptions e) {
                            if (skipUnparseableDDL) {
                                LOGGER.warn("Ignoring unparseable statements '{}' stored in database history: {}", ddl, e);
                            }
                            else {
                                throw e;
                            }
                        }
                    }
                }
                else {
                    LOGGER.debug("Skipping: {}", recovered.ddl());
                }
            }
        });
        listener.recoveryStopped();
        return schema;
    }

    protected void recoverRecords(Consumer<HistoryRecord> records) {
        try (KafkaConsumer<String, String> historyConsumer = new KafkaConsumer<>(consumerConfig.asProperties())) {
            // Subscribe to the only partition for this topic, and seek to the beginning of that partition ...
            LOGGER.debug("Subscribing to database history topic '{}'", historyTopic);
            historyConsumer.subscribe(Collect.arrayListOf(historyTopic));

            // Read all messages in the topic ...
            long lastProcessedOffset = UNLIMITED_VALUE;
            Long endOffset = null;
            int recoveryAttempts = 0;

            // read the topic until the end
            do {
                if (recoveryAttempts > maxRecoveryAttempts) {
                    throw new IllegalStateException("The database history couldn't be recovered. Consider to increase the value for " + RECOVERY_POLL_INTERVAL_MS.name());
                }

                endOffset = getEndOffsetOfDbHistoryTopic(endOffset, historyConsumer);
                LOGGER.debug("End offset of database history topic is {}", endOffset);

                ConsumerRecords<String, String> recoveredRecords = historyConsumer.poll(this.pollInterval);
                int numRecordsProcessed = 0;

                for (ConsumerRecord<String, String> record : recoveredRecords) {
                    try {
                        if (lastProcessedOffset < record.offset()) {
                            if (record.value() == null) {
                                LOGGER.warn("Skipping null database history record. " +
                                        "This is often not an issue, but if it happens repeatedly please check the '{}' topic.", historyTopic);
                            }
                            else {
                                HistoryRecord recordObj = new HistoryRecord(reader.read(record.value()));
                                LOGGER.trace("Recovering database history: {}", recordObj);
                                if (recordObj == null || !recordObj.isValid()) {
                                    LOGGER.warn("Skipping invalid database history record '{}'. " +
                                            "This is often not an issue, but if it happens repeatedly please check the '{}' topic.",
                                            recordObj, historyTopic);
                                }
                                else {
                                    records.accept(recordObj);
                                    LOGGER.trace("Recovered database history: {}", recordObj);
                                }
                            }
                            lastProcessedOffset = record.offset();
                            ++numRecordsProcessed;
                        }
                    }
                    catch (final IOException e) {
                        LOGGER.error("Error while deserializing history record '{}'", record, e);
                    }
                    catch (final Exception e) {
                        LOGGER.error("Unexpected exception while processing record '{}'", record, e);
                        throw e;
                    }
                }
                if (numRecordsProcessed == 0) {
                    LOGGER.debug("No new records found in the database history; will retry");
                    recoveryAttempts++;
                }
                else {
                    LOGGER.debug("Processed {} records from database history", numRecordsProcessed);
                }
            } while (lastProcessedOffset < endOffset - 1);
        }
    }

    private Long getEndOffsetOfDbHistoryTopic(Long previousEndOffset, KafkaConsumer<String, String> historyConsumer) {
        Map<TopicPartition, Long> offsets = historyConsumer.endOffsets(Collections.singleton(new TopicPartition(historyTopic, PARTITION)));
        Long endOffset = offsets.entrySet().iterator().next().getValue();

        // The end offset should never change during recovery; doing this check here just as - a rather weak - attempt
        // to spot other connectors that share the same history topic accidentally
        if (previousEndOffset != null && !previousEndOffset.equals(endOffset)) {
            throw new IllegalStateException("Detected changed end offset of database history topic (previous: "
                    + previousEndOffset + ", current: " + endOffset
                    + "). Make sure that the same history topic isn't shared by multiple connector instances.");
        }

        return endOffset;
    }

    protected void storeRecord(HistoryRecord record) throws DatabaseHistoryException {
        if (this.producer == null) {
            throw new IllegalStateException("No producer is available. Ensure that 'start()' is called before storing database history records.");
        }
        LOGGER.trace("Storing record into database history: {}", record);
        try {
            ProducerRecord<String, String> produced = new ProducerRecord<>(compactedHistoryTopic, PARTITION, null, record.toString());
            Future<RecordMetadata> future = this.producer.send(produced);
            // Flush and then wait ...
            this.producer.flush();
            RecordMetadata metadata = future.get(); // block forever since we have to be sure this gets recorded
            if (metadata != null) {
                LOGGER.debug("Stored record in topic '{}' partition {} at offset {} ",
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        }
        catch (InterruptedException e) {
            LOGGER.trace("Interrupted before record was written into database history: {}", record);
            Thread.currentThread().interrupt();
            throw new DatabaseHistoryException(e);
        }
        catch (ExecutionException e) {
            throw new DatabaseHistoryException(e);
        }
    }

    public void stop() {
        listener.stopped();

        if (this.producer != null) {
            this.producer.close();
        }
        LOGGER.debug("Stopping kafka producer");
    }
}
