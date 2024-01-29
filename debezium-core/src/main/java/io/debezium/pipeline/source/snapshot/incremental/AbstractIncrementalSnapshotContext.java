/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.DebeziumException;
import io.debezium.annotation.NotThreadSafe;
import io.debezium.pipeline.signal.actions.snapshotting.AdditionalCondition;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.HexConverter;
import io.debezium.util.Strings;

/**
 * A class describing current state of incremental snapshot
 *
 * @author Jiri Pechanec
 *
 */
@NotThreadSafe
public class AbstractIncrementalSnapshotContext<T> implements IncrementalSnapshotContext<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractIncrementalSnapshotContext.class);

    // TODO Consider which (if any) information should be exposed in source info
    public static final String INCREMENTAL_SNAPSHOT_KEY = "incremental_snapshot";

    public static final String EVENT_PRIMARY_KEY = INCREMENTAL_SNAPSHOT_KEY + "_primary_key";
    public static final String TABLE_MAXIMUM_KEY = INCREMENTAL_SNAPSHOT_KEY + "_maximum_key";
    public static final String CORRELATION_ID = INCREMENTAL_SNAPSHOT_KEY + "_correlation_id";
    private final SnapshotDataCollection<T> snapshotDataCollection = new SnapshotDataCollection<>();

    /**
     * {@code true} if window is opened and deduplication should be executed
     */
    protected boolean windowOpened = false;

    /**
     * The last primary key in chunk that is now in process.
     */
    private Object[] chunkEndPosition;

    // TODO After extracting add into source info optional block
    // incrementalSnapshotWindow{String from, String to}
    // State to be stored and recovered from offsets

    private final boolean useCatalogBeforeSchema;
    /**
     * The PK of the last record that was passed to Kafka Connect. In case of
     * connector restart the start of the first chunk will be populated from it.
     */
    private Object[] lastEventKeySent;

    private String currentChunkId;

    /**
     * The largest PK in the table at the start of snapshot.
     */
    private Object[] maximumKey;

    private Table schema;

    private boolean schemaVerificationPassed;

    private String correlationId;

    /**
     * Determines if the incremental snapshot was paused or not.
     */
    private final AtomicBoolean paused = new AtomicBoolean(false);

    public AbstractIncrementalSnapshotContext(boolean useCatalogBeforeSchema) {
        this.useCatalogBeforeSchema = useCatalogBeforeSchema;
    }

    public boolean openWindow(String id) {
        if (notExpectedChunk(id)) {
            LOGGER.info("Received request to open window with id = '{}', expected = '{}', request ignored", id, currentChunkId);
            return false;
        }
        LOGGER.debug("Opening window for incremental snapshot chunk");
        windowOpened = true;
        return true;
    }

    public boolean closeWindow(String id) {
        if (notExpectedChunk(id)) {
            LOGGER.info("Received request to close window with id = '{}', expected = '{}', request ignored", id, currentChunkId);
            return false;
        }
        LOGGER.debug("Closing window for incremental snapshot chunk");
        windowOpened = false;
        return true;
    }

    public void pauseSnapshot() {
        LOGGER.info("Pausing incremental snapshot");
        paused.set(true);
    }

    public void resumeSnapshot() {
        LOGGER.info("Resuming incremental snapshot");
        paused.set(false);
    }

    public boolean isSnapshotPaused() {
        return paused.get();
    }

    /**
     * The snapshotting process can receive out-of-order windowing signals after connector restart
     * as depending on committed offset position some signals can be replayed.
     * In extreme case a signal can be received even when the incremental snapshot was completed just
     * before the restart.
     * Such windowing signals are ignored.
     */
    private boolean notExpectedChunk(String id) {
        return currentChunkId == null || !id.startsWith(currentChunkId);
    }

    public boolean deduplicationNeeded() {
        return windowOpened;
    }

    private String arrayToSerializedString(Object[] array) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(array);
            return HexConverter.convertToHexString(bos.toByteArray());
        }
        catch (IOException e) {
            throw new DebeziumException(String.format("Cannot serialize chunk information %s", array));
        }
    }

    private Object[] serializedStringToArray(String field, String serialized) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(HexConverter.convertFromHex(serialized));
                ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (Object[]) ois.readObject();
        }
        catch (Exception e) {
            throw new DebeziumException(String.format("Failed to deserialize '%s' with value '%s'", field, serialized),
                    e);
        }
    }

    public boolean snapshotRunning() {
        return !snapshotDataCollection.isEmpty();
    }

    public Map<String, Object> store(Map<String, Object> offset) {
        if (!snapshotRunning()) {
            return offset;
        }
        offset.put(EVENT_PRIMARY_KEY, arrayToSerializedString(lastEventKeySent));
        offset.put(TABLE_MAXIMUM_KEY, arrayToSerializedString(maximumKey));
        offset.put(SnapshotDataCollection.DATA_COLLECTIONS_TO_SNAPSHOT_KEY, snapshotDataCollection.dataCollectionsAsJsonString());
        offset.put(CORRELATION_ID, correlationId);
        return offset;
    }

    private void addTablesIdsToSnapshot(List<DataCollection<T>> dataCollectionIds) {
        snapshotDataCollection.add(dataCollectionIds);
    }

    @SuppressWarnings("unchecked")
    public List<DataCollection<T>> addDataCollectionNamesToSnapshot(String correlationId, List<String> dataCollectionIds, List<AdditionalCondition> additionalCondition,
                                                                    String surrogateKey) {

        final List<DataCollection<T>> newDataCollectionIds = dataCollectionIds.stream()
                .map(buildDataCollection(additionalCondition, surrogateKey))
                .collect(Collectors.toList());
        addTablesIdsToSnapshot(newDataCollectionIds);
        this.correlationId = correlationId;
        return newDataCollectionIds;
    }

    private Function<String, DataCollection<T>> buildDataCollection(List<AdditionalCondition> additionalCondition, String surrogateKey) {
        return expandedCollectionName -> {
            String filter = additionalCondition.stream()
                    .filter(condition -> condition.getDataCollection().matcher(expandedCollectionName).matches())
                    .map(AdditionalCondition::getFilter)
                    .findFirst()
                    .orElse("");
            return new DataCollection<T>((T) TableId.parse(expandedCollectionName, useCatalogBeforeSchema), filter, surrogateKey);
        };
    }

    @Override
    public void stopSnapshot() {
        this.snapshotDataCollection.clear();
        this.correlationId = null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean removeDataCollectionFromSnapshot(String dataCollectionId) {
        final T collectionId = (T) TableId.parse(dataCollectionId, useCatalogBeforeSchema);
        return snapshotDataCollection.remove(List.of(new DataCollection<>(collectionId)));
    }

    @Override
    public List<DataCollection<T>> getDataCollections() {
        return new ArrayList<>(snapshotDataCollection.getDataCollectionsToSnapshot());
    }

    @Override
    public void unsetCorrelationId() {
        this.correlationId = null;
    }

    @Override
    public String getCorrelationId() {
        return this.correlationId;
    }

    protected static <U> IncrementalSnapshotContext<U> init(AbstractIncrementalSnapshotContext<U> context, Map<String, ?> offsets) {
        final String lastEventSentKeyStr = (String) offsets.get(EVENT_PRIMARY_KEY);
        context.chunkEndPosition = (lastEventSentKeyStr != null)
                ? context.serializedStringToArray(EVENT_PRIMARY_KEY, lastEventSentKeyStr)
                : null;
        context.lastEventKeySent = null;
        final String maximumKeyStr = (String) offsets.get(TABLE_MAXIMUM_KEY);
        context.maximumKey = (maximumKeyStr != null) ? context.serializedStringToArray(TABLE_MAXIMUM_KEY, maximumKeyStr)
                : null;
        final String dataCollectionsStr = (String) offsets.get(SnapshotDataCollection.DATA_COLLECTIONS_TO_SNAPSHOT_KEY);
        context.snapshotDataCollection.clear();
        if (dataCollectionsStr != null) {
            context.addTablesIdsToSnapshot(context.snapshotDataCollection.stringToDataCollections(dataCollectionsStr, context.useCatalogBeforeSchema));
        }
        context.correlationId = (String) offsets.get(CORRELATION_ID);
        return context;
    }

    public void sendEvent(Object[] key) {
        lastEventKeySent = key;
    }

    public DataCollection<T> currentDataCollectionId() {
        return snapshotDataCollection.peek();
    }

    public int dataCollectionsToBeSnapshottedCount() {
        return snapshotDataCollection.size();
    }

    public void nextChunkPosition(Object[] end) {
        chunkEndPosition = end;
    }

    public Object[] chunkEndPosititon() {
        return chunkEndPosition;
    }

    private void resetChunk() {
        lastEventKeySent = null;
        chunkEndPosition = null;
        maximumKey = null;
        schema = null;
        schemaVerificationPassed = false;
    }

    public void revertChunk() {
        chunkEndPosition = lastEventKeySent;
        windowOpened = false;
    }

    public boolean isNonInitialChunk() {
        return chunkEndPosition != null;
    }

    public DataCollection<T> nextDataCollection() {
        resetChunk();
        return snapshotDataCollection.getNext();
    }

    public void startNewChunk() {
        currentChunkId = UUID.randomUUID().toString();
        LOGGER.debug("Starting new chunk with id '{}'", currentChunkId);
    }

    public String currentChunkId() {
        return currentChunkId;
    }

    public void maximumKey(Object[] key) {
        maximumKey = key;
    }

    public Optional<Object[]> maximumKey() {
        return Optional.ofNullable(maximumKey);
    }

    @Override
    public Table getSchema() {
        return schema;
    }

    @Override
    public void setSchema(Table schema) {
        this.schema = schema;
    }

    @Override
    public boolean isSchemaVerificationPassed() {
        return schemaVerificationPassed;
    }

    @Override
    public void setSchemaVerificationPassed(boolean schemaVerificationPassed) {
        this.schemaVerificationPassed = schemaVerificationPassed;
        LOGGER.info("Incremental snapshot's schema verification passed = {}, schema = {}", schemaVerificationPassed, schema);
    }

    @Override
    public String toString() {
        return "IncrementalSnapshotContext [windowOpened=" + windowOpened + ", chunkEndPosition="
                + Arrays.toString(chunkEndPosition) + ", dataCollectionsToSnapshot=" + snapshotDataCollection.getDataCollectionsToSnapshot()
                + ", lastEventKeySent=" + Arrays.toString(lastEventKeySent) + ", maximumKey="
                + Arrays.toString(maximumKey) + "]";
    }

    public static class SnapshotDataCollection<T> extends LinkedList<DataCollection<T>> {

        public static final String DATA_COLLECTIONS_TO_SNAPSHOT_KEY = INCREMENTAL_SNAPSHOT_KEY + "_collections";

        public static final String DATA_COLLECTIONS_TO_SNAPSHOT_KEY_ID = DATA_COLLECTIONS_TO_SNAPSHOT_KEY + "_id";

        public static final String DATA_COLLECTIONS_TO_SNAPSHOT_KEY_ADDITIONAL_CONDITION = DATA_COLLECTIONS_TO_SNAPSHOT_KEY
                + "_additional_condition";

        public static final String DATA_COLLECTIONS_TO_SNAPSHOT_KEY_SURROGATE_KEY = DATA_COLLECTIONS_TO_SNAPSHOT_KEY
                + "_surrogate_key";
        private final ObjectMapper mapper = new ObjectMapper();
        private final TypeReference<List<LinkedHashMap<String, String>>> mapperTypeRef = new TypeReference<>() {
        };
        private final Queue<DataCollection<T>> dataCollectionsToSnapshot = new LinkedList<>();
        private String dataCollectionsToSnapshotJson;

        public SnapshotDataCollection() {
        }

        public void add(List<DataCollection<T>> dataCollectionIds) {
            this.dataCollectionsToSnapshot.addAll(dataCollectionIds);
            this.dataCollectionsToSnapshotJson = jsonString();
        }

        public DataCollection<T> getNext() {
            DataCollection<T> nextDataCollection = this.dataCollectionsToSnapshot.poll();
            this.dataCollectionsToSnapshotJson = jsonString();
            return nextDataCollection;
        }

        public DataCollection<T> peek() {
            return this.dataCollectionsToSnapshot.peek();
        }

        public int size() {
            return this.dataCollectionsToSnapshot.size();
        }

        public void clear() {
            this.dataCollectionsToSnapshot.clear();
            this.dataCollectionsToSnapshotJson = null;
        }

        public boolean isEmpty() {
            return this.dataCollectionsToSnapshot.isEmpty();
        }

        public boolean remove(List<DataCollection<T>> toRemove) {
            boolean removed = this.dataCollectionsToSnapshot.removeAll(toRemove);
            this.dataCollectionsToSnapshotJson = jsonString();
            return removed;
        }

        public String dataCollectionsAsJsonString() {
            return this.dataCollectionsToSnapshotJson;
        }

        public Queue<DataCollection<T>> getDataCollectionsToSnapshot() {
            return this.dataCollectionsToSnapshot;
        }

        private String jsonString() {
            // TODO Handle non-standard table ids containing dots, commas etc.

            if (!Strings.isNullOrEmpty(dataCollectionsToSnapshotJson)) {
                // A cached value to improve performance since this method is called in the "store"
                // that is called during events processing
                return dataCollectionsToSnapshotJson;
            }

            try {
                List<LinkedHashMap<String, String>> dataCollectionsMap = dataCollectionsToSnapshot.stream()
                        .map(x -> {
                            LinkedHashMap<String, String> map = new LinkedHashMap<>();
                            map.put(DATA_COLLECTIONS_TO_SNAPSHOT_KEY_ID, x.getId().toString());
                            map.put(DATA_COLLECTIONS_TO_SNAPSHOT_KEY_ADDITIONAL_CONDITION, x.getAdditionalCondition().orElse(null));
                            map.put(DATA_COLLECTIONS_TO_SNAPSHOT_KEY_SURROGATE_KEY, x.getSurrogateKey().orElse(null));
                            return map;
                        })
                        .collect(Collectors.toList());

                return mapper.writeValueAsString(dataCollectionsMap);
            }
            catch (JsonProcessingException e) {
                throw new DebeziumException("Cannot serialize dataCollectionsToSnapshot information");
            }
        }

        private List<DataCollection<T>> stringToDataCollections(String dataCollectionsStr, boolean useCatalogBeforeSchema) {
            try {
                List<LinkedHashMap<String, String>> dataCollections = mapper.readValue(dataCollectionsStr, mapperTypeRef);
                return dataCollections.stream()
                        .map(x -> new DataCollection<>((T) TableId.parse(x.get(DATA_COLLECTIONS_TO_SNAPSHOT_KEY_ID), useCatalogBeforeSchema),
                                Optional.ofNullable(x.get(DATA_COLLECTIONS_TO_SNAPSHOT_KEY_ADDITIONAL_CONDITION)).orElse(""),
                                Optional.ofNullable(x.get(DATA_COLLECTIONS_TO_SNAPSHOT_KEY_SURROGATE_KEY)).orElse("")))
                        .collect(Collectors.toList());
            }
            catch (JsonProcessingException e) {
                throw new DebeziumException("Cannot de-serialize dataCollectionsToSnapshot information");
            }
        }
    }
}
