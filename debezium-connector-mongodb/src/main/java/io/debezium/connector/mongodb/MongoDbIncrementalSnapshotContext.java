/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.DebeziumException;
import io.debezium.annotation.NotThreadSafe;
import io.debezium.pipeline.source.snapshot.incremental.DataCollection;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.relational.Table;
import io.debezium.util.HexConverter;

/**
 * Describes current state of incremental snapshot of MongoDB connector
 *
 * @author Jiri Pechanec
 *
 */
@NotThreadSafe
public class MongoDbIncrementalSnapshotContext<T> implements IncrementalSnapshotContext<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbIncrementalSnapshotContext.class);

    // TODO Consider which (if any) information should be exposed in source info
    public static final String INCREMENTAL_SNAPSHOT_KEY = "incremental_snapshot";
    public static final String DATA_COLLECTIONS_TO_SNAPSHOT_KEY = INCREMENTAL_SNAPSHOT_KEY + "_collections";

    public static final String DATA_COLLECTIONS_TO_SNAPSHOT_KEY_ID = DATA_COLLECTIONS_TO_SNAPSHOT_KEY + "_id";

    public static final String DATA_COLLECTIONS_TO_SNAPSHOT_KEY_ADDITIONAL_CONDITION = DATA_COLLECTIONS_TO_SNAPSHOT_KEY
            + "_additional_condition";
    public static final String EVENT_PRIMARY_KEY = INCREMENTAL_SNAPSHOT_KEY + "_primary_key";
    public static final String TABLE_MAXIMUM_KEY = INCREMENTAL_SNAPSHOT_KEY + "_maximum_key";

    /**
     * @code(true) if window is opened and deduplication should be executed
     */
    protected boolean windowOpened = false;

    /**
     * The last primary key in chunk that is now in process.
     */
    private Object[] chunkEndPosition;

    // TODO After extracting add into source info optional block
    // incrementalSnapshotWindow{String from, String to}
    // State to be stored and recovered from offsets
    private final Queue<DataCollection<T>> dataCollectionsToSnapshot = new LinkedList<>();

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

    /**
     * Determines if the incremental snapshot was paused or not.
     */
    private AtomicBoolean paused = new AtomicBoolean(false);
    private ObjectMapper mapper = new ObjectMapper();

    private TypeReference<List<LinkedHashMap<String, String>>> mapperTypeRef = new TypeReference<>() {
    };

    public MongoDbIncrementalSnapshotContext(boolean useCatalogBeforeSchema) {
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

    private String dataCollectionsToSnapshotAsString() {
        // TODO Handle non-standard table ids containing dots, commas etc.
        try {
            List<LinkedHashMap<String, String>> dataCollectionsMap = dataCollectionsToSnapshot.stream()
                    .map(x -> {
                        LinkedHashMap<String, String> map = new LinkedHashMap<>();
                        map.put(DATA_COLLECTIONS_TO_SNAPSHOT_KEY_ID, x.getId().toString());
                        map.put(DATA_COLLECTIONS_TO_SNAPSHOT_KEY_ADDITIONAL_CONDITION,
                                x.getAdditionalCondition().orElse(null));
                        return map;
                    })
                    .collect(Collectors.toList());
            return mapper.writeValueAsString(dataCollectionsMap);
        }
        catch (JsonProcessingException e) {
            throw new DebeziumException("Cannot serialize dataCollectionsToSnapshot information");
        }
    }

    private List<DataCollection<T>> stringToDataCollections(String dataCollectionsStr) {
        try {
            List<LinkedHashMap<String, String>> dataCollections = mapper.readValue(dataCollectionsStr, mapperTypeRef);
            List<DataCollection<T>> dataCollectionsList = dataCollections.stream()
                    .map(x -> new DataCollection<T>((T) CollectionId.parse(x.get(DATA_COLLECTIONS_TO_SNAPSHOT_KEY_ID)), null))
                    .filter(x -> x.getId() != null)
                    .collect(Collectors.toList());
            return dataCollectionsList;
        }
        catch (JsonProcessingException e) {
            throw new DebeziumException("Cannot de-serialize dataCollectionsToSnapshot information");
        }
    }

    public boolean snapshotRunning() {
        return !dataCollectionsToSnapshot.isEmpty();
    }

    public Map<String, Object> store(Map<String, Object> offset) {
        if (!snapshotRunning()) {
            return offset;
        }
        offset.put(EVENT_PRIMARY_KEY, arrayToSerializedString(lastEventKeySent));
        offset.put(TABLE_MAXIMUM_KEY, arrayToSerializedString(maximumKey));
        offset.put(DATA_COLLECTIONS_TO_SNAPSHOT_KEY, dataCollectionsToSnapshotAsString());
        return offset;
    }

    private void addTablesIdsToSnapshot(List<DataCollection<T>> dataCollectionIds) {
        dataCollectionsToSnapshot.addAll(dataCollectionIds);
    }

    @SuppressWarnings("unchecked")
    public List<DataCollection<T>> addDataCollectionNamesToSnapshot(List<String> dataCollectionIds, Optional<String> _additionalCondition) {
        final List<DataCollection<T>> newDataCollectionIds = dataCollectionIds.stream()
                .map(x -> new DataCollection<T>((T) CollectionId.parse(x), null))
                .filter(x -> x.getId() != null) // Remove collections with incorrectly formatted name
                .collect(Collectors.toList());
        addTablesIdsToSnapshot(newDataCollectionIds);
        return newDataCollectionIds;
    }

    @Override
    public void stopSnapshot() {
        this.dataCollectionsToSnapshot.clear();
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean removeDataCollectionFromSnapshot(String dataCollectionId) {
        final T collectionId = (T) CollectionId.parse(dataCollectionId);
        return dataCollectionsToSnapshot.remove(new DataCollection<T>(collectionId, null));
    }

    protected static <U> IncrementalSnapshotContext<U> init(MongoDbIncrementalSnapshotContext<U> context, Map<String, ?> offsets) {
        final String lastEventSentKeyStr = (String) offsets.get(EVENT_PRIMARY_KEY);
        context.chunkEndPosition = (lastEventSentKeyStr != null)
                ? context.serializedStringToArray(EVENT_PRIMARY_KEY, lastEventSentKeyStr)
                : null;
        context.lastEventKeySent = null;
        final String maximumKeyStr = (String) offsets.get(TABLE_MAXIMUM_KEY);
        context.maximumKey = (maximumKeyStr != null) ? context.serializedStringToArray(TABLE_MAXIMUM_KEY, maximumKeyStr)
                : null;
        final String dataCollectionsStr = (String) offsets.get(DATA_COLLECTIONS_TO_SNAPSHOT_KEY);
        context.dataCollectionsToSnapshot.clear();
        if (dataCollectionsStr != null) {
            context.addTablesIdsToSnapshot(context.stringToDataCollections(dataCollectionsStr));
        }
        return context;
    }

    public void sendEvent(Object[] key) {
        lastEventKeySent = key;
    }

    public DataCollection<T> currentDataCollectionId() {
        return dataCollectionsToSnapshot.peek();
    }

    public int dataCollectionsToBeSnapshottedCount() {
        return dataCollectionsToSnapshot.size();
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
        return dataCollectionsToSnapshot.poll();
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

    public static <U> MongoDbIncrementalSnapshotContext<U> load(Map<String, ?> offsets, boolean useCatalogBeforeSchema) {
        final MongoDbIncrementalSnapshotContext<U> context = new MongoDbIncrementalSnapshotContext<>(useCatalogBeforeSchema);
        init(context, offsets);
        return context;
    }

    @Override
    public String toString() {
        return "MongoDbIncrementalSnapshotContext [windowOpened=" + windowOpened + ", chunkEndPosition="
                + Arrays.toString(chunkEndPosition) + ", dataCollectionsToSnapshot=" + dataCollectionsToSnapshot
                + ", lastEventKeySent=" + Arrays.toString(lastEventKeySent) + ", maximumKey="
                + Arrays.toString(maximumKey) + "]";
    }
}
