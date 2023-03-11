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
import java.util.*;
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
    public static final String DATA_COLLECTIONS_TO_SNAPSHOT_KEY_ID = "id";
    public static final String DATA_COLLECTIONS_TO_SNAPSHOT_KEY_ADDITIONAL_CONDITION = "additional_condition";
    public static final String EVENT_PRIMARY_KEY = "primary_key";
    public static final String TABLE_MAXIMUM_KEY = "maximum_key";

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
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final ObjectMapper mapper = new ObjectMapper();

    private final TypeReference<List<LinkedHashMap<String, String>>> mapperTypeRef = new TypeReference<>() {
    };

    private final TypeReference<List<Map<String, String>>> offsetMapperTypeRef = new TypeReference<>() {
    };

    public MongoDbIncrementalSnapshotContext(boolean useCatalogBeforeSchema) {
    }

    @Override
    public boolean openWindow(String id, String dataCollectionId) {
        if (notExpectedChunk(id)) {
            LOGGER.info("Received request to open window with id = '{}', expected = '{}', request ignored", id, currentChunkId);
            return false;
        }
        LOGGER.debug("Opening window for incremental snapshot chunk");
        windowOpened = true;
        return true;
    }

    @Override
    public boolean closeWindow(String id, String dataCollectionId) {
        if (notExpectedChunk(id)) {
            LOGGER.info("Received request to close window with id = '{}', expected = '{}', request ignored", id, currentChunkId);
            return false;
        }
        LOGGER.debug("Closing window for incremental snapshot chunk");
        windowOpened = false;
        return true;
    }

    @Override
    public void pauseSnapshot() {
        LOGGER.info("Pausing incremental snapshot");
        paused.set(true);
    }

    @Override
    public void resumeSnapshot() {
        LOGGER.info("Resuming incremental snapshot");
        paused.set(false);
    }

    @Override
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

    @Override
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
                    .map(x -> new DataCollection<T>((T) CollectionId.parse(x.get(DATA_COLLECTIONS_TO_SNAPSHOT_KEY_ID))))
                    .filter(x -> x.getId() != null)
                    .collect(Collectors.toList());
            return dataCollectionsList;
        }
        catch (JsonProcessingException e) {
            throw new DebeziumException("Cannot de-serialize dataCollectionsToSnapshot information");
        }
    }

    @Override
    public boolean snapshotRunning() {
        return !dataCollectionsToSnapshot.isEmpty();
    }

    @Override
    public Map<String, Object> store(Map<String, Object> offset) {
        if (!snapshotRunning()) {
            return offset;
        }

        return serializeIncrementalSnapshotOffsets(offset);
    }

    private Map<String, Object> serializeIncrementalSnapshotOffsets(Map<String, Object> offset) {
        try {
            List<LinkedHashMap<String, String>> offsets = dataCollectionsToSnapshot.stream().map(dc -> {
                LinkedHashMap<String, String> o = new LinkedHashMap<>();
                o.put(DATA_COLLECTIONS_TO_SNAPSHOT_KEY_ID, dc.getId().toString());
                o.put(DATA_COLLECTIONS_TO_SNAPSHOT_KEY_ADDITIONAL_CONDITION,
                        dc.getAdditionalCondition().orElse(null));
                o.put(EVENT_PRIMARY_KEY, arrayToSerializedString(lastEventKeySent));
                o.put(TABLE_MAXIMUM_KEY, arrayToSerializedString(maximumKey));
                return o;
            }).collect(Collectors.toList());

            offset.put(INCREMENTAL_SNAPSHOT_KEY, mapper.writeValueAsString(offsets));
            return offset;
        } catch (JsonProcessingException e) {
            throw new DebeziumException(String.format("Cannot serialize %s information", INCREMENTAL_SNAPSHOT_KEY));
        }
    }

    private List<Map<String,String>> parseIncrementalSnapshotOffsets(Map<String, ?> offsets) {
        try {
            final String incrementalSnapshotOffsetsStr = (String) offsets.get(INCREMENTAL_SNAPSHOT_KEY);
            return incrementalSnapshotOffsetsStr == null ? null : mapper.readValue(incrementalSnapshotOffsetsStr, offsetMapperTypeRef);
        } catch (JsonProcessingException e) {
            throw new DebeziumException(String.format("Cannot deserialize %s information", INCREMENTAL_SNAPSHOT_KEY));
        }
    }

    private void addTablesIdsToSnapshot(List<DataCollection<T>> dataCollectionIds) {
        dataCollectionsToSnapshot.addAll(dataCollectionIds);
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<DataCollection<T>> addDataCollectionNamesToSnapshot(List<String> dataCollectionIds, Optional<String> _additionalCondition,
                                                                    Optional<String> surrogateKey) {
        final List<DataCollection<T>> newDataCollectionIds = dataCollectionIds.stream()
                .map(x -> new DataCollection<T>((T) CollectionId.parse(x)))
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
        return dataCollectionsToSnapshot.remove(new DataCollection<T>(collectionId));
    }


    protected static <U> IncrementalSnapshotContext<U> init(MongoDbIncrementalSnapshotContext<U> context, Map<String, ?> offsets) {
        List<Map<String,String>> incrementalSnapshotOffsets = context.parseIncrementalSnapshotOffsets(offsets);

        List<DataCollection<U>> dataCollections = new ArrayList<>();
        if (incrementalSnapshotOffsets != null) {
            incrementalSnapshotOffsets.stream().forEach(collectionOffset -> {
                final String lastEventSentKeyStr = collectionOffset.get(EVENT_PRIMARY_KEY);
                context.chunkEndPosition = (lastEventSentKeyStr != null)
                        ? context.serializedStringToArray(EVENT_PRIMARY_KEY, lastEventSentKeyStr)
                        : null;
                context.lastEventKeySent = null;

                final String maximumKeyStr = collectionOffset.get(TABLE_MAXIMUM_KEY);
                context.maximumKey = (maximumKeyStr != null) ? context.serializedStringToArray(TABLE_MAXIMUM_KEY, maximumKeyStr)
                        : null;

                dataCollections.add(new DataCollection(CollectionId.parse(collectionOffset.get(DATA_COLLECTIONS_TO_SNAPSHOT_KEY_ID))));
            });
        }

        context.dataCollectionsToSnapshot.clear();
        context.addTablesIdsToSnapshot(dataCollections);
        return context;
    }


    @Override
    public void sendEvent(Object[] key) {
        lastEventKeySent = key;
    }

    @Override
    public DataCollection<T> currentDataCollectionId() {
        return dataCollectionsToSnapshot.peek();
    }

    @Override
    public int dataCollectionsToBeSnapshottedCount() {
        return dataCollectionsToSnapshot.size();
    }

    @Override
    public void nextChunkPosition(Object[] end) {
        chunkEndPosition = end;
    }

    @Override
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

    @Override
    public void revertChunk() {
        chunkEndPosition = lastEventKeySent;
        windowOpened = false;
    }

    @Override
    public boolean isNonInitialChunk() {
        return chunkEndPosition != null;
    }

    @Override
    public DataCollection<T> nextDataCollection() {
        resetChunk();
        return dataCollectionsToSnapshot.poll();
    }

    @Override
    public void startNewChunk() {
        currentChunkId = UUID.randomUUID().toString();
        LOGGER.debug("Starting new chunk with id '{}'", currentChunkId);
    }

    @Override
    public String currentChunkId() {
        return currentChunkId;
    }

    @Override
    public void maximumKey(Object[] key) {
        maximumKey = key;
    }

    @Override
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
