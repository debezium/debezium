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

import javax.ws.rs.core.Link;
import javax.xml.crypto.Data;

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

    // TODO After extracting add into source info optional block
    // incrementalSnapshotWindow{String from, String to}
    // State to be stored and recovered from offsets
    private final Map<T, DataCollection<T>> dataCollectionsToSnapshot = new LinkedHashMap();

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
    public boolean openWindow(String id, T dataCollectionId) {
        return setWindowStatusForDataCollection(id, dataCollectionId, "open");
    }

    @Override
    public boolean closeWindow(String id, T dataCollectionId) {
        return setWindowStatusForDataCollection(id, dataCollectionId, "close");
    }

    private boolean setWindowStatusForDataCollection(String id, T dataCollectionId, String type) {
        // TODO: confirm if dataCollectionId is nullable
        DataCollection<T> dataCollection = dataCollectionsToSnapshot.get(dataCollectionId);
        if (notExpectedChunk(id, dataCollection)) {
            LOGGER.info("Received request to "+type +" window with id = '{}', expected = '{}', request ignored", id, dataCollection.currentChunkId());
            return false;
        }
        LOGGER.debug(type +" window for incremental snapshot chunk");
        dataCollection.setWindowOpened(type == "open");
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
    private boolean notExpectedChunk(String id, DataCollection<T> dataCollection) {
        String currentChunkId = dataCollection.currentChunkId();
        return currentChunkId == null || !id.startsWith(currentChunkId);
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
            List<LinkedHashMap<String, String>> offsets = dataCollectionsToSnapshot.entrySet().stream().map(entry -> {
                DataCollection<T> dc = entry.getValue();
                LinkedHashMap<String, String> o = new LinkedHashMap<>();
                o.put(DATA_COLLECTIONS_TO_SNAPSHOT_KEY_ID, dc.getId().toString());
                o.put(DATA_COLLECTIONS_TO_SNAPSHOT_KEY_ADDITIONAL_CONDITION,
                        dc.getAdditionalCondition().orElse(null));
                dc.lastEventKeySent().ifPresent(lastKeySent -> o.put(EVENT_PRIMARY_KEY, arrayToSerializedString(lastKeySent)));
                dc.maximumKey().ifPresent(mk -> o.put(TABLE_MAXIMUM_KEY, arrayToSerializedString(mk)));
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
        dataCollectionIds.forEach(dc -> dataCollectionsToSnapshot.put(dc.getId(), dc));
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
        DataCollection<T> removedCollection = dataCollectionsToSnapshot.remove(dataCollectionId);
        return removedCollection != null;
    }

    protected static <U> IncrementalSnapshotContext<U> init(MongoDbIncrementalSnapshotContext<U> context, Map<String, ?> offsets) {
        context.dataCollectionsToSnapshot.clear();

        List<Map<String,String>> incrementalSnapshotOffsets = context.parseIncrementalSnapshotOffsets(offsets);
        if (incrementalSnapshotOffsets != null) {
            incrementalSnapshotOffsets.forEach(collectionOffset -> {
                DataCollection<U> dataCollection = new DataCollection(CollectionId.parse(collectionOffset.get(DATA_COLLECTIONS_TO_SNAPSHOT_KEY_ID)));

                final String lastEventSentKeyStr = collectionOffset.get(EVENT_PRIMARY_KEY);
                if (lastEventSentKeyStr != null) {
                    dataCollection.setChunkEndPosition(context.serializedStringToArray(EVENT_PRIMARY_KEY, lastEventSentKeyStr));
                }
                dataCollection.setLastEventKeySent(null);

                final String maximumKeyStr = collectionOffset.get(TABLE_MAXIMUM_KEY);
                if (maximumKeyStr != null) {
                    dataCollection.maximumKey( context.serializedStringToArray(TABLE_MAXIMUM_KEY, maximumKeyStr));
                }

                context.dataCollectionsToSnapshot.put(dataCollection.getId(), dataCollection);
            });
        }

        return context;
    }


    @Override
    public void sendEvent(Object[] key, T dataCollectionId) {
        dataCollectionsToSnapshot.get(dataCollectionId).setLastEventKeySent(key);
    }

    @Override
    public int dataCollectionsToBeSnapshottedCount() {
        return dataCollectionsToSnapshot.size();
    }

    @Override
    public DataCollection<T> nextDataCollection() {
        Iterator<Map.Entry<T, DataCollection<T>>> collections = dataCollectionsToSnapshot.entrySet().iterator();
        return collections.hasNext() ? collections.next().getValue() : null;
    }

    public static <U> MongoDbIncrementalSnapshotContext<U> load(Map<String, ?> offsets, boolean useCatalogBeforeSchema) {
        final MongoDbIncrementalSnapshotContext<U> context = new MongoDbIncrementalSnapshotContext<>(useCatalogBeforeSchema);
        init(context, offsets);
        return context;
    }

    @Override
    public String toString() {
        return "MongoDbIncrementalSnapshotContext [" + Arrays.toString( dataCollectionsToSnapshot.values().toArray()) + "]";
    }
}
