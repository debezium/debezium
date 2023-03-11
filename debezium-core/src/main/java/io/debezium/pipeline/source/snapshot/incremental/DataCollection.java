/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import io.debezium.relational.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * A class describing DataCollection for incremental snapshot
 *
 * @author Vivek Wassan
 *
 */
public class DataCollection<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataCollection.class);
    private final T id;

    private final Optional<String> additionalCondition;

    private final Optional<String> surrogateKey;

    /**
     * The largest PK in the table at the start of snapshot.
     */
    private Object[] maximumKey;

    private String currentChunkId;

    /**
     * The last primary key in chunk that is now in process.
     */
    private Object[] chunkEndPosition;
    private Table schema;
    private boolean schemaVerificationPassed;

    public void setCurrentChunkId(String currentChunkId) {
        this.currentChunkId = currentChunkId;
    }

    public void setChunkEndPosition(Object[] chunkEndPosition) {
        this.chunkEndPosition = chunkEndPosition;
    }

    public void setLastEventKeySent(Object[] lastEventKeySent) {
        this.lastEventKeySent = lastEventKeySent;
    }

    public void setWindowOpened(boolean windowOpened) {
        this.windowOpened = windowOpened;
    }

    /**
     * The PK of the last record that was passed to Kafka Connect. In case of
     * connector restart the start of the first chunk will be populated from it.
     */
    private Object[] lastEventKeySent;

    /**
     * @code(true) if window is opened and deduplication should be executed
     */
    protected boolean windowOpened = false;

    public DataCollection(T id) {
        this(id, Optional.empty(), Optional.empty());
    }

    public DataCollection(T id, Optional<String> additionalCondition, Optional<String> surrogateKey) {
        Objects.requireNonNull(additionalCondition);
        Objects.requireNonNull(surrogateKey);

        this.id = id;
        this.additionalCondition = additionalCondition;
        this.surrogateKey = surrogateKey;
    }

    public T getId() {
        return id;
    }

    public Optional<String> getAdditionalCondition() {
        return additionalCondition;
    }

    public Optional<String> getSurrogateKey() {
        return surrogateKey;
    }

    public void startNewChunk() {
        currentChunkId = UUID.randomUUID().toString();
        LOGGER.debug("Starting new chunk with id '{}'", currentChunkId);
    }

    public String currentChunkId() {
        return currentChunkId;
    }

    public void nextChunkPosition(Object[] end) {
        chunkEndPosition = end;
    }

    public Object[] chunkEndPosititon() {
        return chunkEndPosition;
    }

    public void revertChunk() {
        chunkEndPosition = lastEventKeySent;
        windowOpened = false;
    }

    public void maximumKey(Object[] key) {
        maximumKey = key;
    }

    public Optional<Object[]> maximumKey() {
        return Optional.ofNullable(maximumKey);
    }


    public Optional<Object[]> lastEventKeySent() {
        return Optional.ofNullable(lastEventKeySent);
    }

    public boolean isNonInitialChunk() {
        return chunkEndPosition != null;
    }

    public boolean deduplicationNeeded() {
        return windowOpened;
    }
    
    public Table getSchema() {
        return schema;
    }
    
    public void setSchema(Table schema) {
        this.schema = schema;
    }
    
    public boolean isSchemaVerificationPassed() {
        return schemaVerificationPassed;
    }
    
    public void setSchemaVerificationPassed(boolean schemaVerificationPassed) {
        this.schemaVerificationPassed = schemaVerificationPassed;
        LOGGER.info("Incremental snapshot's schema verification passed = {}, schema = {}", schemaVerificationPassed, schema);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataCollection<?> that = (DataCollection<?>) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "DataCollection{" +
                "id=" + id +
                ", additionalCondition=" + additionalCondition +
                ", surrogateKey=" + surrogateKey +
                ", windowOpened=" + windowOpened +
                ", chunkEndPosition=" + chunkEndPosition +
                ", lastEventKeySent=" + lastEventKeySent +
                ", maximumKey=" + maximumKey +
                '}';
    }
}
