/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.util.function.Consumer;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.data.Envelope;
import io.debezium.relational.TableSchema;
import io.debezium.util.Clock;

/**
 * Class which generates Kafka Connect {@link org.apache.kafka.connect.source.SourceRecord} records.
 * 
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public abstract class RecordsProducer {
    
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    protected final PostgresTaskContext taskContext;
    protected final SourceInfo sourceInfo;
    
    protected RecordsProducer(PostgresTaskContext taskContext, SourceInfo sourceInfo) {
        assert taskContext != null;
        assert sourceInfo != null;
        
        this.sourceInfo = sourceInfo;
        this.taskContext = taskContext;
    }
    
    /**
     * Starts up this producer. This is normally done by a {@link PostgresConnectorTask} instance. Subclasses should start 
     * enqueuing records via a separate thread at the end of this method.
     * 
     * @param recordsConsumer a consumer of {@link SourceRecord} instances, may not be null
     */
    protected abstract void start(Consumer<SourceRecord> recordsConsumer);
    
    /**
     * Notification that offsets have been committed to Kafka.
     */
    protected abstract void commit();
    
    /**
     * Requests that this producer be stopped. This is normally a request coming from a {@link PostgresConnectorTask} instance
     */
    protected abstract void stop();
    
    protected PostgresSchema schema() {
        return taskContext.schema();
    }
    
    protected TopicSelector topicSelector() {
        return taskContext.topicSelector();
    }
    
    protected Clock clock() {
        return taskContext.clock();
    }
    
    protected Envelope createEnvelope(TableSchema tableSchema, String topicName) {
        return Envelope.defineSchema()
                       .withName(schema().validateSchemaName(topicName + ".Envelope"))
                       .withRecord(tableSchema.valueSchema())
                       .withSource(SourceInfo.SCHEMA)
                       .build();
    }
}
