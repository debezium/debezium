/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import org.apache.kafka.connect.source.SourceRecord;

/**
 * An interface that is implemented by a user class which will receive all captured changed data from
 * the connector.
 *
 * @author Jiri Pechanec
 *
 */
public interface ChangeEventHandler {

    /**
     * Process a change event
     * @param record - a data change event
     */
    public void handle(SourceRecord record);
}
