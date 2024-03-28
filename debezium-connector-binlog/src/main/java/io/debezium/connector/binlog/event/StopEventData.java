/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.event;

import com.github.shyiko.mysql.binlog.event.EventData;

/**
 * Indicates a stop event requested.
 *
 * @author Randall Hauch
 * @author Chris Cranford
 */
public class StopEventData implements EventData {
    private static final long serialVersionUID = 1L;

    @Override
    public String toString() {
        return "StopEventData{}";
    }
}
