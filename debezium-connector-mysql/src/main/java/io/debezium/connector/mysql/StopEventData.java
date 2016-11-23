/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import com.github.shyiko.mysql.binlog.event.EventData;

/**
 * @author Randall Hauch
 *
 */
public class StopEventData implements EventData {
    private static final long serialVersionUID = 1L;

    @Override
    public String toString() {
        return "StopEventData{}";
    }
}
