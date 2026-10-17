/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.event;

import com.github.shyiko.mysql.binlog.event.QueryEventData;

/**
 * A query event with the time zone of the session that ran the statement.
 */
public class QueryEventDataWithTimeZone extends QueryEventData {

    private String timeZone;

    /**
     * Returns the time zone of the session, which the server records when the statement depends on it, for example when it
     * contains a {@code TIMESTAMP} literal.
     *
     * @return the time zone, such as {@code SYSTEM} or {@code +09:00}; may be null if the event does not contain it
     */
    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    @Override
    public String toString() {
        return super.toString() + ", timeZone='" + timeZone + "'";
    }
}
