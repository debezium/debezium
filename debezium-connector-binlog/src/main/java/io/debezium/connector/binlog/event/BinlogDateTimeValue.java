/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.event;

/**
 * Raw binlog timestamp components used when a database value cannot be represented as {@link java.time.LocalDateTime}.
 */
public class BinlogDateTimeValue extends BinlogDateValue {

    private final int hour;
    private final int minute;
    private final int second;
    private final int nanos;

    public BinlogDateTimeValue(int year, int month, int day, int hour, int minute, int second, int nanos) {
        super(year, month, day);
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.nanos = nanos;
    }

    public int getHour() {
        return hour;
    }

    public int getMinute() {
        return minute;
    }

    public int getSecond() {
        return second;
    }

    public int getNanos() {
        return nanos;
    }
}
