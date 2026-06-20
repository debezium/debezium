/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.event;

import java.io.Serializable;

/**
 * Raw binlog date components used when a database value cannot be represented as {@link java.time.LocalDate}.
 */
public class BinlogDateValue implements Serializable {

    private final int year;
    private final int month;
    private final int day;

    public BinlogDateValue(int year, int month, int day) {
        this.year = year;
        this.month = month;
        this.day = day;
    }

    public int getYear() {
        return year;
    }

    public int getMonth() {
        return month;
    }

    public int getDay() {
        return day;
    }
}
