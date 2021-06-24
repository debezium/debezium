/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.util.LinkedList;
import java.util.Queue;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;

public class MismatchRecordException extends ConnectException {
    private static final long serialVersionUID = 1L;
    private final LinkedList<SourceRecord> actualRecords;
    private final LinkedList<SourceRecord> expectedRecords;

    public MismatchRecordException(String msg, Queue<SourceRecord> actualRecords, Queue<SourceRecord> expectedRecords) {
        super(msg);
        this.actualRecords = new LinkedList<>(actualRecords);
        this.expectedRecords = new LinkedList<>(expectedRecords);
    }

    public MismatchRecordException(AssertionError error, String msg, Queue<SourceRecord> actualRecords, Queue<SourceRecord> expectedRecords) {
        super(msg, error);
        this.actualRecords = new LinkedList<>(actualRecords);
        this.expectedRecords = new LinkedList<>(expectedRecords);
    }

    public AssertionError getError() {
        return (AssertionError) super.getCause();
    }

    public LinkedList<SourceRecord> getActualRecords() {
        return actualRecords;
    }

    public LinkedList<SourceRecord> getExpectedRecords() {
        return expectedRecords;
    }
}
