/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.util.List;

/**
 * An interface for implementing several kind of buffers
 *
 * @author Gaurav Miglani
 */
public interface Buffer {

    List<SinkRecordDescriptor> add(SinkRecordDescriptor recordDescriptor);

    List<SinkRecordDescriptor> flush();

    boolean isEmpty();
}
