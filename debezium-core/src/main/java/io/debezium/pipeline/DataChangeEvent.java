/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

/**
 * Intermediary representation of data change events, as exchanged between producer threads (e.g. reading the MySQL binlog) and consumer threads (e.g. the Kafka Connect polling loop).
 *
 * @author Gunnar Morling
 *
 */
public interface DataChangeEvent {

    long getApproximateSizeInBytes();
}
