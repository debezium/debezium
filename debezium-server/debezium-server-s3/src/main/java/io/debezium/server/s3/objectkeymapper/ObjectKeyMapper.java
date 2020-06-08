/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.s3.objectkeymapper;

import java.time.LocalDateTime;

/**
 * Transforms the name of the record destination to the Kinesis stream name.
 *
 * @author Jiri Pechanec
 */

public interface ObjectKeyMapper {

    String map(String destination, LocalDateTime batchTime, int batchId);

    String map(String destination, LocalDateTime batchTime, String recordId);
}
