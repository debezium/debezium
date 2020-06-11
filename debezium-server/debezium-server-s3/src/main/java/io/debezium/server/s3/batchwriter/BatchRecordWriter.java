/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.s3.batchwriter;

import java.io.IOException;

public interface BatchRecordWriter {

    void append(String destination, String eventValue) throws IOException;

    void uploadBatch() throws IOException;
}
