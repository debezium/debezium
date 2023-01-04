/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.logwriter;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.Scn;

/**
 * A simple strategy that performs no operations to attempt to flush the Oracle redo
 * log writer buffers (LGWR) because the connection is operating in read-only mode.
 *
 * @author Chris Cranford
 */
public class ReadOnlyLogWriterFlushStrategy implements LogWriterFlushStrategy {
    @Override
    public String getHost() {
        throw new DebeziumException("Not applicable when using read-only flushing strategy");
    }

    @Override
    public void flush(Scn currentScn) throws InterruptedException {
        // no operation
    }

    @Override
    public void close() throws Exception {
        // no operation
    }
}
