/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.logwriter;

import io.debezium.connector.oracle.Scn;
import io.debezium.relational.TableId;

/**
 * Strategy that controls how the Oracle LGWR (LogWriter) process is to be flushed.
 *
 * @author Chris Cranford
 */
public interface LogWriterFlushStrategy extends AutoCloseable {
    /**
     * The LogMiner implemenation's flush table name.
     */
    String LOGMNR_FLUSH_TABLE = "LOG_MINING_FLUSH";

    /**
     * @return the host or ip address that will be flushed by the strategy
     */
    String getHost();

    /**
     * Perform the Oracle LGWR process flush.
     *
     * @param currentScn the current system change number
     */
    void flush(Scn currentScn) throws InterruptedException;

    /**
     * Checks whether the supplied {@code TableId} is the flush table.
     *
     * @param id the table id
     * @param schemaName the schema name
     * @return true if the table is the flush table, false otherwise
     */
    static boolean isFlushTable(TableId id, String schemaName) {
        return id.table().equalsIgnoreCase(LOGMNR_FLUSH_TABLE) && id.schema().equalsIgnoreCase(schemaName);
    }

}
