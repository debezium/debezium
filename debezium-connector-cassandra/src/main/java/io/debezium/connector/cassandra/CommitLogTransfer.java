/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.io.File;
import java.util.Properties;

/**
 * Interface used to transfer commit logs
 */
public interface CommitLogTransfer {

    /**
     * Initialize resources required by the commit log transfer
     */
    default void init(Properties commitLogTransferConfigs) throws Exception {
    }

    /**
     * Destroy resources used by the commit log transfer
     */

    default void destroy() throws Exception {
    }

    /**
     * Transfer a commit log that has been successfully processed.
     */
    void onSuccessTransfer(File file);

    /**
     * Transfer a commit log that has not been successfully processed.
     */
    void onErrorTransfer(File file);

    /**
     * Get all error commitLog files into cdc_raw directory for re-processing.
     */
    void getErrorCommitLogFiles();
}
