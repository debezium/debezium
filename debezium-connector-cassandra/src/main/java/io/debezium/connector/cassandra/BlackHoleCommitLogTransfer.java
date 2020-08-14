/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.io.File;

/**
 * Implementation of {@link CommitLogTransfer} which deletes commit logs.
 */
public class BlackHoleCommitLogTransfer implements CommitLogTransfer {

    @Override
    public void onSuccessTransfer(File file) {
        CommitLogUtil.deleteCommitLog(file);
    }

    @Override
    public void onErrorTransfer(File file) {
        CommitLogUtil.deleteCommitLog(file);
    }

    @Override
    public void getErrorCommitLogFiles() {
    }
}
