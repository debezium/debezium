/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class CommitLogPostProcessorTest extends EmbeddedCassandraConnectorTestBase {

    @Test
    public void testPostProcessor() throws Exception {
        int expectedArchivedFile = 10;
        int expectedErrorFile = 10;
        final AtomicInteger archivedFileCount = new AtomicInteger(0);
        final AtomicInteger errorFileCount = new AtomicInteger(0);

        CommitLogTransfer myTransfer = new CommitLogTransfer() {
            @Override
            public void onSuccessTransfer(File file) {
                archivedFileCount.incrementAndGet();
            }
            @Override
            public void onErrorTransfer(File file) {
                errorFileCount.incrementAndGet();
            }
        };
        CassandraConnectorConfig config = spy(new CassandraConnectorConfig(generateDefaultConfigMap()));
        when(config.getCommitLogTransfer()).thenReturn(myTransfer);
        CassandraConnectorContext context = new CassandraConnectorContext(config);
        CommitLogPostProcessor postProcessor = spy(new CommitLogPostProcessor(context));
        when(postProcessor.isRunning()).thenReturn(true);
        File dir = new File(context.getCassandraConnectorConfig().commitLogRelocationDir());
        populateFakeCommitLogsForDirectory(expectedArchivedFile, new File(dir, QueueProcessor.ARCHIVE_FOLDER));
        populateFakeCommitLogsForDirectory(expectedErrorFile, new File(dir, QueueProcessor.ERROR_FOLDER));

        postProcessor.process();
        postProcessor.shutDown(true);

        assertEquals(expectedArchivedFile, archivedFileCount.get());
        assertEquals(expectedErrorFile, errorFileCount.get());

        clearCommitLogFromDirectory(dir, true);
        context.cleanUp();
    }
}
