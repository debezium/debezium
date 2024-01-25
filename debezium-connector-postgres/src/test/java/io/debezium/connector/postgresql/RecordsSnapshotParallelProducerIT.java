/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import org.junit.Ignore;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration.Builder;

public class RecordsSnapshotParallelProducerIT extends RecordsSnapshotProducerIT {

    @Override
    protected void alterConfig(Builder config) {
        config.with(CommonConnectorConfig.SNAPSHOT_MAX_THREADS, 3);
    }

    @Ignore
    @Test
    @Override
    public void shouldGenerateSnapshotsForDefaultDatatypes() {

    }

    @Ignore
    @Test
    @Override
    public void shouldGenerateSnapshotsForDecimalDatatypesUsingStringEncoding() {

    }

    @Ignore
    @Test
    @Override
    public void shouldGenerateSnapshotsForDefaultDatatypesAdaptiveMicroseconds() {

    }

    @Ignore
    @Test
    @Override
    public void shouldGenerateSnapshotsForPartitionedTables() {

    }

    @Override
    protected boolean checkRecordOrder() {
        return false;
    }

}
