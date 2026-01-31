/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration.Builder;

public class RecordsSnapshotParallelProducerIT extends RecordsSnapshotProducerIT {

    @Override
    protected void alterConfig(Builder config) {
        config.with(CommonConnectorConfig.SNAPSHOT_MAX_THREADS, 3)
                .with(CommonConnectorConfig.LEGACY_SNAPSHOT_MAX_THREADS, Boolean.TRUE);
    }

    @Disabled
    @Test
    @Override
    public void shouldGenerateSnapshotsForDefaultDatatypes() {

    }

    @Disabled
    @Test
    @Override
    public void shouldGenerateSnapshotsForDecimalDatatypesUsingStringEncoding() {

    }

    @Disabled
    @Test
    @Override
    public void shouldGenerateSnapshotsForDefaultDatatypesAdaptiveMicroseconds() {

    }

    @Disabled
    @Test
    @Override
    public void shouldGenerateSnapshotsForPartitionedTables() {

    }

}
