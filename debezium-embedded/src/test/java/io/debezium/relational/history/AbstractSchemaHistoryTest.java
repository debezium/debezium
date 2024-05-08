/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;

/**
 * An abstract database schema history class, allowing each connector to extend to offer a common set of tests
 *
 * @author Chris Cranford
 */
public abstract class AbstractSchemaHistoryTest extends AbstractConnectorTest {

    private MemorySchemaHistory schemaHistory;

    @Before
    public void beforeEach() {
        this.schemaHistory = new MemorySchemaHistory();
    }

    @After
    public void afterEach() {
        if (this.schemaHistory != null) {
            this.schemaHistory.stop();
        }
    }

    @Test
    @FixFor("DBZ-4451")
    public void shouldRecoverRenamedTableWithOnlyTheRenamedEntry() throws Exception {
        // Record records
        record(getRenameCreateHistoryRecord(), getRenameAlterHistoryRecord());

        // Recover
        final Tables tables = recoverHistory();
        assertThat(tables.size()).isEqualTo(1);
        assertThat(tables.tableIds()).containsOnly(getRenameTableId());
    }

    protected abstract HistoryRecord getRenameCreateHistoryRecord();

    protected abstract HistoryRecord getRenameAlterHistoryRecord();

    protected abstract TableId getRenameTableId();

    protected abstract Offsets<Partition, OffsetContext> getOffsets();

    protected abstract DdlParser getDdlParser();

    protected Configuration getHistoryConfiguration() {
        return Configuration.create()
                .with(SchemaHistory.NAME, "my-db-history")
                .build();
    }

    protected void record(HistoryRecord... records) throws Exception {
        Arrays.stream(records).forEach(schemaHistory::storeRecord);
    }

    protected Tables recoverHistory() {
        // Initialize history
        schemaHistory.configure(getHistoryConfiguration(), null, SchemaHistoryMetrics.NOOP, true);
        schemaHistory.start();
        schemaHistory.initializeStorage();

        // Recover records
        final Tables tables = new Tables();
        schemaHistory.recover(getOffsets(), tables, getDdlParser());
        return tables;
    }

}
