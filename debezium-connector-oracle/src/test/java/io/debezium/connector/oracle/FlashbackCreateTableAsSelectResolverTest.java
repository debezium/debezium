/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_BATCH_SIZE;
import static io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_QUEUE_SIZE;
import static io.debezium.config.CommonConnectorConfig.DEFAULT_POLL_DISPATCH_INTERVAL_MILLIS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.base.DefaultQueueProvider;
import io.debezium.connector.oracle.OracleConnection.NonRelationalTableException;
import io.debezium.connector.oracle.jdbc.StandardOracleConnectionFactory;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;

/**
 * Tests how {@link FlashbackCreateTableAsSelectResolver} applies the event processing failure handling mode
 * when the definition of a table created by {@code CREATE TABLE AS SELECT} cannot be read.
 *
 * @author Chris Cranford
 */
public class FlashbackCreateTableAsSelectResolverTest {

    private static final TableId TABLE_ID = TableId.parse("ORCLPDB1.DEBEZIUM.DBZ801_BAK");
    private static final Scn COMMIT_SCN = Scn.valueOf(12345);

    private LogMinerStreamingChangeEventSourceMetrics metrics;

    @Test
    @FixFor("debezium/dbz#801")
    public void shouldReturnTheDefinitionReadAtTheCommit() throws Exception {
        final OracleConnectorConfig config = getConfig("fail");
        final FlashbackCreateTableAsSelectResolver resolver = new FlashbackCreateTableAsSelectResolver(config, new StandardOracleConnectionFactory(config), metrics,
                COMMIT_SCN) {
            @Override
            protected String readTableDdl(TableId tableId, Scn scn) {
                assertThat(tableId).isEqualTo(TABLE_ID);
                assertThat(scn).isEqualTo(COMMIT_SCN);
                return "CREATE TABLE \"DEBEZIUM\".\"DBZ801_BAK\" (\"ID\" NUMBER(9,0), \"NAME\" VARCHAR2(50))";
            }
        };

        assertThat(resolver.resolveCreateTableDdl(TABLE_ID)).startsWith("CREATE TABLE \"DEBEZIUM\".\"DBZ801_BAK\"");
        assertThat(metrics.getWarningCount()).isZero();
    }

    @Test
    @FixFor("debezium/dbz#801")
    public void shouldFailNamingTheMissingPrivilegeWhenHandlingModeIsFail() throws Exception {
        final FlashbackCreateTableAsSelectResolver resolver = getResolverThatCannotRead(getConfig("fail"));

        assertThatThrownBy(() -> resolver.resolveCreateTableDdl(TABLE_ID))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("DBMS_FLASHBACK")
                .hasMessageContaining(TABLE_ID.toString())
                .hasMessageContaining(COMMIT_SCN.toString())
                .hasMessageContaining("ORA-01031")
                .hasCauseInstanceOf(SQLException.class);
        assertThat(metrics.getWarningCount()).isZero();
    }

    @Test
    @FixFor("debezium/dbz#801")
    public void shouldReportTheCauseWithoutThePrivilegeHintWhenTheUndoIsGone() throws Exception {
        final FlashbackCreateTableAsSelectResolver resolver = getResolverThatCannotRead(getConfig("fail"),
                "ORA-01555: snapshot too old: rollback segment number 7 with name \"_SYSSMU7$\" too small");

        assertThatThrownBy(() -> resolver.resolveCreateTableDdl(TABLE_ID))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("ORA-01555")
                .hasMessageContaining(TABLE_ID.toString())
                .hasMessageContaining(COMMIT_SCN.toString())
                .hasMessageNotContaining("DBMS_FLASHBACK");
    }

    @Test
    @FixFor("debezium/dbz#801")
    public void shouldSkipWithAWarningWhenHandlingModeIsWarn() throws Exception {
        final FlashbackCreateTableAsSelectResolver resolver = getResolverThatCannotRead(getConfig("warn"));

        assertThat(resolver.resolveCreateTableDdl(TABLE_ID)).isNull();
        assertThat(metrics.getWarningCount()).isEqualTo(1);
    }

    @Test
    @FixFor("debezium/dbz#801")
    public void shouldSkipQuietlyWhenHandlingModeIsSkip() throws Exception {
        final FlashbackCreateTableAsSelectResolver resolver = getResolverThatCannotRead(getConfig("skip"));

        assertThat(resolver.resolveCreateTableDdl(TABLE_ID)).isNull();
        assertThat(metrics.getWarningCount()).isZero();
    }

    @Test
    @FixFor("debezium/dbz#801")
    public void shouldTreatANonRelationalTableLikeAFailedRead() throws Exception {
        final OracleConnectorConfig config = getConfig("warn");
        final FlashbackCreateTableAsSelectResolver resolver = new FlashbackCreateTableAsSelectResolver(config, new StandardOracleConnectionFactory(config), metrics,
                COMMIT_SCN) {
            @Override
            protected String readTableDdl(TableId tableId, Scn scn) throws NonRelationalTableException {
                throw new NonRelationalTableException("Table " + tableId + " is not a relational table");
            }
        };

        assertThat(resolver.resolveCreateTableDdl(TABLE_ID)).isNull();
        assertThat(metrics.getWarningCount()).isEqualTo(1);
    }

    private FlashbackCreateTableAsSelectResolver getResolverThatCannotRead(OracleConnectorConfig config) {
        return getResolverThatCannotRead(config, "ORA-01031: insufficient privileges");
    }

    private FlashbackCreateTableAsSelectResolver getResolverThatCannotRead(OracleConnectorConfig config, String error) {
        return new FlashbackCreateTableAsSelectResolver(config, new StandardOracleConnectionFactory(config), metrics, COMMIT_SCN) {
            @Override
            protected String readTableDdl(TableId tableId, Scn scn) throws SQLException {
                throw new SQLException(error);
            }
        };
    }

    private OracleConnectorConfig getConfig(String handlingMode) {
        final OracleConnectorConfig config = new OracleConnectorConfig(TestHelper.defaultConfig()
                .with(CommonConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE, handlingMode)
                .build());

        final DefaultQueueProvider<DataChangeEvent> queueProvider = new DefaultQueueProvider<>();
        queueProvider.configure(Map.of("max.queue.size", String.valueOf(DEFAULT_MAX_QUEUE_SIZE)));
        final ChangeEventQueue<DataChangeEvent> queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(Duration.of(DEFAULT_MAX_QUEUE_SIZE, ChronoUnit.MILLIS))
                .pollDispatchInterval(Duration.of(DEFAULT_POLL_DISPATCH_INTERVAL_MILLIS, ChronoUnit.MILLIS))
                .maxBatchSize(DEFAULT_MAX_BATCH_SIZE)
                .maxQueueSize(DEFAULT_MAX_QUEUE_SIZE)
                .queueProvider(queueProvider)
                .build();

        final OracleTaskContext taskContext = Mockito.mock(OracleTaskContext.class);
        Mockito.when(taskContext.getConnectorLogicalName()).thenReturn("server1");
        Mockito.when(taskContext.getConnectorType()).thenReturn("oracle");
        Mockito.when(taskContext.getConfig()).thenReturn(config);

        metrics = new LogMinerStreamingChangeEventSourceMetrics(taskContext, queue, null, config, Collections::emptyList);
        return config;
    }
}
