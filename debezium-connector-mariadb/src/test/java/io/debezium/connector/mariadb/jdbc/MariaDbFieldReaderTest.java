/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Types;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.mariadb.MariaDbConnectorConfig;
import io.debezium.doc.FixFor;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * Verifies how the snapshot reads a {@code DATETIME} value that is not a valid date, for each way the MariaDB driver
 * reports it.
 */
public class MariaDbFieldReaderTest {

    private static final String INVALID_DATETIME = "2020-02-31 10:00:00";
    private static final Column COLUMN = Column.editor().name("dt").jdbcType(Types.TIMESTAMP).type("DATETIME").create();
    private static final Table TABLE = Table.editor().tableId(new TableId("inventory", null, "events")).addColumn(COLUMN).create();

    @Test
    @FixFor("debezium/dbz#2998")
    void emitsTheEpochWhenTheDriverWrapsTheInvalidDateInAnSqlException() throws Exception {
        // MariaDB Connector/J 3.5.11 and later report an invalid date as an SQLDataException (CONJ-1351)
        ResultSet rs = resultSetThrowing(new SQLDataException("value '" + INVALID_DATETIME + "' cannot be decoded as LocalDateTime: "
                + "Invalid date 'FEBRUARY 31'", "22007", new DateTimeException("Invalid date 'FEBRUARY 31'")));

        assertThat(reader("warn").readField(rs, 1, COLUMN, TABLE)).isEqualTo(LocalDate.EPOCH.atStartOfDay());
    }

    @Test
    @FixFor("debezium/dbz#2998")
    void failsWhenTheDriverWrapsTheInvalidDateInAnSqlExceptionInFailMode() throws Exception {
        ResultSet rs = resultSetThrowing(new SQLDataException("value '" + INVALID_DATETIME + "' cannot be decoded as LocalDateTime: "
                + "Invalid date 'FEBRUARY 31'", "22007", new DateTimeException("Invalid date 'FEBRUARY 31'")));

        assertThatThrownBy(() -> reader("fail").readField(rs, 1, COLUMN, TABLE))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Invalid value in column 'dt' of table 'inventory.events': Invalid date 'FEBRUARY 31'");
    }

    @Test
    @FixFor("debezium/dbz#2998")
    void emitsTheEpochWhenTheDriverThrowsTheInvalidDateDirectly() throws Exception {
        // MariaDB Connector/J 3.5.10 and earlier throw the DateTimeException itself
        ResultSet rs = mock(ResultSet.class);
        when(rs.getString(1)).thenReturn(INVALID_DATETIME);
        when(rs.getObject(1, LocalDateTime.class)).thenThrow(new DateTimeException("Invalid date 'FEBRUARY 31'"));

        assertThat(reader("warn").readField(rs, 1, COLUMN, TABLE)).isEqualTo(LocalDate.EPOCH.atStartOfDay());
    }

    @Test
    @FixFor("debezium/dbz#2998")
    void propagatesOtherSqlExceptions() throws Exception {
        // Only an invalid date is handled; a failed read must still fail the snapshot, whatever the mode
        SQLException connectionReset = new SQLException("Connection reset", "08S01");
        ResultSet rs = resultSetThrowing(connectionReset);

        assertThatThrownBy(() -> reader("warn").readField(rs, 1, COLUMN, TABLE)).isSameAs(connectionReset);
    }

    @Test
    @FixFor("debezium/dbz#2998")
    void propagatesSqlDataExceptionsNotCausedByAnInvalidDate() throws Exception {
        SQLDataException notADate = new SQLDataException("Data type TINYINT cannot be decoded as LocalDateTime");
        ResultSet rs = resultSetThrowing(notADate);

        assertThatThrownBy(() -> reader("warn").readField(rs, 1, COLUMN, TABLE)).isSameAs(notADate);
    }

    private static ResultSet resultSetThrowing(SQLException exception) throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.getString(1)).thenReturn(INVALID_DATETIME);
        when(rs.getObject(1, LocalDateTime.class)).thenThrow(exception);
        return rs;
    }

    private static MariaDbFieldReader reader(String failureHandlingMode) {
        return new MariaDbFieldReader(new MariaDbConnectorConfig(Configuration.create()
                .with(MariaDbConnectorConfig.HOSTNAME, "localhost")
                .with(MariaDbConnectorConfig.PORT, 3306)
                .with(MariaDbConnectorConfig.USER, "mariadbuser")
                .with(MariaDbConnectorConfig.PASSWORD, "mariadbpw")
                .with(MariaDbConnectorConfig.SERVER_ID, 18765)
                .with(MariaDbConnectorConfig.TOPIC_PREFIX, "mariadb-server")
                .with(CommonConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE, failureHandlingMode)
                .build()));
    }
}
