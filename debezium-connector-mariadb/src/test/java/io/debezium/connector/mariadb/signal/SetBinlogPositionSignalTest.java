/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.signal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.connector.mariadb.MariaDbOffsetContext;
import io.debezium.connector.mariadb.MariaDbPartition;
import io.debezium.connector.mariadb.jdbc.MariaDbConnection;
import io.debezium.doc.FixFor;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.relational.TableId;

/**
 * Tests for {@link SetBinlogPositionSignal}.
 *
 * @author Debezium Authors
 */
public class SetBinlogPositionSignalTest {

    private EventDispatcher<MariaDbPartition, TableId> eventDispatcher;
    private SetBinlogPositionSignal<MariaDbPartition> signal;
    private MariaDbOffsetContext offsetContext;
    private MariaDbPartition partition;
    private MariaDbConnection connection;

    @BeforeEach
    public void setUp() {
        eventDispatcher = mock(EventDispatcher.class);
        offsetContext = mock(MariaDbOffsetContext.class);
        partition = new MariaDbPartition("test-server", "test-db");

        connection = mock(MariaDbConnection.class);

        signal = new SetBinlogPositionSignal<>(eventDispatcher, () -> connection);
    }

    @Test
    @FixFor("debezium/dbz#2731")
    public void shouldSetBinlogFileAndPosition() throws Exception {
        // Given
        String binlogFilename = "mariadb-bin.000003";
        Long binlogPosition = 1234L;

        Document data = DocumentReader.defaultReader().read(
                "{\"binlog_filename\": \"" + binlogFilename + "\", \"binlog_position\": " + binlogPosition + "}");

        SignalPayload<MariaDbPartition> payload = new SignalPayload<>(
                partition, "test-id", "set-binlog-position", data, offsetContext, Map.of());

        when(connection.gtidSetAtPosition(binlogFilename, binlogPosition)).thenReturn("0-1-57");

        // When
        boolean result = signal.arrived(payload);

        // Then
        assertThat(result).isTrue();
        verify(offsetContext).setBinlogStartPoint(binlogFilename, binlogPosition);
        verify(offsetContext).resetGtidSet();
        verify(offsetContext).setCompletedGtidSet("0-1-57");
        verify(connection).close();
        verify(eventDispatcher).alwaysDispatchHeartbeatEvent(partition, offsetContext);
    }

    @Test
    @FixFor("debezium/dbz#2731")
    public void shouldNotChangeOffsetWhenGtidSetCannotBeResolved() throws Exception {
        // Given
        String binlogFilename = "mariadb-bin.000003";
        Long binlogPosition = 1234L;

        Document data = DocumentReader.defaultReader().read(
                "{\"binlog_filename\": \"" + binlogFilename + "\", \"binlog_position\": " + binlogPosition + "}");

        SignalPayload<MariaDbPartition> payload = new SignalPayload<>(
                partition, "test-id", "set-binlog-position", data, offsetContext, Map.of());

        when(connection.gtidSetAtPosition(binlogFilename, binlogPosition)).thenThrow(new DebeziumException("binlog file not found"));

        // When / Then
        assertThatThrownBy(() -> signal.arrived(payload)).isInstanceOf(DebeziumException.class);
        verify(offsetContext, never()).setBinlogStartPoint(binlogFilename, binlogPosition);
        verify(offsetContext, never()).resetGtidSet();
    }

    @Test
    public void shouldSetGtidSet() throws Exception {
        // Given
        String gtidSet = "0-1-100";

        Document data = DocumentReader.defaultReader().read(
                "{\"gtid_set\": \"" + gtidSet + "\"}");

        SignalPayload<MariaDbPartition> payload = new SignalPayload<>(
                partition, "test-id", "set-binlog-position", data, offsetContext, Map.of());

        // When
        boolean result = signal.arrived(payload);

        // Then
        assertThat(result).isTrue();
        verify(offsetContext).setCompletedGtidSet(gtidSet);
        verify(eventDispatcher).alwaysDispatchHeartbeatEvent(partition, offsetContext);
    }

    @Test
    public void shouldRejectInvalidBinlogFilename() throws Exception {
        // Given
        Document data = DocumentReader.defaultReader().read(
                "{\"binlog_filename\": \"invalid-name\", \"binlog_position\": 1234}");

        SignalPayload<MariaDbPartition> payload = new SignalPayload<>(
                partition, "test-id", "set-binlog-position", data, offsetContext, Map.of());

        // When/Then
        assertThatThrownBy(() -> signal.arrived(payload))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Invalid binlog filename format");
    }

    @Test
    public void shouldRejectMissingPosition() throws Exception {
        // Given
        Document data = DocumentReader.defaultReader().read(
                "{\"binlog_filename\": \"mariadb-bin.000003\"}");

        SignalPayload<MariaDbPartition> payload = new SignalPayload<>(
                partition, "test-id", "set-binlog-position", data, offsetContext, Map.of());

        // When/Then
        assertThatThrownBy(() -> signal.arrived(payload))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Binlog position must be specified");
    }

    @Test
    public void shouldRejectMissingFilename() throws Exception {
        // Given
        Document data = DocumentReader.defaultReader().read(
                "{\"binlog_position\": 1234}");

        SignalPayload<MariaDbPartition> payload = new SignalPayload<>(
                partition, "test-id", "set-binlog-position", data, offsetContext, Map.of());

        // When/Then
        assertThatThrownBy(() -> signal.arrived(payload))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Binlog filename must be specified");
    }

    @Test
    public void shouldRejectBothFilePositionAndGtid() throws Exception {
        // Given
        Document data = DocumentReader.defaultReader().read(
                "{\"binlog_filename\": \"mariadb-bin.000003\", \"binlog_position\": 1234, " +
                        "\"gtid_set\": \"0-1-100\"}");

        SignalPayload<MariaDbPartition> payload = new SignalPayload<>(
                partition, "test-id", "set-binlog-position", data, offsetContext, Map.of());

        // When/Then
        assertThatThrownBy(() -> signal.arrived(payload))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Cannot specify both binlog file/position and GTID set");
    }

    @Test
    public void shouldRejectInvalidGtidSet() throws Exception {
        // Given
        Document data = DocumentReader.defaultReader().read(
                "{\"gtid_set\": \"invalid-gtid-format\"}");

        SignalPayload<MariaDbPartition> payload = new SignalPayload<>(
                partition, "test-id", "set-binlog-position", data, offsetContext, Map.of());

        // When/Then
        assertThatThrownBy(() -> signal.arrived(payload))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("Invalid GTID set format");
    }

    @Test
    public void shouldRejectEmptyData() throws Exception {
        // Given
        Document data = Document.create();

        SignalPayload<MariaDbPartition> payload = new SignalPayload<>(
                partition, "test-id", "set-binlog-position", data, offsetContext, Map.of());

        // When/Then
        boolean result = signal.arrived(payload);
        assertThat(result).isFalse();
    }

    @Test
    public void shouldRejectNullOffsetContext() throws Exception {
        // Given
        Document data = DocumentReader.defaultReader().read(
                "{\"binlog_filename\": \"mariadb-bin.000003\", \"binlog_position\": 1234}");

        SignalPayload<MariaDbPartition> payload = new SignalPayload<>(
                null, "test-id", "set-binlog-position", data, null, Map.of());

        // When/Then
        assertThatThrownBy(() -> signal.arrived(payload))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("No offset context available");
    }

}
