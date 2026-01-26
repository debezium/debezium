/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.signal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlPartition;
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

    private EventDispatcher<MySqlPartition, TableId> eventDispatcher;
    private SetBinlogPositionSignal<MySqlPartition> signal;
    private MySqlOffsetContext offsetContext;
    private MySqlPartition partition;

    @BeforeEach
    public void setUp() {
        eventDispatcher = mock(EventDispatcher.class);
        offsetContext = mock(MySqlOffsetContext.class);
        partition = new MySqlPartition("test-server", "test-db");

        signal = new SetBinlogPositionSignal<>(eventDispatcher);
    }

    @Test
    public void shouldSetBinlogFileAndPosition() throws Exception {
        // Given
        String binlogFilename = "mysql-bin.000003";
        Long binlogPosition = 1234L;

        Document data = DocumentReader.defaultReader().read(
                "{\"binlog_filename\": \"" + binlogFilename + "\", \"binlog_position\": " + binlogPosition + "}");

        SignalPayload<MySqlPartition> payload = new SignalPayload<>(
                partition, "test-id", "set-binlog-position", data, offsetContext, Map.of());

        // When
        boolean result = signal.arrived(payload);

        // Then
        assertThat(result).isTrue();
        verify(offsetContext).setBinlogStartPoint(binlogFilename, binlogPosition);
        verify(eventDispatcher).alwaysDispatchHeartbeatEvent(partition, offsetContext);
    }

    @Test
    public void shouldSetGtidSet() throws Exception {
        // Given
        String gtidSet = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100";

        Document data = DocumentReader.defaultReader().read(
                "{\"gtid_set\": \"" + gtidSet + "\"}");

        SignalPayload<MySqlPartition> payload = new SignalPayload<>(
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

        SignalPayload<MySqlPartition> payload = new SignalPayload<>(
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
                "{\"binlog_filename\": \"mysql-bin.000003\"}");

        SignalPayload<MySqlPartition> payload = new SignalPayload<>(
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

        SignalPayload<MySqlPartition> payload = new SignalPayload<>(
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
                "{\"binlog_filename\": \"mysql-bin.000003\", \"binlog_position\": 1234, " +
                        "\"gtid_set\": \"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100\"}");

        SignalPayload<MySqlPartition> payload = new SignalPayload<>(
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

        SignalPayload<MySqlPartition> payload = new SignalPayload<>(
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

        SignalPayload<MySqlPartition> payload = new SignalPayload<>(
                partition, "test-id", "set-binlog-position", data, offsetContext, Map.of());

        // When/Then
        boolean result = signal.arrived(payload);
        assertThat(result).isFalse();
    }

    @Test
    public void shouldRejectNullOffsetContext() throws Exception {
        // Given
        Document data = DocumentReader.defaultReader().read(
                "{\"binlog_filename\": \"mysql-bin.000003\", \"binlog_position\": 1234}");

        SignalPayload<MySqlPartition> payload = new SignalPayload<>(
                null, "test-id", "set-binlog-position", data, null, Map.of());

        // When/Then
        assertThatThrownBy(() -> signal.arrived(payload))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("No offset context available");
    }

}
