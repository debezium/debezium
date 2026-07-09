/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb.ticdc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.data.Envelope.Operation;
import io.debezium.relational.TableId;

/**
 * Unit tests for {@link TiCdcEventParser}, using messages in the shape produced by a TiCDC
 * changefeed running with {@code protocol=debezium}.
 *
 * @author Aviral Srivastava
 */
public class TiCdcEventParserTest {

    private static final String TOPIC = "ticdc-test";

    private static final String ROW_FIELDS = "["
            + "{\"field\":\"id\",\"type\":\"int64\",\"optional\":false},"
            + "{\"field\":\"name\",\"type\":\"string\",\"optional\":true}"
            + "]";

    private static final String VALUE_SCHEMA = "{"
            + "\"type\":\"struct\",\"optional\":false,\"name\":\"test_cluster.inventory.products.Envelope\",\"fields\":["
            + "{\"field\":\"before\",\"type\":\"struct\",\"optional\":true,\"fields\":" + ROW_FIELDS + "},"
            + "{\"field\":\"after\",\"type\":\"struct\",\"optional\":true,\"fields\":" + ROW_FIELDS + "},"
            + "{\"field\":\"op\",\"type\":\"string\",\"optional\":false},"
            + "{\"field\":\"ts_ms\",\"type\":\"int64\",\"optional\":true},"
            + "{\"field\":\"source\",\"type\":\"struct\",\"optional\":false,\"fields\":["
            + "{\"field\":\"version\",\"type\":\"string\",\"optional\":false},"
            + "{\"field\":\"connector\",\"type\":\"string\",\"optional\":false},"
            + "{\"field\":\"name\",\"type\":\"string\",\"optional\":false},"
            + "{\"field\":\"ts_ms\",\"type\":\"int64\",\"optional\":false},"
            + "{\"field\":\"db\",\"type\":\"string\",\"optional\":false},"
            + "{\"field\":\"table\",\"type\":\"string\",\"optional\":false},"
            + "{\"field\":\"commitTs\",\"type\":\"int64\",\"optional\":true},"
            + "{\"field\":\"clusterId\",\"type\":\"string\",\"optional\":true}"
            + "]}"
            + "]}";

    private static final String KEY = "{"
            + "\"schema\":{\"type\":\"struct\",\"optional\":false,\"name\":\"test_cluster.inventory.products.Key\","
            + "\"fields\":[{\"field\":\"id\",\"type\":\"int64\",\"optional\":false}]},"
            + "\"payload\":{\"id\":17}"
            + "}";

    private TiCdcEventParser parser;

    @BeforeEach
    public void setUp() {
        parser = new TiCdcEventParser();
    }

    @AfterEach
    public void tearDown() {
        parser.close();
    }

    private static String sourceJson(String snapshot) {
        return "{\"version\":\"2.4.0.Final\",\"connector\":\"TiCDC\",\"name\":\"test_cluster\","
                + "\"ts_ms\":1717000000000,\"db\":\"inventory\",\"table\":\"products\","
                + "\"commitTs\":446245805252059137,\"clusterId\":\"tidb-cluster-1\"}";
    }

    private static String message(String before, String after, String op) {
        return "{\"schema\":" + VALUE_SCHEMA + ",\"payload\":{"
                + "\"before\":" + before + ",\"after\":" + after + ","
                + "\"op\":\"" + op + "\",\"ts_ms\":1717000000123,"
                + "\"source\":" + sourceJson("false")
                + "}}";
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    public void shouldParseCreateEvent() {
        final String value = message("null", "{\"id\":17,\"name\":\"scooter\"}", "c");

        final TiCdcEvent event = parser.parse(TOPIC, bytes(KEY), bytes(value));

        assertThat(event).isNotNull();
        assertThat(event.tableId()).isEqualTo(new TableId("inventory", null, "products"));
        assertThat(event.operation()).isEqualTo(Operation.CREATE);
        assertThat(event.before()).isNull();
        assertThat(event.after()).isNotNull();
        assertThat(event.after().getInt64("id")).isEqualTo(17L);
        assertThat(event.after().getString("name")).isEqualTo("scooter");
        assertThat(event.commitTs()).isEqualTo(446245805252059137L);
        assertThat(event.clusterId()).isEqualTo("tidb-cluster-1");
        assertThat(event.sourceTimestamp()).isEqualTo(Instant.ofEpochMilli(1717000000000L));
        assertThat(event.key()).isNotNull();
        assertThat(event.key().getInt64("id")).isEqualTo(17L);
        assertThat(event.rowSchema().field("id")).isNotNull();
        assertThat(event.rowSchema().field("name")).isNotNull();
    }

    @Test
    public void shouldParseUpdateEventWithBeforeImage() {
        final String value = message("{\"id\":17,\"name\":\"scooter\"}", "{\"id\":17,\"name\":\"e-scooter\"}", "u");

        final TiCdcEvent event = parser.parse(TOPIC, bytes(KEY), bytes(value));

        assertThat(event).isNotNull();
        assertThat(event.operation()).isEqualTo(Operation.UPDATE);
        assertThat(event.before().getString("name")).isEqualTo("scooter");
        assertThat(event.after().getString("name")).isEqualTo("e-scooter");
    }

    @Test
    public void shouldParseDeleteEvent() {
        final String value = message("{\"id\":17,\"name\":\"scooter\"}", "null", "d");

        final TiCdcEvent event = parser.parse(TOPIC, bytes(KEY), bytes(value));

        assertThat(event).isNotNull();
        assertThat(event.operation()).isEqualTo(Operation.DELETE);
        assertThat(event.before()).isNotNull();
        assertThat(event.after()).isNull();
    }

    @Test
    public void shouldSkipTombstone() {
        assertThat(parser.parse(TOPIC, bytes(KEY), null)).isNull();
    }

    @Test
    public void shouldSkipMessageWithoutOperation() {
        final String value = "{\"schema\":" + VALUE_SCHEMA + ",\"payload\":{"
                + "\"before\":null,\"after\":null,\"op\":null,\"ts_ms\":1717000000123,"
                + "\"source\":" + sourceJson("false") + "}}";

        assertThat(parser.parse(TOPIC, null, bytes(value))).isNull();
    }

    @Test
    public void shouldRejectMessageWithoutSchema() {
        final String value = "{\"payload\":{\"op\":\"c\"}}";

        assertThatThrownBy(() -> parser.parse(TOPIC, null, bytes(value)))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("protocol=debezium");
    }

    @Test
    public void shouldParseEventWithoutKey() {
        final String value = message("null", "{\"id\":17,\"name\":\"scooter\"}", "c");

        final TiCdcEvent event = parser.parse(TOPIC, null, bytes(value));

        assertThat(event).isNotNull();
        assertThat(event.key()).isNull();
        assertThat(event.keySchema()).isNull();
    }

    @Test
    public void shouldReadSnakeCaseExtensionFields() {
        final String value = "{\"schema\":" + VALUE_SCHEMA + ",\"payload\":{"
                + "\"before\":null,\"after\":{\"id\":17,\"name\":\"scooter\"},\"op\":\"c\",\"ts_ms\":1717000000123,"
                + "\"source\":{\"version\":\"2.4.0.Final\",\"connector\":\"TiCDC\",\"name\":\"test_cluster\","
                + "\"ts_ms\":1717000000000,\"db\":\"inventory\",\"table\":\"products\","
                + "\"commit_ts\":446245805252059138,\"cluster_id\":\"tidb-cluster-2\"}"
                + "}}";

        final TiCdcEvent event = parser.parse(TOPIC, null, bytes(value));

        assertThat(event).isNotNull();
        assertThat(event.commitTs()).isEqualTo(446245805252059138L);
        assertThat(event.clusterId()).isEqualTo("tidb-cluster-2");
    }
}
