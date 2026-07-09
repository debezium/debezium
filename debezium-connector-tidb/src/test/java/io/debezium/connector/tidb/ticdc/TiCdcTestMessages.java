/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb.ticdc;

import java.nio.charset.StandardCharsets;

/**
 * Builders of messages in the shape produced by a TiCDC changefeed running with
 * {@code protocol=debezium}, for use in tests.
 *
 * @author Aviral Srivastava
 */
public class TiCdcTestMessages {

    private static final String ROW_FIELDS = "["
            + "{\"field\":\"id\",\"type\":\"int64\",\"optional\":false},"
            + "{\"field\":\"name\",\"type\":\"string\",\"optional\":true}"
            + "]";

    private static final String VALUE_SCHEMA_TEMPLATE = "{"
            + "\"type\":\"struct\",\"optional\":false,\"name\":\"test_cluster.DB.TABLE.Envelope\",\"fields\":["
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

    private TiCdcTestMessages() {
    }

    public static byte[] keyMessage(String db, String table, long id) {
        return bytes("{"
                + "\"schema\":{\"type\":\"struct\",\"optional\":false,\"name\":\"test_cluster." + db + "." + table + ".Key\","
                + "\"fields\":[{\"field\":\"id\",\"type\":\"int64\",\"optional\":false}]},"
                + "\"payload\":{\"id\":" + id + "}"
                + "}");
    }

    public static byte[] createMessage(String db, String table, long id, String name, long commitTs) {
        return valueMessage(db, table, "null", row(id, name), "c", commitTs);
    }

    public static byte[] updateMessage(String db, String table, long id, String oldName, String newName, long commitTs) {
        return valueMessage(db, table, row(id, oldName), row(id, newName), "u", commitTs);
    }

    public static byte[] deleteMessage(String db, String table, long id, String name, long commitTs) {
        return valueMessage(db, table, row(id, name), "null", "d", commitTs);
    }

    private static String row(long id, String name) {
        return "{\"id\":" + id + ",\"name\":\"" + name + "\"}";
    }

    private static byte[] valueMessage(String db, String table, String before, String after, String op, long commitTs) {
        final String schema = VALUE_SCHEMA_TEMPLATE.replace("DB", db).replace("TABLE", table);
        return bytes(("{\"schema\":" + schema + ",\"payload\":{"
                + "\"before\":" + before + ",\"after\":" + after + ","
                + "\"op\":\"" + op + "\",\"ts_ms\":1717000000123,"
                + "\"source\":{\"version\":\"2.4.0.Final\",\"connector\":\"TiCDC\",\"name\":\"test_cluster\","
                + "\"ts_ms\":1717000000000,\"db\":\"" + db + "\",\"table\":\"" + table + "\","
                + "\"commitTs\":" + commitTs + ",\"clusterId\":\"tidb-cluster-1\"}"
                + "}}"));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
