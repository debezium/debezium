/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.util.function.Supplier;

/**
 * A class that contains assertions or expected values tailored to the behaviour of a concrete decoder plugin
 *
 * @author Jiri Pechanec
 *
 */
public class DecoderDifferences {
    static final String TOASTED_VALUE_PLACEHOLDER = "__debezium_unavailable_value";

    /**
     * wal2json plugin does not send events for updates on tables that does not define primary key.
     *
     * @param expectedCount
     * @param updatesWithoutPK
     * @return modified count
     */
    public static int updatesWithoutPK(final int expectedCount, final int updatesWithoutPK) {
        return !wal2Json() ? expectedCount : expectedCount - updatesWithoutPK;
    }

    /**
     * wal2json plugin is not currently able to encode and parse quoted identifiers
     *
     */
    public static class AreQuotedIdentifiersUnsupported implements Supplier<Boolean> {
        @Override
        public Boolean get() {
            return wal2Json();
        }
    }

    /**
     * wal2json plugin sends heartbeat only at the end of transaction as the changes in a single transaction
     * are under the same LSN.
     */
    public static boolean singleHeartbeatPerTransaction() {
        return wal2Json();
    }

    private static boolean wal2Json() {
        return TestHelper.decoderPlugin() == PostgresConnectorConfig.LogicalDecoder.WAL2JSON
                || TestHelper.decoderPlugin() == PostgresConnectorConfig.LogicalDecoder.WAL2JSON_RDS
                || TestHelper.decoderPlugin() == PostgresConnectorConfig.LogicalDecoder.WAL2JSON_STREAMING
                || TestHelper.decoderPlugin() == PostgresConnectorConfig.LogicalDecoder.WAL2JSON_RDS_STREAMING;
    }

    private static boolean pgoutput() {
        return TestHelper.decoderPlugin() == PostgresConnectorConfig.LogicalDecoder.PGOUTPUT;
    }

    /**
     * wal2json plugin is not currently able to encode and parse NaN and Inf values
     *
     * @author Jiri Pechanec
     *
     */
    public static boolean areSpecialFPValuesUnsupported() {
        return wal2Json();
    }

    /**
     * wal2json plugin include toasted column in the update
     *
     * @author Jiri Pechanec
     *
     */
    public static boolean areToastedValuesPresentInSchema() {
        return !wal2Json();
    }

    public static String optionalToastedValuePlaceholder() {
        return TOASTED_VALUE_PLACEHOLDER;
    }

    public static String mandatoryToastedValuePlaceholder() {
        return TOASTED_VALUE_PLACEHOLDER;
    }
}
