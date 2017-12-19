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
     * @author Jiri Pechanec
     *
     */
    public static class AreQuotedIdentifiersUnsupported implements Supplier<Boolean> {
        @Override
        public Boolean get() {
            return wal2Json();
        }
    }

    private static boolean wal2Json() {
        return TestHelper.decoderPlugin() == PostgresConnectorConfig.LogicalDecoder.WAL2JSON || TestHelper.decoderPlugin() == PostgresConnectorConfig.LogicalDecoder.WAL2JSON_RDS;
    }

}
