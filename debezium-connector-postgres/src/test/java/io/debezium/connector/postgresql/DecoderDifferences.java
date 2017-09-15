/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.util.function.Supplier;

import io.debezium.connector.postgresql.connection.ReplicationConnection;

public class DecoderDifferences {

    public static int updatesWithoutPK(final int expectedCount, final int updatesWithoutPK) {
        return !ReplicationConnection.Builder.WAL2JSON_PLUGIN_NAME.equals(TestHelper.decoderPluginName()) ? expectedCount : expectedCount - updatesWithoutPK;
    }

    public static class AreQuotedIdentifiersUnsupported implements Supplier<Boolean> {
        @Override
        public Boolean get() {
            return ReplicationConnection.Builder.WAL2JSON_PLUGIN_NAME.equals(TestHelper.decoderPluginName());
        }
    }
}
