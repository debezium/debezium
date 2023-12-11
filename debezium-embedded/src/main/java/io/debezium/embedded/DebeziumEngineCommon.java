/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.util.HashMap;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.engine.DebeziumEngine;

/**
 * Common implementations of {@link DebeziumEngine} interfaces which can be shared by different engine implementations.
 */
public class DebeziumEngineCommon {

    /**
     * Implementation of {@link DebeziumEngine.Offsets} which can be used to construct a {@link SourceRecord}
     * with its offsets.
     */
    public static class SourceRecordOffsets implements DebeziumEngine.Offsets {
        private HashMap<String, Object> offsets = new HashMap<>();

        /**
         * Performs {@link HashMap#put(Object, Object)} on the offsets map.
         *
         * @param key key with which to put the value
         * @param value value to be put with the key
         */
        @Override
        public void set(String key, Object value) {
            offsets.put(key, value);
        }

        /**
         * Retrieves the offsets map.
         *
         * @return HashMap of the offsets
         */
        protected HashMap<String, Object> getOffsets() {
            return offsets;
        }
    }
}
