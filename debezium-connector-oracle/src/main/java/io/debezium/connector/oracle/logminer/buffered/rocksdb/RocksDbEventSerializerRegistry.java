/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.rocksdb;

import java.util.HashMap;
import java.util.Map;

import io.debezium.connector.oracle.logminer.buffered.rocksdb.serialization.DmlEventSerializer;
import io.debezium.connector.oracle.logminer.buffered.rocksdb.serialization.ExtendedStringBeginEventSerializer;
import io.debezium.connector.oracle.logminer.buffered.rocksdb.serialization.ExtendedStringWriteEventSerializer;
import io.debezium.connector.oracle.logminer.buffered.rocksdb.serialization.LobEraseEventSerializer;
import io.debezium.connector.oracle.logminer.buffered.rocksdb.serialization.LobWriteEventSerializer;
import io.debezium.connector.oracle.logminer.buffered.rocksdb.serialization.RedoSqlDmlEventSerializer;
import io.debezium.connector.oracle.logminer.buffered.rocksdb.serialization.SelectLobLocatorEventSerializer;
import io.debezium.connector.oracle.logminer.buffered.rocksdb.serialization.TruncateEventSerializer;
import io.debezium.connector.oracle.logminer.buffered.rocksdb.serialization.XmlBeginEventSerializer;
import io.debezium.connector.oracle.logminer.buffered.rocksdb.serialization.XmlEndEventSerializer;
import io.debezium.connector.oracle.logminer.buffered.rocksdb.serialization.XmlWriteEventSerializer;
import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.events.ExtendedStringBeginEvent;
import io.debezium.connector.oracle.logminer.events.ExtendedStringWriteEvent;
import io.debezium.connector.oracle.logminer.events.LobEraseEvent;
import io.debezium.connector.oracle.logminer.events.LobWriteEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.RedoSqlDmlEvent;
import io.debezium.connector.oracle.logminer.events.SelectLobLocatorEvent;
import io.debezium.connector.oracle.logminer.events.TruncateEvent;
import io.debezium.connector.oracle.logminer.events.XmlBeginEvent;
import io.debezium.connector.oracle.logminer.events.XmlEndEvent;
import io.debezium.connector.oracle.logminer.events.XmlWriteEvent;

/**
 * Registry for mapping LogMinerEvent classes to their corresponding RocksDB serializers.
 *
 * @author Debezium Authors
 */
public class RocksDbEventSerializerRegistry {

    private static final Map<Class<? extends LogMinerEvent>, RocksDbEventSerializer.EventSerializerStrategy> SERIALIZER_REGISTRY = createSerializerRegistry();
    private static final Map<Byte, RocksDbEventSerializer.EventSerializerStrategy> TYPE_ID_REGISTRY = createTypeIdRegistry();
    /**
     * Map from type id to the serializer's class name. Helpful for debugging/logging.
     */
    private static final Map<Byte, String> TYPE_ID_TO_CLASS_NAME = createTypeIdToClassNameRegistry();

    private static Map<Class<? extends LogMinerEvent>, RocksDbEventSerializer.EventSerializerStrategy> createSerializerRegistry() {
        final Map<Class<? extends LogMinerEvent>, RocksDbEventSerializer.EventSerializerStrategy> map = new HashMap<>();

        // Explicitly map event classes to their serializers. This is clearer and easier to
        // extend than relying on instanceof checks during registration.
        map.put(DmlEvent.class, new DmlEventSerializer());
        map.put(SelectLobLocatorEvent.class, new SelectLobLocatorEventSerializer());
        map.put(ExtendedStringBeginEvent.class, new ExtendedStringBeginEventSerializer());
        map.put(LobWriteEvent.class, new LobWriteEventSerializer());
        map.put(LobEraseEvent.class, new LobEraseEventSerializer());
        map.put(TruncateEvent.class, new TruncateEventSerializer());
        map.put(XmlBeginEvent.class, new XmlBeginEventSerializer());
        map.put(XmlWriteEvent.class, new XmlWriteEventSerializer());
        map.put(XmlEndEvent.class, new XmlEndEventSerializer());
        map.put(RedoSqlDmlEvent.class, new RedoSqlDmlEventSerializer());
        map.put(ExtendedStringWriteEvent.class, new ExtendedStringWriteEventSerializer());

        return java.util.Collections.unmodifiableMap(map);
    }

    private static Map<Byte, RocksDbEventSerializer.EventSerializerStrategy> createTypeIdRegistry() {
        final Map<Byte, RocksDbEventSerializer.EventSerializerStrategy> map = new HashMap<>();

        // Populate type id -> serializer map from the serializer instances in the serializer registry
        for (RocksDbEventSerializer.EventSerializerStrategy serializer : SERIALIZER_REGISTRY.values()) {
            map.put(serializer.getTypeId(), serializer);
        }

        // Add generic serializer for base LogMinerEvent class
        map.put(RocksDbEventSerializer.TYPE_GENERIC_LOG_MINER_EVENT, new RocksDbEventSerializer.GenericLogMinerEventSerializer());

        return java.util.Collections.unmodifiableMap(map);
    }

    private static Map<Byte, String> createTypeIdToClassNameRegistry() {
        final Map<Byte, String> map = new HashMap<>();
        for (Map.Entry<Byte, RocksDbEventSerializer.EventSerializerStrategy> e : createTypeIdRegistry().entrySet()) {
            map.put(e.getKey(), e.getValue().getClass().getName());
        }
        return java.util.Collections.unmodifiableMap(map);
    }

    public static RocksDbEventSerializer.EventSerializerStrategy getSerializer(Class<? extends LogMinerEvent> eventClass) {
        RocksDbEventSerializer.EventSerializerStrategy serializer = SERIALIZER_REGISTRY.get(eventClass);
        if (serializer != null) {
            return serializer;
        }
        // For base LogMinerEvent class, return the generic serializer instance
        if (eventClass == LogMinerEvent.class) {
            return TYPE_ID_REGISTRY.get(RocksDbEventSerializer.TYPE_GENERIC_LOG_MINER_EVENT);
        }
        // Default to DML serializer for unknown types (preserve previous behaviour)
        return SERIALIZER_REGISTRY.get(DmlEvent.class);
    }

    public static RocksDbEventSerializer.EventSerializerStrategy getDeserializer(byte typeId) {
        RocksDbEventSerializer.EventSerializerStrategy deserializer = TYPE_ID_REGISTRY.get(typeId);
        if (deserializer == null) {
            String className = TYPE_ID_TO_CLASS_NAME.get(typeId);
            throw new IllegalArgumentException("Unknown event type ID for RocksDB deserialization: " + typeId
                    + (className != null ? (" (expected: " + className + ")") : ""));
        }
        return deserializer;
    }

    /**
     * Returns the serializer class name associated with a type id, if available.
     */
    public static String getSerializerClassNameForTypeId(byte typeId) {
        return TYPE_ID_TO_CLASS_NAME.get(typeId);
    }
}
