/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.chronicle;

import java.util.HashMap;
import java.util.Map;

import io.debezium.connector.oracle.logminer.buffered.chronicle.serialization.DmlEventSerializer;
import io.debezium.connector.oracle.logminer.buffered.chronicle.serialization.ExtendedStringBeginEventSerializer;
import io.debezium.connector.oracle.logminer.buffered.chronicle.serialization.ExtendedStringWriteEventSerializer;
import io.debezium.connector.oracle.logminer.buffered.chronicle.serialization.LobEraseEventSerializer;
import io.debezium.connector.oracle.logminer.buffered.chronicle.serialization.LobWriteEventSerializer;
import io.debezium.connector.oracle.logminer.buffered.chronicle.serialization.RedoSqlDmlEventSerializer;
import io.debezium.connector.oracle.logminer.buffered.chronicle.serialization.SelectLobLocatorEventSerializer;
import io.debezium.connector.oracle.logminer.buffered.chronicle.serialization.TruncateEventSerializer;
import io.debezium.connector.oracle.logminer.buffered.chronicle.serialization.XmlBeginEventSerializer;
import io.debezium.connector.oracle.logminer.buffered.chronicle.serialization.XmlEndEventSerializer;
import io.debezium.connector.oracle.logminer.buffered.chronicle.serialization.XmlWriteEventSerializer;
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
 * Registry for mapping LogMinerEvent classes to their corresponding serializers.
 *
 * @author Debezium Authors
 */
public class ChronicleEventSerializerRegistry {

    private static final String UNKNOWN_TYPE_ID_FORMAT = "Unknown Type ID: %d";
    private static final Map<Class<? extends LogMinerEvent>, ChronicleEventSerializer.EventSerializerStrategy> SERIALIZER_REGISTRY = createSerializerRegistry();
    private static final Map<Byte, ChronicleEventSerializer.EventSerializerStrategy> TYPE_ID_REGISTRY = createTypeIdRegistry();

    private static final Map<Byte, String> TYPE_ID_TO_CLASS_NAME = createTypeIdToClassNameRegistry();

    private static Map<Class<? extends LogMinerEvent>, ChronicleEventSerializer.EventSerializerStrategy> createSerializerRegistry() {
        Map<Class<? extends LogMinerEvent>, ChronicleEventSerializer.EventSerializerStrategy> registry = new HashMap<>();

        registry.put(DmlEvent.class, new DmlEventSerializer());
        registry.put(SelectLobLocatorEvent.class, new SelectLobLocatorEventSerializer());
        registry.put(ExtendedStringBeginEvent.class, new ExtendedStringBeginEventSerializer());
        registry.put(XmlBeginEvent.class, new XmlBeginEventSerializer());
        registry.put(XmlWriteEvent.class, new XmlWriteEventSerializer());
        registry.put(XmlEndEvent.class, new XmlEndEventSerializer());
        registry.put(LobWriteEvent.class, new LobWriteEventSerializer());
        registry.put(TruncateEvent.class, new TruncateEventSerializer());
        registry.put(RedoSqlDmlEvent.class, new RedoSqlDmlEventSerializer());
        registry.put(LobEraseEvent.class, new LobEraseEventSerializer());
        registry.put(ExtendedStringWriteEvent.class, new ExtendedStringWriteEventSerializer());

        return registry;
    }

    private static Map<Byte, ChronicleEventSerializer.EventSerializerStrategy> createTypeIdRegistry() {
        Map<Byte, ChronicleEventSerializer.EventSerializerStrategy> registry = new HashMap<>();

        registry.put(ChronicleEventSerializer.TYPE_DML_EVENT, new DmlEventSerializer());
        registry.put(ChronicleEventSerializer.TYPE_SELECT_LOB_LOCATOR, new SelectLobLocatorEventSerializer());
        registry.put(ChronicleEventSerializer.TYPE_EXTENDED_STRING_BEGIN, new ExtendedStringBeginEventSerializer());
        registry.put(ChronicleEventSerializer.TYPE_XML_BEGIN, new XmlBeginEventSerializer());
        registry.put(ChronicleEventSerializer.TYPE_XML_WRITE, new XmlWriteEventSerializer());
        registry.put(ChronicleEventSerializer.TYPE_XML_END, new XmlEndEventSerializer());
        registry.put(ChronicleEventSerializer.TYPE_LOB_WRITE, new LobWriteEventSerializer());
        registry.put(ChronicleEventSerializer.TYPE_TRUNCATE, new TruncateEventSerializer());
        registry.put(ChronicleEventSerializer.TYPE_REDO_SQL_DML, new RedoSqlDmlEventSerializer());
        registry.put(ChronicleEventSerializer.TYPE_LOB_ERASE, new LobEraseEventSerializer());
        registry.put(ChronicleEventSerializer.TYPE_EXTENDED_STRING_WRITE, new ExtendedStringWriteEventSerializer());
        registry.put(ChronicleEventSerializer.TYPE_GENERIC_LOG_MINER_EVENT, new ChronicleEventSerializer.GenericLogMinerEventSerializer());

        return registry;
    }

    private static Map<Byte, String> createTypeIdToClassNameRegistry() {
        Map<Byte, String> registry = new HashMap<>();
        registry.put(ChronicleEventSerializer.TYPE_DML_EVENT, DmlEvent.class.getName());
        registry.put(ChronicleEventSerializer.TYPE_SELECT_LOB_LOCATOR, SelectLobLocatorEvent.class.getName());
        registry.put(ChronicleEventSerializer.TYPE_EXTENDED_STRING_BEGIN, ExtendedStringBeginEvent.class.getName());
        registry.put(ChronicleEventSerializer.TYPE_XML_BEGIN, XmlBeginEvent.class.getName());
        registry.put(ChronicleEventSerializer.TYPE_XML_WRITE, XmlWriteEvent.class.getName());
        registry.put(ChronicleEventSerializer.TYPE_XML_END, XmlEndEvent.class.getName());
        registry.put(ChronicleEventSerializer.TYPE_LOB_WRITE, LobWriteEvent.class.getName());
        registry.put(ChronicleEventSerializer.TYPE_TRUNCATE, TruncateEvent.class.getName());
        registry.put(ChronicleEventSerializer.TYPE_REDO_SQL_DML, RedoSqlDmlEvent.class.getName());
        registry.put(ChronicleEventSerializer.TYPE_LOB_ERASE, LobEraseEvent.class.getName());
        registry.put(ChronicleEventSerializer.TYPE_EXTENDED_STRING_WRITE, ExtendedStringWriteEvent.class.getName());
        registry.put(ChronicleEventSerializer.TYPE_GENERIC_LOG_MINER_EVENT, LogMinerEvent.class.getName());
        return registry;
    }

    /**
     * Get the class name for a given type ID for debugging purposes.
     *
     * @param typeId the type identifier
     * @return the corresponding class name, or "Unknown" if not found
     */
    public static String getClassName(byte typeId) {
        return TYPE_ID_TO_CLASS_NAME.getOrDefault(typeId, String.format(UNKNOWN_TYPE_ID_FORMAT, typeId));
    }

    /**
     * Get the appropriate serializer for a given event class.
     *
     * @param eventClass the event class
     * @return the corresponding serializer strategy
     */
    public static ChronicleEventSerializer.EventSerializerStrategy getSerializer(Class<? extends LogMinerEvent> eventClass) {
        ChronicleEventSerializer.EventSerializerStrategy serializer = SERIALIZER_REGISTRY.get(eventClass);
        if (serializer == null) {
            // For base LogMinerEvent class, use a generic serializer
            if (eventClass == LogMinerEvent.class) {
                return new ChronicleEventSerializer.GenericLogMinerEventSerializer();
            }
            // Default to DML serializer for unknown types
            return new DmlEventSerializer();
        }
        return serializer;
    }

    /**
     * Get the appropriate deserializer for a given type ID.
     *
     * @param typeId the type identifier
     * @return the corresponding serializer strategy
     */
    public static ChronicleEventSerializer.EventSerializerStrategy getDeserializer(byte typeId) {
        ChronicleEventSerializer.EventSerializerStrategy deserializer = TYPE_ID_REGISTRY.get(typeId);
        if (deserializer == null) {
            throw new IllegalArgumentException("Unknown event type ID: " + typeId);
        }
        return deserializer;
    }
}
