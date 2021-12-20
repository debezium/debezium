/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.infinispan.marshalling;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

/**
 * An interface that is used by the ProtoStream framework to designate the adapters and path
 * to where the a Protocol Buffers .proto file will be generated based on the adapters
 * at compile time.
 *
 * @author Chris Cranford
 */
@AutoProtoSchemaBuilder(includeClasses = { LogMinerEventAdapter.class, DmlEventAdapter.class, SelectLobLocatorEventAdapter.class, LobWriteEventAdapter.class,
        LobEraseEventAdapter.class, LogMinerDmlEntryImplAdapter.class, TruncateEventAdapter.class }, schemaFilePath = "/")
public interface LogMinerEventMarshaller extends SerializationContextInitializer {
}
