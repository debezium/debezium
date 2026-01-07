/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.infinispan.marshalling;

import java.time.Instant;

import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LobEraseEvent;
import io.debezium.relational.TableId;

/**
 * An Infinispan ProtoStream adapter to marshall {@link LobEraseEvent} instances.
 *
 * This class defines a factory for creating {@link LobEraseEvent} instances when hydrating
 * records from the persisted datastore asa well as field handlers to extract values
 * to be marshalled to the protocol buffer stream.
 *
 * @see LogMinerEventAdapter for the structure format of this adapter.
 * @author Chris Cranford
 */
@ProtoAdapter(LobEraseEvent.class)
public class LobEraseEventAdapter extends LogMinerEventAdapter {

    /**
     * A ProtoStream factory that creates {@link LobEraseEvent} instances.
     *
     * @param eventType the event type
     * @param scn the system change number, must not be {@code null}
     * @param tableId the fully-qualified table name
     * @param rowId the Oracle row-id the change is associated with
     * @param rsId the Oracle rollback segment identifier
     * @param changeTime the time the change occurred
     * @return the constructed LobEraseEvent
     */
    @ProtoFactory
    public LobEraseEvent factory(int eventType, String scn, String tableId, String rowId, String rsId, String changeTime) {
        return new LobEraseEvent(EventType.from(eventType), Scn.valueOf(scn), TableId.parse(tableId), rowId, rsId, Instant.parse(changeTime));
    }
}
