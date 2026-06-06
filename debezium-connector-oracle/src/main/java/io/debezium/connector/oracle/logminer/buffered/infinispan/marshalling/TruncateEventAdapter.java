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
import io.debezium.connector.oracle.logminer.events.TruncateEvent;
import io.debezium.relational.TableId;

@ProtoAdapter(TruncateEvent.class)
public class TruncateEventAdapter extends DmlEventAdapter {

    /**
     * A ProtoStream factory that creates {@link TruncateEvent} instances.
     *
     * @param eventType the event type
     * @param scn the system change number, must not be {@code null}
     * @param tableId the fully-qualified table name
     * @param rowId the Oracle row-id the change is associated with
     * @param rsId the Oracle rollback segment identifier
     * @param changeTime the time the change occurred
     * @param oldValues old column values
     * @param newValues new column values
     * @return the constructed DmlEvent
     */
    @ProtoFactory
    public TruncateEvent factory(int eventType, String scn, String tableId, String rowId, String rsId, String changeTime,
                                 String[] oldValues, String[] newValues) {
        return new TruncateEvent(
                EventType.from(eventType),
                Scn.valueOf(scn),
                TableId.parse(tableId),
                rowId,
                rsId,
                Instant.parse(changeTime),
                oldValues,
                newValues);
    }

}
