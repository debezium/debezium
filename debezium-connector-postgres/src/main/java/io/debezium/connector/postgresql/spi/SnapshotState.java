/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.spi;

public class SnapshotState {
    public final OffsetState offset;
    public final SlotState slot;
    private SlotCreationResult slotCreationResult = null;

    public SnapshotState(OffsetState offset, SlotState slot) {
        super();
        this.offset = offset;
        this.slot = slot;
    }

    public SlotCreationResult getSlotCreationResult() {
        return slotCreationResult;
    }

    public void setSlotCreationResult(SlotCreationResult slotCreationResult) {
        this.slotCreationResult = slotCreationResult;
    }
}
