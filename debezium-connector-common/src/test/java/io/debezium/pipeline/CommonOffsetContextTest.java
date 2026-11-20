/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import static io.debezium.pipeline.PositionOffsetContext.POSITION_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;
import io.debezium.pipeline.txmetadata.DefaultTransactionInfo;
import io.debezium.pipeline.txmetadata.TransactionContext;

public class CommonOffsetContextTest {

    @Test
    @FixFor("debezium/dbz#2549")
    public void shouldUseCurrentOffsetForIncompleteEventUntilAnOffsetIsEmitted() {
        final PositionOffsetContext offsetContext = new PositionOffsetContext(1L);

        assertThat(offsetContext.getOffsetForIncompleteEvent()).isEqualTo(Map.of(POSITION_KEY, 1L));
    }

    @Test
    @FixFor("debezium/dbz#2549")
    public void shouldUseLastEmittedOffsetForIncompleteEvent() {
        final PositionOffsetContext offsetContext = new PositionOffsetContext(1L);
        offsetContext.markOffsetEmitted(offsetContext.getOffset());

        // The connector advances to the next source event, whose records are not emitted yet
        offsetContext.setPosition(2L);

        assertThat(offsetContext.getOffsetForIncompleteEvent()).isEqualTo(Map.of(POSITION_KEY, 1L));
        assertThat(offsetContext.getOffset()).isEqualTo(Map.of(POSITION_KEY, 2L));
    }

    @Test
    @FixFor("debezium/dbz#2549")
    public void shouldStoreTransactionContextWithoutModifyingEmittedOffset() {
        final PositionOffsetContext offsetContext = new PositionOffsetContext(1L);
        final Map<String, ?> emittedOffset = offsetContext.getOffset();
        offsetContext.markOffsetEmitted(emittedOffset);
        offsetContext.setPosition(2L);

        offsetContext.getTransactionContext().beginTransaction(new DefaultTransactionInfo("tx-1"));
        offsetContext.updateTransactionContextForIncompleteEvent();

        // The emitted offset belongs to a record that may still be in the queue
        assertThat(emittedOffset).isEqualTo(Map.of(POSITION_KEY, 1L));
        assertThat(offsetContext.getOffsetForIncompleteEvent()).isEqualTo(Map.of(
                POSITION_KEY, 1L,
                TransactionContext.OFFSET_TRANSACTION_ID, "tx-1"));
        assertThatThrownBy(() -> offsetContext.getOffsetForIncompleteEvent().remove(POSITION_KEY))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
