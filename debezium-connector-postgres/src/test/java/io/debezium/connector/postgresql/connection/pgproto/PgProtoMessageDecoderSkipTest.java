/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgproto;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.connector.postgresql.connection.TransactionMessage;
import io.debezium.connector.postgresql.connection.WalPositionLocator;
import io.debezium.connector.postgresql.proto.PgProto;
import io.debezium.connector.postgresql.proto.PgProto.Op;
import io.debezium.doc.FixFor;

/**
 * The decoderbufs plugin reports the BEGIN of a transaction and the rows that follow it at the
 * same LSN. The BEGIN is not an event {@code lsn_events_processed} counts, so it must not use up
 * one of the skips that are reserved for the rows which were already processed at the stored LSN.
 */
class PgProtoMessageDecoderSkipTest {

    private static final Lsn LAST_COMMIT_STORED_LSN = Lsn.valueOf(100L);
    private static final Lsn SHARED_LSN = Lsn.valueOf(140L);
    private static final Lsn LATER_LSN = Lsn.valueOf(150L);

    @Test
    @FixFor("debezium/dbz#2894")
    void beginSharingStoredLsnShouldNotUseUpSkipOfAlreadyProcessedRow() {
        final WalPositionLocator locator = locatorWithTwoProcessedRowsAtStoredLsn();
        final PgProtoMessageDecoder decoder = new PgProtoMessageDecoder();

        // The BEGIN of the transaction is replayed at the stored LSN. It does not stand for an
        // already processed row, so it must not use up one of the two skips reserved for the rows.
        assertThat(decoder.shouldMessageBeSkipped(buffer(Op.BEGIN), SHARED_LSN, SHARED_LSN, locator))
                .as("The BEGIN must always be reprocessed")
                .isFalse();

        // The first row was already processed.
        assertThat(decoder.shouldMessageBeSkipped(buffer(Op.INSERT), SHARED_LSN, SHARED_LSN, locator))
                .as("The first already processed row must be skipped")
                .isTrue();

        // The second row was already processed as well, so it must not be emitted again.
        assertThat(decoder.shouldMessageBeSkipped(buffer(Op.INSERT), SHARED_LSN, SHARED_LSN, locator))
                .as("The second already processed row must be skipped and not emitted again")
                .isTrue();

        // Only the third row was not processed yet.
        assertThat(decoder.shouldMessageBeSkipped(buffer(Op.INSERT), SHARED_LSN, SHARED_LSN, locator))
                .as("The row that was not processed must be emitted")
                .isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2894")
    void commitSharingStoredLsnShouldNotUseUpSkipOfAlreadyProcessedRow() {
        final WalPositionLocator locator = locatorWithTwoProcessedRowsAtStoredLsn();
        final PgProtoMessageDecoder decoder = new PgProtoMessageDecoder();

        // The COMMIT of the transaction is replayed at the stored LSN. It does not stand for an
        // already processed row, so it must not use up one of the two skips reserved for the rows.
        assertThat(decoder.shouldMessageBeSkipped(buffer(Op.COMMIT), SHARED_LSN, SHARED_LSN, locator))
                .as("The COMMIT must always be reprocessed")
                .isFalse();

        assertThat(decoder.shouldMessageBeSkipped(buffer(Op.INSERT), SHARED_LSN, SHARED_LSN, locator))
                .as("The first already processed row must be skipped")
                .isTrue();

        assertThat(decoder.shouldMessageBeSkipped(buffer(Op.INSERT), SHARED_LSN, SHARED_LSN, locator))
                .as("The second already processed row must be skipped and not emitted again")
                .isTrue();

        assertThat(decoder.shouldMessageBeSkipped(buffer(Op.INSERT), SHARED_LSN, SHARED_LSN, locator))
                .as("The row that was not processed must be emitted")
                .isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2894")
    void rowAtDifferentLsnIsNotSkipped() {
        final WalPositionLocator locator = locatorWithTwoProcessedRowsAtStoredLsn();
        final PgProtoMessageDecoder decoder = new PgProtoMessageDecoder();

        // The replay of the stored LSN ends with the row that was not processed before.
        assertThat(decoder.shouldMessageBeSkipped(buffer(Op.INSERT), SHARED_LSN, SHARED_LSN, locator))
                .as("The first already processed row must be skipped")
                .isTrue();
        assertThat(decoder.shouldMessageBeSkipped(buffer(Op.INSERT), SHARED_LSN, SHARED_LSN, locator))
                .as("The second already processed row must be skipped")
                .isTrue();
        assertThat(decoder.shouldMessageBeSkipped(buffer(Op.INSERT), SHARED_LSN, SHARED_LSN, locator))
                .as("The row that was not processed must be emitted")
                .isFalse();

        // A row that arrives later, at another LSN, is not skipped: the decoder does not skip rows
        // on its own account, the locator decides, and it filters on the LSN.
        assertThat(decoder.shouldMessageBeSkipped(buffer(Op.INSERT), LATER_LSN, SHARED_LSN, locator))
                .as("A row at another LSN must not be skipped")
                .isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2894")
    void messageWithoutOpDoesNotSpendSkip() {
        final WalPositionLocator locator = locatorWithTwoProcessedRowsAtStoredLsn();
        final PgProtoMessageDecoder decoder = new PgProtoMessageDecoder();

        // A message with an operation this version does not know is dropped when it is processed,
        // so it never counts as an event lsn_events_processed counts.
        assertThat(decoder.shouldMessageBeSkipped(bufferWithoutOp(), SHARED_LSN, SHARED_LSN, locator))
                .as("The message with an unknown operation must be skipped")
                .isTrue();

        assertThat(decoder.shouldMessageBeSkipped(buffer(Op.INSERT), SHARED_LSN, SHARED_LSN, locator))
                .as("The first already processed row must be skipped")
                .isTrue();

        assertThat(decoder.shouldMessageBeSkipped(buffer(Op.INSERT), SHARED_LSN, SHARED_LSN, locator))
                .as("The second already processed row must be skipped")
                .isTrue();

        assertThat(decoder.shouldMessageBeSkipped(buffer(Op.INSERT), SHARED_LSN, SHARED_LSN, locator))
                .as("The row that was not processed must be emitted")
                .isFalse();
    }

    @ParameterizedTest
    @EnumSource(Op.class)
    @FixFor("debezium/dbz#2894")
    void onlyRowOperationsSpendASkip(Op op) {
        final WalPositionLocator locator = locatorWithTwoProcessedRowsAtStoredLsn();
        final PgProtoMessageDecoder decoder = new PgProtoMessageDecoder();

        final boolean skipped = decoder.shouldMessageBeSkipped(buffer(op), SHARED_LSN, SHARED_LSN, locator);

        // The two rows that follow tell whether the operation under test used up one of the two
        // skips that are reserved for the already processed rows.
        final boolean firstRowSkipped = decoder.shouldMessageBeSkipped(buffer(Op.INSERT), SHARED_LSN, SHARED_LSN, locator);
        final boolean secondRowSkipped = decoder.shouldMessageBeSkipped(buffer(Op.INSERT), SHARED_LSN, SHARED_LSN, locator);

        switch (op) {
            case BEGIN, COMMIT -> {
                assertThat(skipped).as("A %s must always be reprocessed", op).isFalse();
                assertThat(firstRowSkipped).as("A %s must not use up a skip", op).isTrue();
                assertThat(secondRowSkipped).as("A %s must not use up a skip", op).isTrue();
            }
            case INSERT, UPDATE, DELETE -> {
                assertThat(skipped).as("A %s at the stored LSN must be skipped", op).isTrue();
                assertThat(firstRowSkipped).as("A %s must use up one of the two skips", op).isTrue();
                assertThat(secondRowSkipped).as("A %s must use up one of the two skips", op).isFalse();
            }
            default -> {
                assertThat(skipped).as("An operation this version does not know must be skipped").isTrue();
                assertThat(firstRowSkipped).as("An unknown operation must not use up a skip").isTrue();
                assertThat(secondRowSkipped).as("An unknown operation must not use up a skip").isTrue();
            }
        }
    }

    @Test
    @FixFor("debezium/dbz#2894")
    void bufferWithoutArrayIsRejected() {
        final WalPositionLocator locator = locatorWithTwoProcessedRowsAtStoredLsn();
        final PgProtoMessageDecoder decoder = new PgProtoMessageDecoder();

        // A direct buffer has no backing array, so the message cannot be read from it.
        final ByteBuffer buffer = ByteBuffer.allocateDirect(16);

        assertThatThrownBy(() -> decoder.shouldMessageBeSkipped(buffer, SHARED_LSN, SHARED_LSN, locator))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Invalid buffer received from Postgres server during streaming replication");
    }

    @Test
    @FixFor("debezium/dbz#2894")
    void bufferWithNonZeroArrayOffsetIsReadCorrectly() {
        final WalPositionLocator locator = locatorWithTwoProcessedRowsAtStoredLsn();
        final PgProtoMessageDecoder decoder = new PgProtoMessageDecoder();

        final byte[] content = message(Op.BEGIN);
        final byte[] padded = new byte[content.length + 2];
        System.arraycopy(content, 0, padded, 2, content.length);
        final ByteBuffer buffer = ByteBuffer.wrap(padded).position(2).slice();
        assertThat(buffer.arrayOffset()).isEqualTo(2);

        // The operation is read from the message that starts inside the array, so the BEGIN is
        // reprocessed just like it is when it starts at the beginning of the array.
        assertThat(decoder.shouldMessageBeSkipped(buffer, SHARED_LSN, SHARED_LSN, locator))
                .as("The BEGIN must always be reprocessed")
                .isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2894")
    void malformedMessageIsRejected() {
        final WalPositionLocator locator = locatorWithTwoProcessedRowsAtStoredLsn();
        final PgProtoMessageDecoder decoder = new PgProtoMessageDecoder();

        // The varint of the transaction id is cut off, so the message is not a valid protobuf.
        final ByteBuffer buffer = ByteBuffer.wrap(new byte[]{ 0x08 });

        assertThatThrownBy(() -> decoder.shouldMessageBeSkipped(buffer, SHARED_LSN, SHARED_LSN, locator))
                .isInstanceOf(ConnectException.class);
    }

    @Test
    @FixFor("debezium/dbz#2894")
    void messageWithoutBufferIsTolerated() {
        final WalPositionLocator locator = locatorWithTwoProcessedRowsAtStoredLsn();
        final PgProtoMessageDecoder decoder = new PgProtoMessageDecoder();

        // The replication stream returns no buffer when it received no message, so there is no
        // operation to read and the decision is the one the base class makes.
        assertThat(decoder.shouldMessageBeSkipped(null, SHARED_LSN, SHARED_LSN, locator))
                .as("A message that was not received is skipped the way the base class decides")
                .isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2894")
    void opIsReadFromEveryBuffer() {
        final WalPositionLocator locator = locatorWithTwoProcessedRowsAtStoredLsn();
        final PgProtoMessageDecoder decoder = new PgProtoMessageDecoder();

        // The decoder keeps the message it parsed for the skip decision, so the operation of a
        // message must be read from the buffer of that message and not from the previous one.
        assertThat(decoder.shouldMessageBeSkipped(buffer(Op.INSERT), SHARED_LSN, SHARED_LSN, locator))
                .as("The first already processed row must be skipped")
                .isTrue();

        assertThat(decoder.shouldMessageBeSkipped(buffer(Op.BEGIN), SHARED_LSN, SHARED_LSN, locator))
                .as("The BEGIN must always be reprocessed")
                .isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2894")
    void opIsReadFromTheContentOfTheBuffer() {
        final WalPositionLocator locator = locatorWithTwoProcessedRowsAtStoredLsn();
        final PgProtoMessageDecoder decoder = new PgProtoMessageDecoder();

        // The driver may hand out the same buffer again with another message in it. The messages
        // below carry the operation only, so that they have the same size and can replace each
        // other in the buffer.
        final byte[] beginMessage = messageWithOperationOnly(Op.BEGIN);
        final byte[] insertMessage = messageWithOperationOnly(Op.INSERT);
        assertThat(insertMessage.length).isEqualTo(beginMessage.length);

        final ByteBuffer buffer = ByteBuffer.wrap(beginMessage);
        assertThat(decoder.shouldMessageBeSkipped(buffer, SHARED_LSN, SHARED_LSN, locator))
                .as("The BEGIN must always be reprocessed")
                .isFalse();

        // The buffer holds a row now, in the same instance the decision above was taken for.
        System.arraycopy(insertMessage, 0, buffer.array(), 0, insertMessage.length);

        // The decision has to be taken from the content of the buffer, not from the message that
        // was parsed before.
        assertThat(decoder.shouldMessageBeSkipped(buffer, SHARED_LSN, SHARED_LSN, locator))
                .as("The row in the buffer must be skipped")
                .isTrue();

        // The row spent one of the two skips, so one more row at the stored LSN is skipped and the
        // row that was not processed before is emitted.
        assertThat(decoder.shouldMessageBeSkipped(buffer(Op.INSERT), SHARED_LSN, SHARED_LSN, locator))
                .as("The next row at the stored LSN must be skipped")
                .isTrue();
        assertThat(decoder.shouldMessageBeSkipped(buffer(Op.INSERT), SHARED_LSN, SHARED_LSN, locator))
                .as("The row that was not processed must be emitted")
                .isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2894")
    void processedMessageIsTheOneOfItsOwnBuffer() throws Exception {
        final WalPositionLocator locator = locatorWithTwoProcessedRowsAtStoredLsn();
        final PgProtoMessageDecoder decoder = new PgProtoMessageDecoder();

        decoder.shouldMessageBeSkipped(buffer(Op.BEGIN), SHARED_LSN, SHARED_LSN, locator);

        // A message that is processed without a preceding skip decision must be read from its own
        // buffer and not from the one the skip decision parsed.
        final List<ReplicationMessage> processed = new ArrayList<>();
        // Only the operation of the message is read, so the type registry is not needed.
        decoder.processMessage(buffer(Op.INSERT), processed::add, null);

        assertThat(processed).hasSize(1);
        assertThat(processed.get(0).getOperation()).isEqualTo(ReplicationMessage.Operation.INSERT);
    }

    /**
     * Builds the locator for an offset that was stored after the second of three rows which share
     * the LSN of the BEGIN of their transaction, and drives it through the location phase.
     */
    private WalPositionLocator locatorWithTwoProcessedRowsAtStoredLsn() {
        final WalPositionLocator locator = new WalPositionLocator(LAST_COMMIT_STORED_LSN, SHARED_LSN,
                ReplicationMessage.Operation.INSERT, 2);

        // The location phase sees the BEGIN and three rows, all of them at the stored LSN.
        assertThat(locator.resumeFromLsn(SHARED_LSN, createBeginMessage())).isEmpty();
        assertThat(locator.resumeFromLsn(SHARED_LSN, createInsertMessage())).isEmpty(); // row #1
        assertThat(locator.resumeFromLsn(SHARED_LSN, createInsertMessage())).isEmpty(); // row #2

        // The third row exceeds the processed count, so the streaming restarts at the stored LSN
        // and the two already processed rows have to be skipped during the replay.
        final Optional<Lsn> resumeLsn = locator.resumeFromLsn(SHARED_LSN, createInsertMessage()); // row #3
        assertThat(resumeLsn).hasValue(SHARED_LSN);

        locator.enableFiltering();
        return locator;
    }

    private ReplicationMessage createBeginMessage() {
        return new TransactionMessage(ReplicationMessage.Operation.BEGIN, 1L, Instant.now());
    }

    private ReplicationMessage createInsertMessage() {
        return new ReplicationMessage.NoopMessage(1L, Instant.now(), ReplicationMessage.Operation.INSERT);
    }

    private ByteBuffer buffer(Op op) {
        return ByteBuffer.wrap(message(op));
    }

    private byte[] message(Op op) {
        final PgProto.RowMessage.Builder builder = PgProto.RowMessage.newBuilder()
                .setOp(op)
                .setTransactionId(1)
                .setCommitTime(0);
        if (op == Op.INSERT || op == Op.UPDATE || op == Op.DELETE) {
            builder.setTable("public.test")
                    .addNewTypeinfo(PgProto.TypeInfo.newBuilder().setModifier("int4").setValueOptional(false))
                    .addNewTuple(PgProto.DatumMessage.newBuilder().setColumnName("id").setColumnType(23).setDatumInt32(1));
        }
        return builder.build().toByteArray();
    }

    /**
     * Builds a message that carries the operation only, without the tuples of a row message, so
     * that the messages of two operations have the same size and can replace each other in one
     * buffer.
     */
    private byte[] messageWithOperationOnly(Op op) {
        return PgProto.RowMessage.newBuilder()
                .setOp(op)
                .setTransactionId(1)
                .setCommitTime(0)
                .build()
                .toByteArray();
    }

    private ByteBuffer bufferWithoutOp() {
        return ByteBuffer.wrap(PgProto.RowMessage.newBuilder()
                .setTransactionId(1)
                .setCommitTime(0)
                .build()
                .toByteArray());
    }
}
