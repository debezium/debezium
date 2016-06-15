/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.kafka.connect.data.Struct;
import org.fest.assertions.GenericAssert;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

import io.confluent.connect.avro.AvroData;
import io.debezium.document.Document;

public class SourceInfoTest {

    private static int avroSchemaCacheSize = 1000;
    private static final AvroData avroData = new AvroData(avroSchemaCacheSize);
    private static final String FILENAME = "mysql-bin.00001";
    private static final String GTID_SET = "gtid-set"; // can technically be any string
    private static final String SERVER_NAME = "my-server"; // can technically be any string

    private SourceInfo source;

    @Before
    public void beforeEach() {
        source = new SourceInfo();
    }

    @Test
    public void shouldStartSourceInfoFromZeroBinlogCoordinates() {
        source.setBinlogStartPoint(FILENAME, 0);
        assertThat(source.binlogFilename()).isEqualTo(FILENAME);
        assertThat(source.nextBinlogPosition()).isEqualTo(0);
        assertThat(source.lastBinlogPosition()).isEqualTo(0);
        assertThat(source.nextEventRowNumber()).isEqualTo(0);
        assertThat(source.lastEventRowNumber()).isEqualTo(0);
        assertThat(source.isSnapshotInEffect()).isFalse();
    }

    @Test
    public void shouldStartSourceInfoFromNonZeroBinlogCoordinates() {
        source.setBinlogStartPoint(FILENAME, 100);
        assertThat(source.binlogFilename()).isEqualTo(FILENAME);
        assertThat(source.nextBinlogPosition()).isEqualTo(100);
        assertThat(source.lastBinlogPosition()).isEqualTo(100);
        assertThat(source.nextEventRowNumber()).isEqualTo(0);
        assertThat(source.lastEventRowNumber()).isEqualTo(0);
        assertThat(source.isSnapshotInEffect()).isFalse();
    }

    // -------------------------------------------------------------------------------------
    // Test reading the offset map and recovering the proper SourceInfo state
    // -------------------------------------------------------------------------------------

    @Test
    public void shouldRecoverSourceInfoFromOffsetWithZeroBinlogCoordinates() {
        sourceWith(offset(0, 0));
        assertThat(source.gtidSet()).isNull();
        assertThat(source.binlogFilename()).isEqualTo(FILENAME);
        assertThat(source.nextBinlogPosition()).isEqualTo(0);
        assertThat(source.lastBinlogPosition()).isEqualTo(0);
        assertThat(source.nextEventRowNumber()).isEqualTo(0);
        assertThat(source.lastEventRowNumber()).isEqualTo(0);
        assertThat(source.isSnapshotInEffect()).isFalse();
    }

    @Test
    public void shouldRecoverSourceInfoFromOffsetWithNonZeroBinlogCoordinates() {
        sourceWith(offset(100, 0));
        assertThat(source.gtidSet()).isNull();
        assertThat(source.binlogFilename()).isEqualTo(FILENAME);
        assertThat(source.nextBinlogPosition()).isEqualTo(100);
        assertThat(source.lastBinlogPosition()).isEqualTo(100);
        assertThat(source.nextEventRowNumber()).isEqualTo(0);
        assertThat(source.lastEventRowNumber()).isEqualTo(0);
        assertThat(source.isSnapshotInEffect()).isFalse();
    }

    @Test
    public void shouldRecoverSourceInfoFromOffsetWithZeroBinlogCoordinatesAndNonZeroRow() {
        sourceWith(offset(0, 5));
        assertThat(source.gtidSet()).isNull();
        assertThat(source.binlogFilename()).isEqualTo(FILENAME);
        assertThat(source.nextBinlogPosition()).isEqualTo(0);
        assertThat(source.lastBinlogPosition()).isEqualTo(0);
        assertThat(source.nextEventRowNumber()).isEqualTo(5);
        assertThat(source.lastEventRowNumber()).isEqualTo(5);
        assertThat(source.isSnapshotInEffect()).isFalse();
    }

    @Test
    public void shouldRecoverSourceInfoFromOffsetWithNonZeroBinlogCoordinatesAndNonZeroRow() {
        sourceWith(offset(100, 5));
        assertThat(source.gtidSet()).isNull();
        assertThat(source.binlogFilename()).isEqualTo(FILENAME);
        assertThat(source.nextBinlogPosition()).isEqualTo(100);
        assertThat(source.lastBinlogPosition()).isEqualTo(100);
        assertThat(source.nextEventRowNumber()).isEqualTo(5);
        assertThat(source.lastEventRowNumber()).isEqualTo(5);
        assertThat(source.isSnapshotInEffect()).isFalse();
    }

    @Test
    public void shouldRecoverSourceInfoFromOffsetWithZeroBinlogCoordinatesAndSnapshot() {
        sourceWith(offset(0, 0, true));
        assertThat(source.gtidSet()).isNull();
        assertThat(source.binlogFilename()).isEqualTo(FILENAME);
        assertThat(source.nextBinlogPosition()).isEqualTo(0);
        assertThat(source.lastBinlogPosition()).isEqualTo(0);
        assertThat(source.nextEventRowNumber()).isEqualTo(0);
        assertThat(source.lastEventRowNumber()).isEqualTo(0);
        assertThat(source.isSnapshotInEffect()).isTrue();
    }

    @Test
    public void shouldRecoverSourceInfoFromOffsetWithNonZeroBinlogCoordinatesAndSnapshot() {
        sourceWith(offset(100, 0, true));
        assertThat(source.gtidSet()).isNull();
        assertThat(source.binlogFilename()).isEqualTo(FILENAME);
        assertThat(source.nextBinlogPosition()).isEqualTo(100);
        assertThat(source.lastBinlogPosition()).isEqualTo(100);
        assertThat(source.nextEventRowNumber()).isEqualTo(0);
        assertThat(source.lastEventRowNumber()).isEqualTo(0);
        assertThat(source.isSnapshotInEffect()).isTrue();
    }

    @Test
    public void shouldRecoverSourceInfoFromOffsetWithZeroBinlogCoordinatesAndNonZeroRowAndSnapshot() {
        sourceWith(offset(0, 5, true));
        assertThat(source.gtidSet()).isNull();
        assertThat(source.binlogFilename()).isEqualTo(FILENAME);
        assertThat(source.nextBinlogPosition()).isEqualTo(0);
        assertThat(source.lastBinlogPosition()).isEqualTo(0);
        assertThat(source.nextEventRowNumber()).isEqualTo(5);
        assertThat(source.lastEventRowNumber()).isEqualTo(5);
        assertThat(source.isSnapshotInEffect()).isTrue();
    }

    @Test
    public void shouldRecoverSourceInfoFromOffsetWithNonZeroBinlogCoordinatesAndNonZeroRowAndSnapshot() {
        sourceWith(offset(100, 5, true));
        assertThat(source.gtidSet()).isNull();
        assertThat(source.binlogFilename()).isEqualTo(FILENAME);
        assertThat(source.nextBinlogPosition()).isEqualTo(100);
        assertThat(source.lastBinlogPosition()).isEqualTo(100);
        assertThat(source.nextEventRowNumber()).isEqualTo(5);
        assertThat(source.lastEventRowNumber()).isEqualTo(5);
        assertThat(source.isSnapshotInEffect()).isTrue();
    }

    @Test
    public void shouldStartSourceInfoFromBinlogCoordinatesWithGtidsAndZeroBinlogCoordinates() {
        sourceWith(offset(GTID_SET, 0, 0, false));
        assertThat(source.gtidSet()).isEqualTo(GTID_SET);
        assertThat(source.binlogFilename()).isEqualTo(FILENAME);
        assertThat(source.nextBinlogPosition()).isEqualTo(0);
        assertThat(source.lastBinlogPosition()).isEqualTo(0);
        assertThat(source.nextEventRowNumber()).isEqualTo(0);
        assertThat(source.lastEventRowNumber()).isEqualTo(0);
        assertThat(source.isSnapshotInEffect()).isFalse();
    }

    @Test
    public void shouldStartSourceInfoFromBinlogCoordinatesWithGtidsAndZeroBinlogCoordinatesAndNonZeroRow() {
        sourceWith(offset(GTID_SET, 0, 5, false));
        assertThat(source.gtidSet()).isEqualTo(GTID_SET);
        assertThat(source.binlogFilename()).isEqualTo(FILENAME);
        assertThat(source.nextBinlogPosition()).isEqualTo(0);
        assertThat(source.lastBinlogPosition()).isEqualTo(0);
        assertThat(source.nextEventRowNumber()).isEqualTo(5);
        assertThat(source.lastEventRowNumber()).isEqualTo(5);
        assertThat(source.isSnapshotInEffect()).isFalse();
    }

    @Test
    public void shouldStartSourceInfoFromBinlogCoordinatesWithGtidsAndNonZeroBinlogCoordinates() {
        sourceWith(offset(GTID_SET, 100, 0, false));
        assertThat(source.gtidSet()).isEqualTo(GTID_SET);
        assertThat(source.binlogFilename()).isEqualTo(FILENAME);
        assertThat(source.nextBinlogPosition()).isEqualTo(100);
        assertThat(source.lastBinlogPosition()).isEqualTo(100);
        assertThat(source.nextEventRowNumber()).isEqualTo(0);
        assertThat(source.lastEventRowNumber()).isEqualTo(0);
        assertThat(source.isSnapshotInEffect()).isFalse();
    }

    @Test
    public void shouldStartSourceInfoFromBinlogCoordinatesWithGtidsAndNonZeroBinlogCoordinatesAndNonZeroRow() {
        sourceWith(offset(GTID_SET, 100, 5, false));
        assertThat(source.gtidSet()).isEqualTo(GTID_SET);
        assertThat(source.binlogFilename()).isEqualTo(FILENAME);
        assertThat(source.nextBinlogPosition()).isEqualTo(100);
        assertThat(source.lastBinlogPosition()).isEqualTo(100);
        assertThat(source.nextEventRowNumber()).isEqualTo(5);
        assertThat(source.lastEventRowNumber()).isEqualTo(5);
        assertThat(source.isSnapshotInEffect()).isFalse();
    }

    @Test
    public void shouldStartSourceInfoFromBinlogCoordinatesWithGtidsAndZeroBinlogCoordinatesAndSnapshot() {
        sourceWith(offset(GTID_SET, 0, 0, true));
        assertThat(source.gtidSet()).isEqualTo(GTID_SET);
        assertThat(source.binlogFilename()).isEqualTo(FILENAME);
        assertThat(source.nextBinlogPosition()).isEqualTo(0);
        assertThat(source.lastBinlogPosition()).isEqualTo(0);
        assertThat(source.nextEventRowNumber()).isEqualTo(0);
        assertThat(source.lastEventRowNumber()).isEqualTo(0);
        assertThat(source.isSnapshotInEffect()).isTrue();
    }

    @Test
    public void shouldStartSourceInfoFromBinlogCoordinatesWithGtidsAndZeroBinlogCoordinatesAndNonZeroRowAndSnapshot() {
        sourceWith(offset(GTID_SET, 0, 5, true));
        assertThat(source.gtidSet()).isEqualTo(GTID_SET);
        assertThat(source.binlogFilename()).isEqualTo(FILENAME);
        assertThat(source.nextBinlogPosition()).isEqualTo(0);
        assertThat(source.lastBinlogPosition()).isEqualTo(0);
        assertThat(source.nextEventRowNumber()).isEqualTo(5);
        assertThat(source.lastEventRowNumber()).isEqualTo(5);
        assertThat(source.isSnapshotInEffect()).isTrue();
    }

    @Test
    public void shouldStartSourceInfoFromBinlogCoordinatesWithGtidsAndNonZeroBinlogCoordinatesAndSnapshot() {
        sourceWith(offset(GTID_SET, 100, 0, true));
        assertThat(source.gtidSet()).isEqualTo(GTID_SET);
        assertThat(source.binlogFilename()).isEqualTo(FILENAME);
        assertThat(source.nextBinlogPosition()).isEqualTo(100);
        assertThat(source.lastBinlogPosition()).isEqualTo(100);
        assertThat(source.nextEventRowNumber()).isEqualTo(0);
        assertThat(source.lastEventRowNumber()).isEqualTo(0);
        assertThat(source.isSnapshotInEffect()).isTrue();
    }

    @Test
    public void shouldStartSourceInfoFromBinlogCoordinatesWithGtidsAndNonZeroBinlogCoordinatesAndNonZeroRowAndSnapshot() {
        sourceWith(offset(GTID_SET, 100, 5, true));
        assertThat(source.gtidSet()).isEqualTo(GTID_SET);
        assertThat(source.binlogFilename()).isEqualTo(FILENAME);
        assertThat(source.nextBinlogPosition()).isEqualTo(100);
        assertThat(source.lastBinlogPosition()).isEqualTo(100);
        assertThat(source.nextEventRowNumber()).isEqualTo(5);
        assertThat(source.lastEventRowNumber()).isEqualTo(5);
        assertThat(source.isSnapshotInEffect()).isTrue();
    }

    // -------------------------------------------------------------------------------------
    // Test advancing SourceInfo state (similar to how the BinlogReader uses it)
    // -------------------------------------------------------------------------------------

    @Test
    public void shouldAdvanceSourceInfoFromNonZeroPositionAndRowZeroForEventsWithOneRow() {
        sourceWith(offset(100, 0));
        handleNextEvent(200, 10, withRowCount(1));
        handleNextEvent(220, 10, withRowCount(1));
        handleNextEvent(250, 50, withRowCount(1));
    }

    @Test
    public void shouldAdvanceSourceInfoFromNonZeroPositionAndRowZeroForEventsWithMultipleRow() {
        sourceWith(offset(100, 0));
        handleNextEvent(200, 10, withRowCount(3));
        handleNextEvent(220, 10, withRowCount(4));
        handleNextEvent(250, 50, withRowCount(6));
        handleNextEvent(300, 20, withRowCount(1));
        handleNextEvent(350, 20, withRowCount(3));
    }

    // -------------------------------------------------------------------------------------
    // Utility methods
    // -------------------------------------------------------------------------------------

    protected int withRowCount(int rowCount) {
        return rowCount;
    }

    protected void handleNextEvent(long positionOfEvent, long eventSize, int rowCount) {
        source.setEventPosition(positionOfEvent, eventSize);
        for (int i = 0; i != rowCount; ++i) {
            // Get the offset for this row (always first!) ...
            Map<String, ?> offset = source.offsetForRow(i, rowCount);
            if ((i + 1) < rowCount) {
                // This is not the last row, so the next binlog position should be for next row in this event ...
                assertThat(offset.get(SourceInfo.BINLOG_POSITION_OFFSET_KEY)).isEqualTo(positionOfEvent);
                assertThat(offset.get(SourceInfo.BINLOG_EVENT_ROW_NUMBER_OFFSET_KEY)).isEqualTo(i+1);
            } else {
                // This is the last row, so the next binlog position should be for first row in next event ...
                assertThat(offset.get(SourceInfo.BINLOG_POSITION_OFFSET_KEY)).isEqualTo(positionOfEvent + eventSize);
                assertThat(offset.get(SourceInfo.BINLOG_EVENT_ROW_NUMBER_OFFSET_KEY)).isEqualTo(0);
            }
            assertThat(offset.get(SourceInfo.BINLOG_FILENAME_OFFSET_KEY)).isEqualTo(FILENAME);
            if ( source.gtidSet() != null ) {
                assertThat(offset.get(SourceInfo.GTID_SET_KEY)).isEqualTo(source.gtidSet());
            }
            // Get the source struct for this row (always second), which should always reflect this row in this event ...
            Struct recordSource = source.struct();
            assertThat(recordSource.getInt64(SourceInfo.BINLOG_POSITION_OFFSET_KEY)).isEqualTo(positionOfEvent);
            assertThat(recordSource.getInt32(SourceInfo.BINLOG_EVENT_ROW_NUMBER_OFFSET_KEY)).isEqualTo(i);
            assertThat(recordSource.getString(SourceInfo.BINLOG_FILENAME_OFFSET_KEY)).isEqualTo(FILENAME);
            if ( source.gtidSet() != null ) {
                assertThat(recordSource.getString(SourceInfo.GTID_SET_KEY)).isEqualTo(source.gtidSet());
            }
        }
    }

    protected Map<String, String> offset(long position, int row) {
        return offset(null, position, row, false);
    }

    protected Map<String, String> offset(long position, int row, boolean snapshot) {
        return offset(null, position, row, snapshot);
    }

    protected Map<String, String> offset(String gtidSet, long position, int row, boolean snapshot) {
        Map<String, String> offset = new HashMap<>();
        offset.put(SourceInfo.BINLOG_FILENAME_OFFSET_KEY, FILENAME);
        offset.put(SourceInfo.BINLOG_POSITION_OFFSET_KEY, Long.toString(position));
        offset.put(SourceInfo.BINLOG_EVENT_ROW_NUMBER_OFFSET_KEY, Integer.toString(row));
        if (gtidSet != null) offset.put(SourceInfo.GTID_SET_KEY, gtidSet);
        if (snapshot) offset.put(SourceInfo.SNAPSHOT_KEY, Boolean.TRUE.toString());
        return offset;
    }

    protected SourceInfo sourceWith(Map<String, String> offset) {
        source = new SourceInfo();
        source.setOffset(offset);
        source.setServerName(SERVER_NAME);
        return source;
    }

    /**
     * When we want to consume SinkRecord which generated by debezium-connector-mysql, it should not
     * throw error "org.apache.avro.SchemaParseException: Illegal character in: server-id"
     */
    @Test
    public void shouldValidateSourceInfoSchema() {
        org.apache.kafka.connect.data.Schema kafkaSchema = SourceInfo.SCHEMA;
        Schema avroSchema = avroData.fromConnectSchema(kafkaSchema);
        assertTrue(avroSchema != null);
    }

    @Test
    public void shouldConsiderPositionsWithSameGtidSetsAsSame() {
        assertPositionWithGtids("IdA:1-5").isAtOrBefore(positionWithGtids("IdA:1-5")); // same, single
        assertPositionWithGtids("IdA:1-5,IdB:1-20").isAtOrBefore(positionWithGtids("IdA:1-5,IdB:1-20")); // same, multiple
        assertPositionWithGtids("IdA:1-5,IdB:1-20").isAtOrBefore(positionWithGtids("IdB:1-20,IdA:1-5")); // equivalent
    }

    @Test
    public void shouldConsiderPositionsWithSameGtidSetsAndSnapshotAsSame() {
        assertPositionWithGtids("IdA:1-5", true).isAtOrBefore(positionWithGtids("IdA:1-5", true)); // same, single
        assertPositionWithGtids("IdA:1-5,IdB:1-20", true).isAtOrBefore(positionWithGtids("IdA:1-5,IdB:1-20", true)); // same,
                                                                                                                     // multiple
        assertPositionWithGtids("IdA:1-5,IdB:1-20", true).isAtOrBefore(positionWithGtids("IdB:1-20,IdA:1-5", true)); // equivalent
    }

    @Test
    public void shouldOrderPositionWithGtidAndSnapshotBeforePositionWithSameGtidButNoSnapshot() {
        assertPositionWithGtids("IdA:1-5", true).isAtOrBefore(positionWithGtids("IdA:1-5")); // same, single
        assertPositionWithGtids("IdA:1-5,IdB:1-20", true).isAtOrBefore(positionWithGtids("IdA:1-5,IdB:1-20")); // same, multiple
        assertPositionWithGtids("IdA:1-5,IdB:1-20", true).isAtOrBefore(positionWithGtids("IdB:1-20,IdA:1-5")); // equivalent
    }

    @Test
    public void shouldOrderPositionWithoutGtidAndSnapshotAfterPositionWithSameGtidAndSnapshot() {
        assertPositionWithGtids("IdA:1-5", false).isAfter(positionWithGtids("IdA:1-5", true)); // same, single
        assertPositionWithGtids("IdA:1-5,IdB:1-20", false).isAfter(positionWithGtids("IdA:1-5,IdB:1-20", true)); // same, multiple
        assertPositionWithGtids("IdA:1-5,IdB:1-20", false).isAfter(positionWithGtids("IdB:1-20,IdA:1-5", true)); // equivalent
    }

    @Test
    public void shouldOrderPositionWithGtidsAsBeforePositionWithExtraServerUuidInGtids() {
        assertPositionWithGtids("IdA:1-5").isBefore(positionWithGtids("IdA:1-5,IdB:1-20"));
    }

    @Test
    public void shouldOrderPositionsWithSameServerButLowerUpperLimitAsBeforePositionWithSameServerUuidInGtids() {
        assertPositionWithGtids("IdA:1-5").isBefore(positionWithGtids("IdA:1-6"));
        assertPositionWithGtids("IdA:1-5:7-9").isBefore(positionWithGtids("IdA:1-10"));
        assertPositionWithGtids("IdA:2-5:8-9").isBefore(positionWithGtids("IdA:1-10"));
    }

    @Test
    public void shouldOrderPositionWithoutGtidAsBeforePositionWithGtid() {
        assertPositionWithoutGtids("filename.01", Integer.MAX_VALUE, 0).isBefore(positionWithGtids("IdA:1-5"));
    }

    @Test
    public void shouldOrderPositionWithGtidAsAfterPositionWithoutGtid() {
        assertPositionWithGtids("IdA:1-5").isAfter(positionWithoutGtids("filename.01", 0, 0));
    }

    protected Document positionWithGtids(String gtids) {
        return positionWithGtids(gtids, false);
    }

    protected Document positionWithGtids(String gtids, boolean snapshot) {
        if (snapshot) {
            return Document.create(SourceInfo.GTID_SET_KEY, gtids, SourceInfo.SNAPSHOT_KEY, true);
        }
        return Document.create(SourceInfo.GTID_SET_KEY, gtids);
    }

    protected Document positionWithoutGtids(String filename, int position, int row) {
        return positionWithoutGtids(filename, position, row, false);
    }

    protected Document positionWithoutGtids(String filename, int position, int row, boolean snapshot) {
        if (snapshot) {
            return Document.create(SourceInfo.BINLOG_FILENAME_OFFSET_KEY, filename,
                                   SourceInfo.BINLOG_POSITION_OFFSET_KEY, position,
                                   SourceInfo.BINLOG_EVENT_ROW_NUMBER_OFFSET_KEY, row,
                                   SourceInfo.SNAPSHOT_KEY, true);
        }
        return Document.create(SourceInfo.BINLOG_FILENAME_OFFSET_KEY, filename,
                               SourceInfo.BINLOG_POSITION_OFFSET_KEY, position,
                               SourceInfo.BINLOG_EVENT_ROW_NUMBER_OFFSET_KEY, row);
    }

    protected PositionAssert assertThatDocument(Document position) {
        return new PositionAssert(position);
    }

    protected PositionAssert assertPositionWithGtids(String gtids) {
        return assertThatDocument(positionWithGtids(gtids));
    }

    protected PositionAssert assertPositionWithGtids(String gtids, boolean snapshot) {
        return assertThatDocument(positionWithGtids(gtids, snapshot));
    }

    protected PositionAssert assertPositionWithoutGtids(String filename, int position, int row) {
        return assertPositionWithoutGtids(filename, position, row, false);
    }

    protected PositionAssert assertPositionWithoutGtids(String filename, int position, int row, boolean snapshot) {
        return assertThatDocument(positionWithoutGtids(filename, position, row, snapshot));
    }

    protected static class PositionAssert extends GenericAssert<PositionAssert, Document> {
        public PositionAssert(Document position) {
            super(PositionAssert.class, position);
        }

        public PositionAssert isAt(Document otherPosition) {
            if (SourceInfo.isPositionAtOrBefore(actual, otherPosition)) return this;
            failIfCustomMessageIsSet();
            throw failure(actual + " should be consider same position as " + otherPosition);
        }

        public PositionAssert isBefore(Document otherPosition) {
            return isAtOrBefore(otherPosition);
        }

        public PositionAssert isAtOrBefore(Document otherPosition) {
            if (SourceInfo.isPositionAtOrBefore(actual, otherPosition)) return this;
            failIfCustomMessageIsSet();
            throw failure(actual + " should be consider same position as or before " + otherPosition);
        }

        public PositionAssert isAfter(Document otherPosition) {
            if (!SourceInfo.isPositionAtOrBefore(actual, otherPosition)) return this;
            failIfCustomMessageIsSet();
            throw failure(actual + " should be consider after " + otherPosition);
        }
    }
}
