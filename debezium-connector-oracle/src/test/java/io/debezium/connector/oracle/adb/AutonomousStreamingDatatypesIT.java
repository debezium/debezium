/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.adb;

import io.debezium.connector.oracle.StreamingDatatypesIT;
import io.debezium.connector.oracle.junit.SkipWhenNotAutonomous;
import io.debezium.connector.oracle.util.TestHelper;

/**
 * Integration test that verifies the Oracle datatypes captured during streaming against an
 * Oracle Autonomous Database.
 *
 * On an Autonomous Database changes only become visible to the streaming engine after the
 * current online redo log has been archived, so the record consumption entry point is
 * overridden to force an archive before delegating to the inherited behavior; the tests
 * themselves are inherited as-is.
 *
 * @author Chris Cranford
 */
@SkipWhenNotAutonomous(reason = "Verifies datatype streaming against an Autonomous Database")
public class AutonomousStreamingDatatypesIT extends StreamingDatatypesIT {

    @Override
    protected SourceRecords consumeRecordsByTopic(int numRecords) throws InterruptedException {
        TestHelper.forceStreamingVisibility();
        return super.consumeRecordsByTopic(numRecords);
    }
}