/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.adb;

import io.debezium.connector.oracle.SnapshotDatatypesIT;
import io.debezium.connector.oracle.junit.SkipWhenNotAutonomous;

/**
 * Integration test that verifies the Oracle datatypes captured during initial snapshotting
 * against an Oracle Autonomous Database. Snapshots read data through JDBC, so no special
 * change-visibility handling is required; all tests are inherited as-is.
 *
 * @author Chris Cranford
 */
@SkipWhenNotAutonomous(reason = "Verifies datatype snapshotting against an Autonomous Database")
public class AutonomousSnapshotDatatypesIT extends SnapshotDatatypesIT {
}