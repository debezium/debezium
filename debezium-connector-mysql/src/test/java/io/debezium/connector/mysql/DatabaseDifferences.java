/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

/**
 * A centralized expression of differences in behaviour between MySQL 5.x and 8.x
 *
 * @author Jiri Pechanec
 */
public interface DatabaseDifferences {

    boolean isCurrentDateTimeDefaultGenerated();

    String currentDateTimeDefaultOptional(String isoString);
}
