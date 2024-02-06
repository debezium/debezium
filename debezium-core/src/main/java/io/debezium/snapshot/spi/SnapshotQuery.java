/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.snapshot.spi;

import java.util.List;
import java.util.Optional;

import io.debezium.service.Service;
import io.debezium.spi.common.Configurable;

/**
 * {@link SnapshotQuery} is used to determine the query used during a data snapshot
 *
 * @author Mario Fiore Vitale
 */
public interface SnapshotQuery extends Configurable, Service {

    /**
     * @return the name of the snapshot lock.
     *
     *
     */
    String name();

    /**
     * Generate a valid query string for the specified table, or an empty {@link Optional}
     * to skip snapshotting this table (but that table will still be streamed from)
     *
     * @param tableId the table to generate a query for
     * @param snapshotSelectColumns the columns to be used in the snapshot select based on the column
     *                              include/exclude filters
     * @return a valid query string, or none to skip snapshotting this table
     */
    Optional<String> snapshotQuery(String tableId, List<String> snapshotSelectColumns);

}
