/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spi.snapshot;

import java.util.List;
import java.util.Map;

/**
 * This interface is used to provide custom snapshotters:
 * Implementations must:
 *
 * provide a map of snapshotter in the {@link #create(Configuration config)} method.
 *
 * @author Mario Fiore Vitale
 */
public interface SnapshotterProvider {

    /**
     * Create a map of snapshotter where the key is its name used in 'snapshot.mode' configuration.
     *
     * @param Configuration the connector configuration
     *
     * @return a map of custom snapshotter
     */
    Map<String, Snapshotter> create(Configuration config); // Can we move the Configuration interface from core module?
}
