/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import java.util.Objects;
import java.util.UUID;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.UUIDUtils;

/**
 * Context holder for OpenLineage operations, encapsulating the necessary components
 * for tracking data lineage in Debezium change data capture processes.
 *
 * <p>This class serves as a container for OpenLineage-related objects and provides
 * a unique run identifier for tracking lineage events. Each instance represents
 * a specific execution context with its own UUID for correlation across lineage events.</p>
 *
 * <p>The context includes:
 * <ul>
 *   <li>OpenLineage client for creating lineage events</li>
 *   <li>Configuration settings for OpenLineage integration</li>
 *   <li>Job identifier for the specific Debezium job</li>
 *   <li>Unique run UUID for correlating related events</li>
 * </ul>
 *
 * <p>Equality and hash code are based solely on the run UUID, making each
 * context instance unique per execution run.</p>
 *
 * @see OpenLineage
 * @see DebeziumOpenLineageConfiguration
 * @see OpenLineageJobIdentifier
 */
public class OpenLineageContext {

    /** Unique identifier for this execution run, used to correlate lineage events */
    private final UUID runUuid;

    /** OpenLineage client instance for creating and managing lineage events */
    private final OpenLineage openLineage;

    /** Configuration settings for OpenLineage integration with Debezium */
    private final DebeziumOpenLineageConfiguration configuration;

    /** Identifier for the specific job being tracked */
    private final OpenLineageJobIdentifier jobIdentifier;

    /**
     * Constructs a new OpenLineageContext with the specified components.
     *
     * <p>A new UUID is automatically generated for this run to ensure uniqueness
     * and enable correlation of lineage events within this execution context.</p>
     *
     * @param openLineage the OpenLineage client instance for creating lineage events
     * @param configuration the configuration settings for OpenLineage integration
     * @param jobIdentifier the identifier for the job being tracked
     * @throws IllegalArgumentException if any parameter is null
     */
    public OpenLineageContext(OpenLineage openLineage, DebeziumOpenLineageConfiguration configuration, OpenLineageJobIdentifier jobIdentifier) {
        this.openLineage = openLineage;
        this.configuration = configuration;
        this.jobIdentifier = jobIdentifier;
        runUuid = UUIDUtils.generateNewUUID();
    }

    /**
     * Returns the OpenLineage client instance.
     *
     * @return the OpenLineage client for creating lineage events
     */
    public OpenLineage getOpenLineage() {
        return openLineage;
    }

    /**
     * Returns the OpenLineage configuration settings.
     *
     * @return the configuration for OpenLineage integration with Debezium
     */
    public DebeziumOpenLineageConfiguration getConfiguration() {
        return configuration;
    }

    /**
     * Returns the unique run identifier for this context.
     *
     * <p>This UUID is generated once per context instance and is used to
     * correlate all lineage events within this execution run.</p>
     *
     * @return the unique run UUID
     */
    public UUID getRunUuid() {
        return runUuid;
    }

    /**
     * Returns the job identifier for this context.
     *
     * @return the identifier for the job being tracked
     */
    public OpenLineageJobIdentifier getJobIdentifier() {
        return jobIdentifier;
    }

    /**
     * Compares this context with another object for equality.
     *
     * <p>Two OpenLineageContext instances are considered equal if they have
     * the same run UUID. This ensures that each execution run has a unique
     * context instance.</p>
     *
     * @param o the object to compare with
     * @return {@code true} if the objects are equal, {@code false} otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OpenLineageContext that = (OpenLineageContext) o;
        return Objects.equals(runUuid, that.runUuid);
    }

    /**
     * Returns the hash code for this context.
     *
     * <p>The hash code is based solely on the run UUID, ensuring consistency
     * with the equals method.</p>
     *
     * @return the hash code value
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(runUuid);
    }

    /**
     * Returns a string representation of this context.
     *
     * <p>The string includes all field values for debugging and logging purposes.</p>
     *
     * @return a string representation of this OpenLineageContext
     */
    @Override
    public String toString() {
        return "OpenLineageContext{" +
                "runUuid=" + runUuid +
                ", openLineage=" + openLineage +
                ", configuration=" + configuration +
                ", jobIdentifier=" + jobIdentifier +
                '}';
    }
}
