/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

import java.util.Map;
import java.util.Objects;

/**
 * The role of a server as a connector observed it, with how the role was determined and the facts it was determined from.
 * A role that a database reports explicitly and a role that the connector inferred from other facts are told apart by
 * the source.
 *
 * @param role the observed role; never null
 * @param source how the role was determined, for example {@code GROUP_REPLICATION} or {@code INFERRED}; empty when the
 *            role is unknown
 * @param state the connector-specific facts that the role was determined from; never null
 */
public record ServerRoleObservation(ServerRole role, String source, Map<String, String> state) {

    public static final ServerRoleObservation UNKNOWN = new ServerRoleObservation(ServerRole.UNKNOWN, "", Map.of());

    public ServerRoleObservation {
        Objects.requireNonNull(role);
        Objects.requireNonNull(source);
        state = Map.copyOf(state);
    }
}
