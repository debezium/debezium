/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.debezium.annotation.Immutable;
import io.debezium.annotation.VisibleForTesting;
import io.debezium.connector.postgresql.connection.ServerInfo;
import io.debezium.relational.TableId;

/**
 * Class that records Replica Identity information for the {@link PostgresConnector}
 * @author Ben White, Miguel Sotomayor
 */
@Immutable
public class ReplicaIdentityMapper {
    public static final Pattern REPLICA_AUTO_SET_PATTERN = Pattern
            .compile("(?i)^\\s*(?<tablename>[^\\s:]+):(?<replicaidentity>DEFAULT|(INDEX) (?<indexname>.\\w*)|FULL|NOTHING)\\s*$");
    public static final Pattern PATTERN_SPLIT = Pattern.compile(",");
    private final Map<TableId, ServerInfo.ReplicaIdentity> replicaIdentityMapper;
    private final String replicaAutoSetValue;

    public ReplicaIdentityMapper(String replicaAutoSetValue) {
        this.replicaAutoSetValue = replicaAutoSetValue;
        this.replicaIdentityMapper = getReplicaIdentityMap();
    }

    public Set<TableId> getTableIds() {
        return replicaIdentityMapper != null ? new HashSet<>(replicaIdentityMapper.keySet())
                : Collections.emptySet();
    }

    public Optional<ServerInfo.ReplicaIdentity> findReplicaIdentity(TableId tableId) {
        return Optional.ofNullable(this.replicaIdentityMapper)
                .map(mapper -> mapper.get(tableId));
    }

    @VisibleForTesting
    Map<TableId, ServerInfo.ReplicaIdentity> getReplicaIdentityMap() {

        if (replicaAutoSetValue == null) {
            return null;
        }

        return Arrays.stream(PATTERN_SPLIT.split(replicaAutoSetValue))
                .map(REPLICA_AUTO_SET_PATTERN::matcher)
                .filter(Matcher::matches)
                .collect(Collectors.toMap(
                        t -> {
                            String[] tableName = t.group("tablename").split("\\.");
                            return new TableId("", tableName[0], tableName[1]);
                        },
                        t -> {
                            ServerInfo.ReplicaIdentity replicaIdentity = ServerInfo.ReplicaIdentity.valueOf(
                                    t.group("replicaidentity").replaceAll("\\s.*", ""));
                            replicaIdentity.setIndexName(t.group("indexname"));
                            return replicaIdentity;
                        }));
    }
}
