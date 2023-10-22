/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.re2j.Matcher;
import com.google.re2j.Pattern;

import io.debezium.DebeziumException;
import io.debezium.annotation.Immutable;
import io.debezium.connector.postgresql.connection.ReplicaIdentityInfo;
import io.debezium.function.Predicates;
import io.debezium.relational.TableId;

/**
 * Class that records Replica Identity information for the {@link PostgresConnector}
 * @author Ben White, Miguel Sotomayor
 */
@Immutable
public class ReplicaIdentityMapper {
    public static final Pattern REPLICA_AUTO_SET_PATTERN = Pattern
            .compile("(?i)^\\s*(?P<tablepredicate>[^\\s:]+):(?P<replicaidentity>DEFAULT|(INDEX) (?P<indexname>.\\w*)|FULL|NOTHING)\\s*$");
    public static final Pattern PATTERN_SPLIT = Pattern.compile(",");
    private Map<Predicate<TableId>, ReplicaIdentityInfo> replicaIdentityPredicateMap;
    private final String replicaAutoSetValue;

    public ReplicaIdentityMapper(String replicaAutoSetValue) {
        this.replicaAutoSetValue = replicaAutoSetValue;
    }

    /**
     * This method get the Replica Identity of the {@link TableId}, checking if the {@link TableId} is contained in the {@link Map} of {@link Predicates} stored
     * in {@link ReplicaIdentityMapper#replicaIdentityPredicateMap}
     *
     * @param tableId the identifier of the table
     * @return {@link ReplicaIdentityInfo} of the {@link TableId}
     * @throws DebeziumException if there is a problem obtaining the replica identity for the given table
     */
    public Optional<ReplicaIdentityInfo> findReplicaIdentity(TableId tableId) throws DebeziumException {
        if (replicaIdentityPredicateMap == null) {
            this.replicaIdentityPredicateMap = this.getReplicaIdentityPredicateMap();
        }
        return this.replicaIdentityPredicateMap
                .entrySet()
                .stream()
                .filter(item -> item.getKey().test(tableId))
                .map(Map.Entry::getValue)
                .reduce((a, b) -> { // If the map has only one entry, then the reduction will only happen once, and the final result will be the single entry.
                    throw new DebeziumException(String.format("More than one Regular expressions matched table %s", tableId));
                });
    }

    /**
     * This method parses the property `replica.identity.autoset.values` stored in {@link ReplicaIdentityMapper#replicaAutoSetValue} attribute
     * generating a map collection with the Table and its respective Replica Identity
     *
     * @return {@link Map} collection that contains {@link TableId} as Key and {@link ReplicaIdentityInfo} as value
     */
    private Map<Predicate<TableId>, ReplicaIdentityInfo> getReplicaIdentityPredicateMap() {

        if (replicaAutoSetValue == null) {
            return Collections.emptyMap();
        }

        return Arrays.stream(PATTERN_SPLIT.split(replicaAutoSetValue))
                .map(REPLICA_AUTO_SET_PATTERN::matcher)
                .filter(Matcher::matches)
                .collect(Collectors.toMap(
                        t -> Predicates.includes(t.group("tablepredicate"), TableId::toString),
                        t -> new ReplicaIdentityInfo(ReplicaIdentityInfo.ReplicaIdentity.valueOf(
                                t.group("replicaidentity").replaceAll("\\s.*", "")), t.group("indexname"))));
    }
}
