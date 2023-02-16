/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import static io.debezium.testing.testcontainers.MongoDbContainer.node;
import static io.debezium.testing.testcontainers.util.DockerUtils.logDockerDesktopBanner;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.JsonNode;

import io.debezium.testing.testcontainers.MongoDbContainer.Address;
import io.debezium.testing.testcontainers.util.MoreStartables;
import io.debezium.testing.testcontainers.util.PortResolver;
import io.debezium.testing.testcontainers.util.RandomPortResolver;

/**
 * A MongoDB replica set.
 */
public class MongoDbReplicaSet implements Startable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbReplicaSet.class);

    private final String name;
    private final int memberCount;
    private final boolean configServer;
    private final Network network;
    private final PortResolver portResolver;

    private final List<MongoDbContainer> members = new ArrayList<>();

    private boolean started;

    public static Builder replicaSet() {
        return new Builder();
    }

    public static class Builder {

        private String name = "rs0";
        private String namespace = "test-mongo";
        private int memberCount = 3;
        private boolean configServer = false;

        private Network network = Network.newNetwork();
        private PortResolver portResolver = new RandomPortResolver();
        private boolean skipDockerDesktopLogWarning = false;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder namespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        public Builder memberCount(int memberCount) {
            this.memberCount = memberCount;
            return this;
        }

        public Builder configServer(boolean configServer) {
            this.configServer = configServer;
            return this;
        }

        public Builder network(Network network) {
            this.network = network;
            return this;
        }

        public Builder skipDockerDesktopLogWarning(boolean skipDockerDesktopLogWarning) {
            this.skipDockerDesktopLogWarning = skipDockerDesktopLogWarning;
            return this;
        }

        public Builder portResolver(PortResolver portResolver) {
            this.portResolver = portResolver;
            return this;
        }

        public MongoDbReplicaSet build() {
            return new MongoDbReplicaSet(this);
        }
    }

    private MongoDbReplicaSet(Builder builder) {
        this.name = builder.name;
        this.memberCount = builder.memberCount;
        this.configServer = builder.configServer;
        this.network = builder.network;
        this.portResolver = builder.portResolver;

        for (int i = 1; i <= memberCount; i++) {
            members.add(node()
                    .network(network)
                    .name(builder.namespace + i)
                    .replicaSet(name)
                    .portResolver(portResolver)
                    .skipDockerDesktopLogWarning(true)
                    .build());
        }

        logDockerDesktopBanner(LOGGER, getHostNames(), builder.skipDockerDesktopLogWarning);
    }

    public String getName() {
        return name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Startable> getDependencies() {
        return new HashSet<>(members);
    }

    /**
     * @return the <a href="https://www.mongodb.com/docs/manual/reference/connection-string/#standard-connection-string-format">standard connection string</a>
     * to the replica set, comprised of only the {@code mongod} hosts.
     */
    public String getConnectionString() {
        return "mongodb://" + members.stream()
                .map(MongoDbContainer::getClientAddress)
                .map(Objects::toString)
                .collect(joining(","));
    }

    /**
     * Returns the replica set member containers.
     *
     * @return the replica set members
     */
    public List<MongoDbContainer> getMembers() {
        return members;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        // `start` needs to be reentrant for `Startables.deepStart` or it will be sad
        if (started) {
            return;
        }

        // Start all containers in parallel
        LOGGER.info("[{}] Starting {} node replica set...", name, memberCount);
        MoreStartables.deepStartSync(getDependencies().stream());

        // Initialize the configured replica set to contain all the cluster's members
        LOGGER.info("[{}] Initializing replica set...", name);
        initializeReplicaSet();

        // Wait until replica primary is active
        LOGGER.info("[{}] Awaiting primary...", name);
        awaitReplicaPrimary();

        started = true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        LOGGER.info("[{}] Stopping...", name);
        MoreStartables.deepStopSync(members.stream());
        network.close();
    }

    private void initializeReplicaSet() {
        var arbitraryNode = members.get(0);
        var serverAddresses = members.stream()
                .map(MongoDbContainer::getClientAddress)
                .toArray(Address[]::new);

        arbitraryNode.initReplicaSet(configServer, serverAddresses);
    }

    public void awaitReplicaPrimary() {
        await()
                .atMost(1, MINUTES)
                .pollDelay(1, SECONDS)
                .until(() -> tryPrimary().isPresent());
    }

    public void stepDown() {
        tryPrimary().ifPresent(MongoDbContainer::stepDown);
    }

    public void killPrimary() {
        tryPrimary().ifPresent((node) -> {
            node.kill();
            members.remove(node);
        });
    }

    public Optional<MongoDbContainer> tryPrimary() {
        return stream(getStatus().path("members"))
                .filter(memberStatus -> "PRIMARY".equals(memberStatus.path("stateStr").textValue()))
                .findFirst()
                .flatMap(this::findMember);
    }

    private Optional<MongoDbContainer> findMember(JsonNode memberStatus) {
        var name = memberStatus.path("name").textValue();
        return members.stream()
                .filter(node -> node.getNamedAddress().toString().equals(name) || // Match by name or possibly IP
                        node.getClientAddress().toString().equals(name))
                .findFirst();
    }

    private JsonNode getStatus() {
        var arbitraryNode = members.get(0);
        return arbitraryNode.eval("rs.status()");
    }

    public List<String> getHostNames() {
        return members.stream()
                .map(MongoDbContainer::getNamedAddress)
                .map(Address::getHost)
                .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "MongoDbReplicaSet{" +
                "name='" + name + '\'' +
                ", memberCount=" + memberCount +
                ", configServer=" + configServer +
                ", network=" + network +
                ", members=" + members +
                ", started=" + started +
                '}';
    }

    private static <T> Stream<T> stream(Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false);
    }

}
