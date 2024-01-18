/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import static io.debezium.testing.testcontainers.util.DockerUtils.logDockerDesktopBanner;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static org.awaitility.Awaitility.await;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import io.debezium.testing.testcontainers.MongoDbContainer.Address;
import io.debezium.testing.testcontainers.util.MoreStartables;
import io.debezium.testing.testcontainers.util.PortResolver;
import io.debezium.testing.testcontainers.util.RandomPortResolver;

/**
 * A MongoDB replica set.
 */
public class MongoDbReplicaSet implements MongoDbDeployment {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbReplicaSet.class);

    private final String name;
    private final int memberCount;
    private final boolean configServer;
    private final Network network;
    private final PortResolver portResolver;

    private final List<MongoDbContainer> members = new ArrayList<>();
    private final DockerImageName imageName;
    private final boolean authEnabled;
    private final String rootUser;
    private final String rootPassword;
    private final Supplier<MongoDbContainer.Builder> nodeSupplier;

    private boolean started = false;

    public static Builder replicaSet() {
        return new Builder().nodeSupplier(MongoDbContainer::node);
    }

    public static Builder shardReplicaSet() {
        return new Builder().nodeSupplier(MongoDbContainer::shardServerNode);
    }

    public static Builder configServerReplicaSet() {
        return new Builder().nodeSupplier(MongoDbContainer::configServerNode);
    }

    public static class Builder {

        private String name = "rs0";
        private String namespace = "test-mongo";
        private int memberCount = 3;
        private boolean configServer = false;

        private Network network = Network.newNetwork();
        private PortResolver portResolver = new RandomPortResolver();
        private boolean skipDockerDesktopLogWarning = false;
        private DockerImageName imageName;
        private boolean authEnabled;
        private String rootUser = "root";
        private String rootPassword = "secret";
        private Supplier<MongoDbContainer.Builder> nodeSupplier = MongoDbContainer::node;

        public Builder nodeSupplier(Supplier<MongoDbContainer.Builder> nodeSupplier) {
            this.nodeSupplier = nodeSupplier;
            return this;
        }

        public Builder authEnabled(boolean authEnabled) {
            this.authEnabled = authEnabled;
            return this;
        }

        public Builder imageName(DockerImageName imageName) {
            this.imageName = imageName;
            return this;
        }

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

        public Builder rootUser(String username, String password) {
            this.rootUser = username;
            this.rootPassword = password;
            return this;
        }

        public MongoDbReplicaSet build() {
            return new MongoDbReplicaSet(this);
        }
    }

    private MongoDbReplicaSet(Builder builder) {
        this.nodeSupplier = builder.nodeSupplier;
        this.name = builder.name;
        this.memberCount = builder.memberCount;
        this.configServer = builder.configServer;
        this.network = builder.network;
        this.portResolver = builder.portResolver;
        this.imageName = builder.imageName;
        this.authEnabled = builder.authEnabled;
        this.rootUser = builder.rootUser;
        this.rootPassword = builder.rootPassword;

        for (int i = 1; i <= memberCount; i++) {
            members.add(nodeSupplier.get()
                    .network(network)
                    .name(builder.namespace + i)
                    .replicaSet(name)
                    .portResolver(portResolver)
                    .skipDockerDesktopLogWarning(true)
                    .imageName(imageName)
                    .authEnabled(authEnabled)
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
    @Override
    public String getConnectionString() {
        if (authEnabled) {
            return getAuthConnectionString(rootUser, rootPassword, "admin");
        }
        return getNoAuthConnectionString();
    }

    @Override
    public String getNoAuthConnectionString() {
        return getConnectionString(false, null, null, null);
    }

    @Override
    public String getAuthConnectionString(String username, String password, String authSource) {
        return getConnectionString(true, username, password, authSource);
    }

    private String getConnectionString(boolean useAuth, String username, String password, String authSource) {
        var builder = new StringBuilder("mongodb://");

        if (useAuth) {
            builder
                    .append(URLEncoder.encode(username, StandardCharsets.UTF_8))
                    .append(":")
                    .append(URLEncoder.encode(password, StandardCharsets.UTF_8))
                    .append("@");
        }

        var hosts = members.stream()
                .map(MongoDbContainer::getClientAddress)
                .map(Objects::toString)
                .collect(joining(","));

        builder.append(hosts)
                .append("/?replicaSet=").append(name);

        if (useAuth) {
            builder.append("&").append("authSource=").append(authSource);
        }
        return builder.toString();
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

        // Create rootUser
        LOGGER.info("[{}] Creating root user...", name);
        createRootUser();
        // Make sure RS status is available to root user
        awaitReplicaPrimary();

        started = true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        if (started) {
            LOGGER.info("[{}] Stopping...", name);
            MoreStartables.deepStopSync(members.stream());
            started = false;
        }
    }

    private void initializeReplicaSet() {
        var arbitraryNode = members.get(0);
        var serverAddresses = members.stream()
                .map(MongoDbContainer::getClientAddress)
                .toArray(Address[]::new);

        arbitraryNode.initReplicaSet(configServer, serverAddresses);
    }

    private void createRootUser() {
        // guard here to simplify start code
        if (authEnabled) {
            var primary = tryPrimary().orElseThrow();
            primary.createUser(rootUser, rootPassword, "admin", true, "root");
        }
    }

    /**
     * Creates new user in the RS via primary;
     * @param username name
     * @param password password
     * @param database auth database
     * @param rolePairs either role name or "role:database" pair
     */
    public void createUser(String username, String password, String database, String... rolePairs) {
        var primary = tryPrimary().orElseThrow();
        primary.createUser(username, password, database, false, rolePairs);
    }

    /**
     * Upload and executes mongodb javascript file against current primary
     *
     * See {@link  MongoDbContainer#execMongoScriptInContainer(MountableFile, String)}
     */
    public Container.ExecResult execMongoScript(MountableFile file, String containerPath) {
        return tryPrimary()
                .map(primary -> primary.execMongoScriptInContainer(file, containerPath))
                .orElseThrow();
    }

    public void awaitReplicaPrimary() {
        await()
                .atMost(1, MINUTES)
                .pollDelay(1, SECONDS)
                .ignoreException(IllegalStateException.class)
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

    public MongoDbReplicaSet withStartupTimeout(Duration startupTimeout) {
        members.forEach(member -> member.withStartupTimeout(startupTimeout));
        return this;
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
