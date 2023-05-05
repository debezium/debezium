/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import static io.debezium.testing.testcontainers.util.DockerUtils.addFakeDnsEntry;
import static io.debezium.testing.testcontainers.util.DockerUtils.isDockerDesktop;
import static io.debezium.testing.testcontainers.util.DockerUtils.logDockerDesktopBanner;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.command.SyncDockerCmd;
import com.github.dockerjava.api.model.ContainerNetwork;

import io.debezium.testing.testcontainers.util.DockerUtils;
import io.debezium.testing.testcontainers.util.PortResolver;
import io.debezium.testing.testcontainers.util.RandomPortResolver;

/**
 * A container for running a single MongoDB {@code mongod} or {@code mongos} process.
 * <p>
 * In order to interact with a running container from the host running Docker Desktop using a client driver, the
 * container's network alias ({@link #name}) must be resolvable from the host. On most systems this will require
 * configuring {@code /etc/hosts} to have an entry that maps {@link #name} to {@code 127.0.0.1}. Fixed ports are used on
 * the host and are mapped 1:1 exactly with the container. Random free ports are assigned to minimize the chance of
 * conflicts.
 */
public class MongoDbContainer extends GenericContainer<MongoDbContainer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbContainer.class);

    /**
     * Default should match {@code version.mongo.server} in parent {@code pom.xml}.
     */
    public static final String IMAGE_VERSION = System.getProperty("version.mongo.server", "6.0");
    private static final DockerImageName IMAGE_NAME = DockerImageName.parse("mongo:" + IMAGE_VERSION);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final String name;
    private final int port;
    private final String replicaSet;
    private final PortResolver portResolver;

    public static Builder node() {
        return new Builder();
    }

    public static final class Builder {

        private DockerImageName imageName = IMAGE_NAME;
        private String name;
        private int port = 27017;
        private PortResolver portResolver = new RandomPortResolver();
        private String replicaSet;
        private Network network = Network.SHARED;
        private boolean skipDockerDesktopLogWarning = false;

        public Builder imageName(DockerImageName imageName) {
            if (imageName != null) {
                this.imageName = imageName;
            }
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder portResolver(PortResolver portResolver) {
            this.portResolver = portResolver;
            return this;
        }

        public Builder replicaSet(String replicaSet) {
            this.replicaSet = replicaSet;
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

        public MongoDbContainer build() {
            return new MongoDbContainer(this);
        }

    }

    private MongoDbContainer(Builder builder) {
        super(builder.imageName);
        this.name = builder.name;
        this.replicaSet = builder.replicaSet;
        this.portResolver = builder.portResolver;

        if (isDockerDesktop()) {
            this.port = portResolver.resolveFreePort();
            addFixedExposedPort(port, port);
        }
        else {
            this.port = builder.port;
        }

        logDockerDesktopBanner(LOGGER, List.of(name), builder.skipDockerDesktopLogWarning);

        withNetwork(builder.network);
        withNetworkAliases(name);
        withCommand(
                "--replSet", replicaSet,
                "--port", String.valueOf(port),
                "--bind_ip", "localhost," + name);
        waitingFor(Wait.forLogMessage("(?i).*waiting for connections.*", 1));
    }

    /**
     * Returns the public address that should be used by clients.
     * <p>
     * Must be called after {@link #start()} since the public address may not be available on all platforms before then.
     *
     * @return the host-addressable address
     */
    public Address getClientAddress() {
        checkStarted();

        // Technically we only need to do this for Mac
        if (isDockerDesktop()) {
            return getNamedAddress();
        }

        // On Linux we can address directly
        return new Address(getNetworkIp(), port);
    }

    /**
     * Returns the named-address that is only guaranteed available within the network.
     * <p>
     * Can always be called before {@link #start()} safely. Useful for intra-cluster addressing that must
     * be configured before containers are running and the addresses are not available (e.g. in the constructor of
     * a replica set or sharded cluster).
     *
     * @return the name-addressable network address
     */
    public Address getNamedAddress() {
        return new Address(name, port);
    }

    /**
     * Invokes <a href="https://www.mongodb.com/docs/manual/reference/method/rs.initiate/">rs.initiate</a> on the
     * container.
     *
     * @param configServer    whether this replica set is a used for a <a href="https://www.mongodb.com/docs/manual/reference/replica-configuration/#mongodb-rsconf-rsconf.configsvr">sharded cluster's config server</a>.
     * @param Addresses the list of <a href="https://www.mongodb.com/docs/manual/reference/replica-configuration/#mongodb-rsconf-rsconf.members-n-.host">hostname / port numbers</a> of the set members
     */
    public void initReplicaSet(boolean configServer, Address... Addresses) {
        LOGGER.info("[{}:{}] Initializing replica set...", replicaSet, name);
        eval("rs.initiate({_id:'" + replicaSet + "',configsvr:" + configServer + ",members:[" +
                range(0, Addresses.length)
                        .mapToObj(i -> "{_id:" + i + ",host:'" + Addresses[i] + "'}")
                        .collect(joining(","))
                +
                "]})");
    }

    /**
     * Uploads given file to container and executes is as mongodb javascript
     *
     * @param file file to be uploaded
     * @param containerPath path in the container
     * @return execution result
     */
    public Container.ExecResult execMongoScriptInContainer(MountableFile file, String containerPath) {
        try {
            copyFileToContainer(file, containerPath);
            return execMongoInContainer(containerPath);
        }
        catch (IOException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Invokes <a href="https://www.mongodb.com/docs/manual/reference/method/rs.stepDown/">rs.stepDown</a> on the
     * container to instruct the primary of the replica set to become the primary.
     */
    public void stepDown() {
        LOGGER.info("[{}:{}] Stepping down...", replicaSet, name);
        eval("rs.stepDown()");
    }

    public void kill() {
        LOGGER.info("[{}:{}] Killing...", replicaSet, name);
        dockerCommand((client) -> client.killContainerCmd(getContainerId()));
    }

    public void pause() {
        LOGGER.info("[{}:{}] Pausing...", replicaSet, name);
        dockerCommand((client) -> client.pauseContainerCmd(getContainerId()));
    }

    public void unpause() {
        LOGGER.info("[{}:{}] Unpausing...", replicaSet, name);
        dockerCommand((client) -> client.unpauseContainerCmd(getContainerId()));
    }

    private String getNetworkIp() {
        var info = getContainerInfo();
        return info
                .getNetworkSettings()
                .getNetworks()
                .values()
                .stream()
                .findFirst() // Only one, and it's the one we set in the constructor
                .map(ContainerNetwork::getIpAddress)
                .orElseThrow();
    }

    private void dockerCommand(Function<DockerClient, SyncDockerCmd<?>> action) {
        action.apply(DockerClientFactory.instance().client()).exec();
    }

    public Container.ExecResult execMongoInContainer(String... command) throws IOException, InterruptedException {
        checkStarted();
        // Support newer and older MongoDB versions respectively
        var mongoCommand = Stream.concat(
                Stream.of(
                        isLegacy() ? "" : "mongosh",
                        "mongo",
                        "--quiet",
                        "--host " + name,
                        "--port " + port),
                Arrays.stream(command)).collect(joining(" "));

        LOGGER.debug("Running command inside container: {}", mongoCommand);
        var result = execInContainer("sh", "-c", mongoCommand);
        LOGGER.debug(result.getStdout());

        checkExitCode(result);
        return result;
    }

    public JsonNode eval(String command) {
        try {
            var result = execMongoInContainer("--eval", "\"JSON.stringify(" + command + ")\"");

            String stdout = result.getStdout();
            var response = parseResponse(stdout);
            LOGGER.info("{}:", response);
            return response;
        }
        catch (IOException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    private static JsonNode parseResponse(String stdout) {
        try {
            return OBJECT_MAPPER.readTree(stdout);
        }
        catch (IOException e) {
            LOGGER.warn("Could not parse the following text as JSON: {}", stdout, e);
            return OBJECT_MAPPER.createObjectNode();
        }
    }

    private void checkExitCode(ExecResult result) {
        // See https://docs.publishing.service.gov.uk/manual/mongo-db-commands.html#step-down-the-primary for exit
        // code 252 and `rs.stepDown` on Mongo 4.0
        boolean ok = result.getExitCode() == 0 || isLegacy() && result.getExitCode() == 252;
        if (ok) {
            return;
        }

        var message = "An error occurred: " + result.getStderr();
        LOGGER.error(message);
        throw new IllegalStateException(message);
    }

    private void checkStarted() {
        if (getContainerId() == null) {
            throw new IllegalStateException("Cannot execute operation before calling `start`.");
        }
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        super.containerIsStarted(containerInfo);
        addFakeDnsEntry(name);
    }

    @Override
    protected void containerIsStopped(InspectContainerResponse containerInfo) {
        super.containerIsStopped(containerInfo);
        DockerUtils.removeFakeDnsEntry(name);
        portResolver.releasePort(port);
    }

    private static boolean isLegacy() {
        var major = Integer.parseInt(IMAGE_VERSION.substring(0, 1));
        return major <= 4;
    }

    public static class Address {

        /**
         * The host.
         */
        private final String host;

        /**
         * The port.
         */
        private final int port;

        public Address(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        @Override
        public String toString() {
            return host + ":" + port;
        }

    }

}
