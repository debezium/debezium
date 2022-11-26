/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.cluster;

import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.SyncDockerCmd;
import com.github.dockerjava.api.model.ContainerNetwork;
import com.mongodb.ServerAddress;

/**
 * A container for running a single MongoDB {@code mongod} or {@code mongos} process.
 * <p>
 * In order to interact with a running container from the host using a client driver, the container's network alias
 * ({@link #name}) must be resolvable from the host. On most systems this will require configuring {@code /etc/hosts}
 * to have an entry that maps {@link #name} to {@code 127.0.0.1}. To make this portable across systems, fixed ports are
 * used on the host and are mapped exactly to the container. Random free ports are assigned to minimize the chance of
 * conflicts.
 */
public class MongoDbContainer extends GenericContainer<MongoDbContainer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbContainer.class);

    /**
     * Default should match {@code version.mongo.server} in parent {@code pom.xml}.
     */
    public static final String IMAGE_VERSION = System.getProperty("version.mongo.server", "6.0");
    private static final DockerImageName IMAGE_NAME = DockerImageName.parse("mongo:" + IMAGE_VERSION);

    private final String name;
    private final int port;
    private final String replicaSet;

    public static Builder node() {
        return new Builder();
    }

    public static final class Builder {

        private String name;
        private int port = -1;
        private String replicaSet;
        private Network network = Network.SHARED;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
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

        public MongoDbContainer build() {
            return new MongoDbContainer(this);
        }

    }

    private MongoDbContainer(Builder builder) {
        super(IMAGE_NAME);
        this.name = builder.name;
        this.port = builder.port == -1 ? findFreePort() : builder.port;
        this.replicaSet = builder.replicaSet;

        withNetwork(builder.network);
        addFixedExposedPort(port, port);
        withCreateContainerCmdModifier(cmd -> cmd.withName(name));
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
    public ServerAddress getClientAddress() {
        checkStarted();

        // Technically we only need to do this for Mac
        if (isDockerDesktop()) {
            return getNamedAddress();
        }

        // On Linux we can address directly
        return new ServerAddress(getNetworkIp(), port);
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
    public ServerAddress getNamedAddress() {
        return new ServerAddress(name, port);
    }

    public void initReplicaSet(boolean configServer, ServerAddress... serverAddresses) {
        LOGGER.info("[{}:{}] Initializing replica set...", replicaSet, name);
        eval("rs.initiate({_id:'" + replicaSet + "',configsvr:" + configServer + ",members:[" +
                range(0, serverAddresses.length)
                        .mapToObj(i -> "{_id:" + i + ",host:'" + serverAddresses[i] + "'}")
                        .collect(joining(","))
                +
                "]});");
    }

    public void stepDown() {
        LOGGER.info("[{}:{}] Stepping down...", replicaSet, name);
        eval("rs.stepDown();");
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

    public void eval(String command) {
        checkStarted();

        try {
            var mongoCommand = "mongo " +
                    "--quiet " +
                    "--host " + name + " " +
                    "--port " + port + " " +
                    "--eval \"" + command + "\"";

            // Support newer and older MongoDB versions respectively
            var result = execInContainer("sh", "-c", isLegacy() ? mongoCommand : "mongosh " + mongoCommand);
            LOGGER.info(result.getStdout());
            checkExitcode(result);
        }
        catch (IOException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    private void checkExitcode(ExecResult result) {
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

    private static boolean isDockerDesktop() {
        var info = DockerClientFactory.instance().getInfo();
        return "docker-desktop".equals(info.getName());
    }

    private static boolean isLegacy() {
        return IMAGE_VERSION.equals("4.0") || IMAGE_VERSION.equals("4.4");
    }

    private static int findFreePort() {
        try (var serverSocket = new ServerSocket(0)) {
            serverSocket.setReuseAddress(true);
            return serverSocket.getLocalPort();
        }
        catch (IOException e) {
            return -1;
        }
    }

}
