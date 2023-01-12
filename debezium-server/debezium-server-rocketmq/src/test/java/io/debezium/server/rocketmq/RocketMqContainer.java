/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.rocketmq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.command.InspectContainerResponse;

/**
 * rocketmq container
 */
public class RocketMqContainer extends GenericContainer<RocketMqContainer> {

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("apache/rocketmq:4.9.4");
    private static final int defaultBrokerPermission = 6;
    public static final int NAMESRV_PORT = 9876;
    public static final int BROKER_PORT = 10911;

    public RocketMqContainer() {
        super(DEFAULT_IMAGE_NAME);
        withExposedPorts(NAMESRV_PORT, BROKER_PORT, BROKER_PORT - 2);
    }

    @Override
    protected void configure() {
        String command = "#!/bin/bash\n";
        command += "./mqnamesrv &\n";
        command += "./mqbroker -n localhost:" + NAMESRV_PORT;
        withCommand("sh", "-c", command);
    }

    @Override

    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        List<String> updateBrokerConfigCommands = new ArrayList<>();
        updateBrokerConfigCommands.add(updateBrokerConfig("brokerIP1", getHost()));
        updateBrokerConfigCommands.add(updateBrokerConfig("listenPort", getMappedPort(BROKER_PORT)));
        updateBrokerConfigCommands.add(updateBrokerConfig("brokerPermission", defaultBrokerPermission));
        final String command = String.join(" && ", updateBrokerConfigCommands);
        ExecResult result = null;
        try {
            result = execInContainer(
                    "/bin/sh",
                    "-c",
                    command);
        }
        catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (result != null && result.getExitCode() != 0) {
            throw new IllegalStateException(result.toString());
        }
    }

    private String updateBrokerConfig(final String key, final Object val) {
        final String brokerAddr = "localhost:" + BROKER_PORT;
        return "./mqadmin updateBrokerConfig -b " + brokerAddr + " -k " + key + " -v " + val;
    }

    public String getNamesrvAddr() {
        return String.format("%s:%s", getHost(), getMappedPort(NAMESRV_PORT));
    }

}
