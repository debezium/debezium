/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.cube;

import org.arquillian.cube.docker.impl.await.LogScanningAwaitStrategy;
import org.arquillian.cube.docker.impl.await.PollingAwaitStrategy;
import org.arquillian.cube.docker.impl.client.config.Await;
import org.arquillian.cube.docker.impl.docker.DockerClientExecutor;
import org.arquillian.cube.spi.Cube;
import org.arquillian.cube.spi.await.AwaitStrategy;

/**
 * This strategy is used in arquillian.xml to verify that MySQL Docker containers were initialized.
 * It is necessary to combine two strategies - log and polling for port. Using them in isolation fails
 * because the initialization logic of container is
 * <ul>
 * <ol>Start MySQL and initialize the database</ol>
 * <ol>Print message - MySQL init process done. Ready for start up.</ol>
 * <ol>Restart database</ol>
 * </ul>
 * It is thus necessary to poll for 3306 port only AFTER the message is present in a log file otherwise
 * Cube would mistakenly suppose the container is initialized as the polling strategy would hit
 * the MySQL start during the first phase.
 * 
 * @author Jiri Pechanec
 *
 */
public class MySQLAwaitStrategy implements AwaitStrategy {
    private static final int WAIT_AFTER_LOG_MESSAGE = 5000;
    private Await params;
    private DockerClientExecutor dockerClientExecutor;
    private Cube<?> cube;

    public void setCube(Cube<?> cube) {
        this.cube = cube;
    }

    public void setDockerClientExecutor(DockerClientExecutor dockerClientExecutor) {
        this.dockerClientExecutor = dockerClientExecutor;
    }

    public void setParams(Await params) {
        this.params = params;
    }

    @Override
    public boolean await() {
        final LogScanningAwaitStrategy log = new LogScanningAwaitStrategy(cube, dockerClientExecutor, params);
        final PollingAwaitStrategy polling = new PollingAwaitStrategy(cube, dockerClientExecutor, params);

        final boolean logAwait = log.await();
        if (logAwait) {
            try {
                Thread.sleep(WAIT_AFTER_LOG_MESSAGE);
            } catch (InterruptedException e) {
            }
        }
        else {
            return false;
        }

        return polling.await();
    }}
