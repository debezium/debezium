/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.client.dsl.ExecListener;

/**
 *
 * @author Jakub Cechacek
 */
public class DatabaseInitListener implements ExecListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseInitListener.class);

    private final String name;
    private final CountDownLatch latch;

    public DatabaseInitListener(String name, CountDownLatch latch) {
        this.latch = latch;
        this.name = name;
    }

    @Override
    public void onOpen() {
        LOGGER.info("Initializing  database '" + name + "'");
    }

    @Override
    public void onFailure(Throwable t, Response response) {
        LOGGER.error("Error initializing database '" + name + "'");
        latch.countDown();
    }

    @Override
    public void onClose(int code, String reason) {
        LOGGER.info("Init executor close: [" + code + "] " + reason);
        latch.countDown();
    }
}
