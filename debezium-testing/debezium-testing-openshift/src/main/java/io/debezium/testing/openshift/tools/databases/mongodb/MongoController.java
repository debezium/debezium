/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.tools.databases.mongodb;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.openshift.tools.databases.DatabaseController;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.dsl.ExecListener;
import io.fabric8.kubernetes.client.dsl.ExecWatch;
import io.fabric8.openshift.client.OpenShiftClient;

import okhttp3.Response;

/**
 *
 * @author Jakub Cechacek
 */
public class MongoController extends DatabaseController<MongoDatabaseClient> {
    private static class MongoInitListener implements ExecListener {

        private CountDownLatch latch;

        public MongoInitListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onOpen(Response response) {
            LOGGER.info("Initializing MongoDB database");
        }

        @Override
        public void onFailure(Throwable t, Response response) {
            LOGGER.error("Error initializing MongoDB database");
            LOGGER.error(response.message());
            latch.countDown();
        }

        @Override
        public void onClose(int code, String reason) {
            LOGGER.info("MongoDb init executor close: [" + code + "] " + reason);
            latch.countDown();
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoController.class);
    private static final String DB_INIT_SCRIPT_PATH_CONTAINER = "/usr/local/bin/init-inventory.sh";

    public MongoController(Deployment deployment, List<Service> services, String dbType, OpenShiftClient ocp) {
        super(deployment, services, dbType, ocp);
    }

    @Override
    protected String constructDatabaseUrl(String hostname, int port) {
        return "mongodb://" + hostname + ":" + port;
    }

    public void initialize() throws InterruptedException {
        Pod pod = ocp.pods().inNamespace(project).withLabel("deployment", name).list().getItems().get(0);
        CountDownLatch latch = new CountDownLatch(1);
        try (ExecWatch exec = ocp.pods().inNamespace(project).withName(pod.getMetadata().getName())
                .inContainer("mongo")
                .writingOutput(System.out) // CHECKSTYLE IGNORE RegexpSinglelineJava FOR NEXT 2 LINES
                .writingError(System.err)
                .usingListener(new MongoInitListener(latch))
                .exec("bash", "-c", DB_INIT_SCRIPT_PATH_CONTAINER)) {
            LOGGER.info("Waiting until database is initialized");
            latch.await(1, TimeUnit.MINUTES);
        }
    }

    public MongoDatabaseClient getDatabaseClient(String username, String password) {
        return getDatabaseClient(username, password, "admin");
    }

    public MongoDatabaseClient getDatabaseClient(String username, String password, String authSource) {
        return new MongoDatabaseClient(getDatabaseUrl(), username, password, authSource);
    }
}
