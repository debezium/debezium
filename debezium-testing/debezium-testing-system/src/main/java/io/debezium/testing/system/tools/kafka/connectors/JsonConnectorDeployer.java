/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka.connectors;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;

import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class JsonConnectorDeployer implements ConnectorDeployer {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonConnectorDeployer.class);

    private final HttpUrl apiUrl;
    private final OkHttpClient http;

    public JsonConnectorDeployer(HttpUrl apiURL, OkHttpClient http) {
        this.apiUrl = apiURL;
        this.http = http;
    }

    @Override
    public void deploy(ConnectorConfigBuilder config) {
        LOGGER.info("Deploying connector JSON for connector " + config.getConnectorName());

        HttpUrl url = apiUrl.resolve("/connectors/" + config.getConnectorName() + "/config");
        Request r = new Request.Builder()
                .url(url)
                .put(RequestBody.create(config.getJsonString(), MediaType.parse("application/json")))
                .build();

        try (Response res = http.newCall(r).execute()) {
            if (!res.isSuccessful()) {
                LOGGER.error(res.request().url().toString());
                LOGGER.error(new String(res.body().bytes()));
                throw new RuntimeException("Connector registration request returned status code '" + res.code() + "'");
            }
            LOGGER.info("Registered kafka connector '" + config.getConnectorName() + "'");
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void undeploy(String name) {
        LOGGER.info("Undeploying kafka connector " + name);
        HttpUrl url = apiUrl.resolve("/connectors/" + name);
        Request r = new Request.Builder().url(url).delete().build();

        try (Response res = http.newCall(r).execute()) {
            if (!res.isSuccessful()) {
                LOGGER.error(res.request().url().toString());
                throw new RuntimeException("Connector deletion request returned status code '" + res.code() + "'");
            }
            LOGGER.info("Deleted kafka connector '" + name + "'");
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
