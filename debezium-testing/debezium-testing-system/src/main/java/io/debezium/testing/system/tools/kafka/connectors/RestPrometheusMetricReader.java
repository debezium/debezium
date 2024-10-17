/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka.connectors;

import static io.debezium.testing.system.tools.WaitConditions.scaled;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class RestPrometheusMetricReader implements ConnectorMetricsReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(RestPrometheusMetricReader.class);
    private final HttpUrl url;

    public RestPrometheusMetricReader(HttpUrl url) {
        this.url = url;
    }

    public List<String> getMetrics() {
        LOGGER.info("Retrieving connector metrics from " + url.toString());
        OkHttpClient httpClient = new OkHttpClient();
        Request r = new Request.Builder().url(url).get().build();

        try (Response res = httpClient.newCall(r).execute()) {
            String metrics = res.body().string();
            return Stream.of(metrics.split("\\r?\\n")).collect(Collectors.toList());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Waits until Snapshot phase of given connector completes
     *
     * @param connectorName name of the connect
     * @param metricName    name of the metric used to determine the state
     */
    public void waitForSnapshot(String connectorName, String metricName) {
        LOGGER.info("Waiting for connector '" + connectorName + "' to finish snapshot");
        await()
                .atMost(scaled(5), TimeUnit.MINUTES)
                .pollInterval(10, TimeUnit.SECONDS)
                .until(() -> getMetrics().stream().anyMatch(s -> s.contains(metricName) && s.contains(connectorName)));
    }

    @Override
    public void waitForMySqlSnapshot(String connectorName) {
        waitForSnapshot(connectorName, "debezium_mysql_connector_metrics_snapshotcompleted");
    }

    @Override
    public void waitForMariaDbSnapshot(String connectorName) {
        waitForSnapshot(connectorName, "debezium_mariadb_connector_metrics_snapshotcompleted");
    }

    @Override
    public void waitForPostgreSqlSnapshot(String connectorName) {
        waitForSnapshot(connectorName, "debezium_postgres_connector_metrics_snapshotcompleted");
    }

    @Override
    public void waitForSqlServerSnapshot(String connectorName) {
        waitForSnapshot(connectorName, "debezium_sql_server_connector_metrics_snapshotcompleted");
    }

    @Override
    public void waitForMongoSnapshot(String connectorName) {
        waitForSnapshot(connectorName, "debezium_mongodb_connector_metrics_snapshotcompleted");
    }

    @Override
    public void waitForDB2Snapshot(String connectorName) {
        waitForSnapshot(connectorName, "debezium_db2_server_connector_metrics_snapshotcompleted");
    }

    @Override
    public void waitForOracleSnapshot(String connectorName) {
        waitForSnapshot(connectorName, "debezium_oracle_connector_metrics_snapshotcompleted");
    }

}
