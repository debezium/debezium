/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.util;

import java.math.BigInteger;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import javax.management.JMException;

import org.awaitility.Awaitility;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.Scn;
import io.debezium.embedded.util.MetricsHelper;

/**
 * Set of Oracle connector specific utility methods that interact with the connector metrics.
 *
 * @author Chris Cranford
 */
public class OracleMetricsHelper extends MetricsHelper {

    public static Long getFetchingQueryCount() throws JMException {
        return getStreamingMetric("FetchQueryCount");
    }

    public static BigInteger getOffsetScn() throws JMException {
        return getStreamingMetric("OffsetScn");
    }

    public static BigInteger getCommittedScn() throws JMException {
        return getStreamingMetric("CommittedScn");
    }

    public static Long getTotalCapturedDmlCount() throws JMException {
        return getStreamingMetric("TotalCapturedDmlCount");
    }

    public static void waitForFetchQueryCountGreaterThan(long count) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .until(() -> count <= getFetchingQueryCount());
    }

    public static void waitForCurrentScnToHaveBeenSeenByConnector() throws SQLException {
        try (OracleConnection admin = TestHelper.adminConnection(true)) {
            final Scn scn = admin.getCurrentScn();
            Awaitility.await()
                    .atMost(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS)
                    .until(() -> {
                        final BigInteger scnValue = getStreamingMetric("CurrentScn");
                        if (scnValue == null) {
                            return false;
                        }
                        return new Scn(scnValue).compareTo(scn) > 0;
                    });
        }
    }

    public static void waitForCurrentScnAfter(Scn scn) {
        Awaitility.await()
                .atMost(Duration.ofMinutes(5))
                .pollInterval(Duration.ofSeconds(2))
                .until(() -> new Scn(getStreamingMetric("CurrentScn")).compareTo(scn) > 0);
    }

    public static void waitForOffsetScnAfter(Scn scn) {
        Awaitility.await()
                .atMost(Duration.ofMinutes(5))
                .pollInterval(Duration.ofSeconds(2))
                .until(() -> {
                    final BigInteger offsetScn = getStreamingMetric("OffsetScn");
                    return offsetScn != null && Scn.valueOf(offsetScn.toString()).compareTo(scn) > 0;
                });
    }

    public static <T> T getStreamingMetric(String metricName) {
        return getStreamingMetric(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME, getStreamingNamespace(), metricName);
    }
}
