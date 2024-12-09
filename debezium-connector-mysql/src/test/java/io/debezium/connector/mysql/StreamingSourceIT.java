/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static io.debezium.junit.EqualityCheck.LESS_THAN_OR_EQUAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import io.debezium.connector.binlog.BinlogStreamingSourceIT;
import io.debezium.connector.binlog.junit.SkipWhenDatabaseIs;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;
import io.debezium.junit.SkipWhenDatabaseVersion;

/**
 * @author Randall Hauch, Jiri Pechanec
 *
 */
public class StreamingSourceIT extends BinlogStreamingSourceIT<MySqlConnector> implements MySqlCommon {

    private static final String SET_TLS_PROTOCOLS = "database.enabledTLSProtocols";

    @Test
    @FixFor("DBZ-1208")
    @SkipWhenDatabaseIs(value = SkipWhenDatabaseIs.Type.MYSQL, versions = @SkipWhenDatabaseVersion(check = LESS_THAN_OR_EQUAL, major = 5, minor = 6, reason = "MySQL 5.6 does not support SSL"))
    @SkipWhenDatabaseIs(value = SkipWhenDatabaseIs.Type.MARIADB, reason = "MariaDB does not support SSL by default")
    public void shouldFailOnUnknownTlsProtocol() {
        final UniqueDatabase REGRESSION_DATABASE = TestHelper.getUniqueDatabase("logical_server_name", "regression_test")
                .withDbHistoryPath(SCHEMA_HISTORY_PATH);
        REGRESSION_DATABASE.createAndInitialize();

        config = simpleConfig()
                .with(MySqlConnectorConfig.SSL_MODE, MySqlConnectorConfig.MySqlSecureConnectionMode.REQUIRED)
                .with(SET_TLS_PROTOCOLS, "TLSv1.7")
                .build();

        // Start the connector ...
        Map<String, Object> result = new HashMap<>();
        start(getConnectorClass(), config, (success, message, error) -> {
            result.put("success", success);
            result.put("message", message);
        });

        assertEquals(false, result.get("success"));
        assertEquals(
                "Connector configuration is not valid. Unable to connect: Specified list of TLS versions only contains non valid TLS protocols. Accepted values are TLSv1.2 and TLSv1.3.",
                result.get("message").toString());
    }

    @Test
    @FixFor("DBZ-1208")
    @SkipWhenDatabaseIs(value = SkipWhenDatabaseIs.Type.MYSQL, versions = @SkipWhenDatabaseVersion(check = LESS_THAN_OR_EQUAL, major = 5, minor = 6, reason = "MySQL 5.6 does not support SSL"))
    @SkipWhenDatabaseIs(value = SkipWhenDatabaseIs.Type.MARIADB, reason = "MariaDB does not support SSL by default")
    public void shouldAcceptTls12() throws Exception {
        final UniqueDatabase REGRESSION_DATABASE = TestHelper.getUniqueDatabase("logical_server_name", "regression_test")
                .withDbHistoryPath(SCHEMA_HISTORY_PATH);
        REGRESSION_DATABASE.createAndInitialize();

        config = simpleConfig()
                .with(MySqlConnectorConfig.SSL_MODE, MySqlConnectorConfig.MySqlSecureConnectionMode.REQUIRED)
                .with(SET_TLS_PROTOCOLS, "TLSv1.2")
                .build();

        // Start the connector ...
        AtomicReference<Throwable> exception = new AtomicReference<>();
        start(getConnectorClass(), config, (success, message, error) -> exception.set(error));

        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), "streaming");
        assertThat(exception.get()).isNull();
    }

}
