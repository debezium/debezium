/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.strategy;

import static io.debezium.config.CommonConnectorConfig.DATABASE_CONFIG_PREFIX;
import static io.debezium.config.CommonConnectorConfig.DRIVER_CONFIG_PREFIX;

import java.time.Duration;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.Strings;

/**
 * @author Chris Cranford
 */
public abstract class AbstractConnectionConfiguration implements ConnectionConfiguration {

    public static final String URL_PATTERN = "${protocol}://${hostname}:${port}/?useInformationSchema=true&nullCatalogMeansCurrent=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&connectTimeout=${connectTimeout}";

    private final JdbcConfiguration jdbcConfig;
    private final JdbcConnection.ConnectionFactory factory;
    private final Configuration config;

    public AbstractConnectionConfiguration(Configuration config) {
        // Set up the JDBC connection without actually connecting, with extra MySQL-specific properties
        // to give us better JDBC database metadata behavior, including using UTF-8 for the client-side character encoding
        // per https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-charsets.html
        this.config = config;
        final boolean useSSL = sslModeEnabled();
        final Configuration dbConfig = config
                .edit()
                .withDefault(MySqlConnectorConfig.PORT, MySqlConnectorConfig.PORT.defaultValue())
                .withDefault(MySqlConnectorConfig.JDBC_PROTOCOL, MySqlConnectorConfig.JDBC_PROTOCOL.defaultValue())
                .build()
                .subset(DATABASE_CONFIG_PREFIX, true)
                .merge(config.subset(DRIVER_CONFIG_PREFIX, true));

        final Configuration.Builder jdbcConfigBuilder = dbConfig
                .edit()
                .with("connectTimeout", Long.toString(getConnectionTimeout().toMillis()))
                .with("sslMode", sslMode().getValue());

        if (useSSL) {
            if (!Strings.isNullOrBlank(sslTrustStore())) {
                jdbcConfigBuilder.with("trustCertificateKeyStoreUrl", "file:" + sslTrustStore());
            }
            if (sslTrustStorePassword() != null) {
                jdbcConfigBuilder.with("trustCertificateKeyStorePassword", String.valueOf(sslTrustStorePassword()));
            }
            if (!Strings.isNullOrBlank(sslKeyStore())) {
                jdbcConfigBuilder.with("clientCertificateKeyStoreUrl", "file:" + sslKeyStore());
            }
            if (sslKeyStorePassword() != null) {
                jdbcConfigBuilder.with("clientCertificateKeyStorePassword", String.valueOf(sslKeyStorePassword()));
            }
        }

        jdbcConfigBuilder.with(getConnectionTimeZonePropertyName(), resolveConnectionTimeZone(dbConfig));

        // Set and remove options to prevent potential vulnerabilities
        jdbcConfigBuilder
                .with("allowLoadLocalInfile", "false")
                .with("allowUrlInLocalInfile", "false")
                .with("autoDeserialize", false)
                .without("queryInterceptors");

        this.jdbcConfig = JdbcConfiguration.adapt(jdbcConfigBuilder.build());
        String driverClassName = this.config.getString(MySqlConnectorConfig.JDBC_DRIVER);
        Field protocol = MySqlConnectorConfig.JDBC_PROTOCOL;

        factory = JdbcConnection.patternBasedFactory(URL_PATTERN, driverClassName, getClass().getClassLoader(), protocol);

    }

    @Override
    public JdbcConfiguration config() {
        return jdbcConfig;
    }

    @Override
    public Configuration originalConfig() {
        return config;
    }

    @Override
    public JdbcConnection.ConnectionFactory factory() {
        return factory;
    }

    @Override
    public String username() {
        return config.getString(MySqlConnectorConfig.USER);
    }

    @Override
    public String password() {
        return config.getString(MySqlConnectorConfig.PASSWORD);
    }

    @Override
    public String hostname() {
        return config.getString(MySqlConnectorConfig.HOSTNAME);
    }

    @Override
    public int port() {
        return config.getInteger(MySqlConnectorConfig.PORT);
    }

    @Override
    public MySqlConnectorConfig.SecureConnectionMode sslMode() {
        String mode = config.getString(MySqlConnectorConfig.SSL_MODE);
        return MySqlConnectorConfig.SecureConnectionMode.parse(mode);
    }

    @Override
    public boolean sslModeEnabled() {
        return sslMode() != MySqlConnectorConfig.SecureConnectionMode.DISABLED;
    }

    public String sslKeyStore() {
        return config.getString(MySqlConnectorConfig.SSL_KEYSTORE);
    }

    public char[] sslKeyStorePassword() {
        String password = config.getString(MySqlConnectorConfig.SSL_KEYSTORE_PASSWORD);
        return Strings.isNullOrBlank(password) ? null : password.toCharArray();
    }

    public String sslTrustStore() {
        return config.getString(MySqlConnectorConfig.SSL_TRUSTSTORE);
    }

    public char[] sslTrustStorePassword() {
        String password = config.getString(MySqlConnectorConfig.SSL_TRUSTSTORE_PASSWORD);
        return Strings.isNullOrBlank(password) ? null : password.toCharArray();
    }

    public Duration getConnectionTimeout() {
        return Duration.ofMillis(config.getLong(MySqlConnectorConfig.CONNECTION_TIMEOUT_MS));
    }

    public CommonConnectorConfig.EventProcessingFailureHandlingMode eventProcessingFailureHandlingMode() {
        String mode = config.getString(CommonConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE);
        if (mode == null) {
            mode = config.getString(MySqlConnectorConfig.EVENT_DESERIALIZATION_FAILURE_HANDLING_MODE);
        }
        return CommonConnectorConfig.EventProcessingFailureHandlingMode.parse(mode);
    }

    public CommonConnectorConfig.EventProcessingFailureHandlingMode inconsistentSchemaHandlingMode() {
        String mode = config.getString(MySqlConnectorConfig.INCONSISTENT_SCHEMA_HANDLING_MODE);
        return CommonConnectorConfig.EventProcessingFailureHandlingMode.parse(mode);
    }

    protected abstract String getConnectionTimeZonePropertyName();

    protected abstract String resolveConnectionTimeZone(Configuration dbConfig);
}
