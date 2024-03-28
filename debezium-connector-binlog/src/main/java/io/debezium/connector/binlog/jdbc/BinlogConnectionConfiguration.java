/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.jdbc;

import static io.debezium.config.CommonConnectorConfig.DATABASE_CONFIG_PREFIX;
import static io.debezium.config.CommonConnectorConfig.DRIVER_CONFIG_PREFIX;

import java.time.Duration;
import java.util.Objects;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection.ConnectionFactory;
import io.debezium.util.Strings;

/**
 * @author Chris Cranford
 */
public abstract class BinlogConnectionConfiguration implements ConnectionConfiguration {

    private final JdbcConfiguration jdbcConfig;
    private final ConnectionFactory factory;
    private final Configuration configuration;

    public BinlogConnectionConfiguration(Configuration configuration) {
        this.configuration = configuration;
        this.jdbcConfig = getJdbcConfiguration(getDatabaseConfiguration(configuration)
                .build()
                .subset(DATABASE_CONFIG_PREFIX, true)
                .merge(configuration.subset(DRIVER_CONFIG_PREFIX, true)));
        this.factory = createFactory(configuration);
    }

    @Override
    public JdbcConfiguration config() {
        return jdbcConfig;
    }

    @Override
    public Configuration originalConfig() {
        return configuration;
    }

    @Override
    public ConnectionFactory factory() {
        return factory;
    }

    @Override
    public String username() {
        return configuration.getString(BinlogConnectorConfig.USER);
    }

    @Override
    public String password() {
        return configuration.getString(BinlogConnectorConfig.PASSWORD);
    }

    @Override
    public String hostname() {
        return configuration.getString(BinlogConnectorConfig.HOSTNAME);
    }

    @Override
    public int port() {
        return configuration.getInteger(BinlogConnectorConfig.PORT);
    }

    @Override
    public BinlogConnectorConfig.SecureConnectionMode sslMode() {
        final String sslMode = configuration.getString(BinlogConnectorConfig.SSL_MODE);
        return BinlogConnectorConfig.SecureConnectionMode.parse(sslMode);
    }

    @Override
    public boolean sslModeEnabled() {
        return sslMode() != BinlogConnectorConfig.SecureConnectionMode.DISABLED;
    }

    @Override
    public String sslKeyStore() {
        return configuration.getString(BinlogConnectorConfig.SSL_KEYSTORE);
    }

    @Override
    public char[] sslKeyStorePassword() {
        final String password = configuration.getString(BinlogConnectorConfig.SSL_KEYSTORE_PASSWORD);
        return Strings.isNullOrBlank(password) ? null : password.toCharArray();
    }

    @Override
    public String sslTrustStore() {
        return configuration.getString(BinlogConnectorConfig.SSL_TRUSTSTORE);
    }

    @Override
    public char[] sslTrustStorePassword() {
        final String password = configuration.getString(BinlogConnectorConfig.SSL_TRUSTSTORE_PASSWORD);
        return Strings.isNullOrBlank(password) ? null : password.toCharArray();
    }

    public abstract String getUrlPattern();

    protected Configuration.Builder getDatabaseConfiguration(Configuration configuration) {
        return configuration.edit().with(BinlogConnectorConfig.PORT, BinlogConnectorConfig.PORT.defaultValue());
    }

    protected JdbcConfiguration getJdbcConfiguration(Configuration configuration) {
        final Configuration.Builder builder = configuration.edit()
                .with("connectTimeout", Long.toString(getConnectionTimeout(configuration).toMillis()))
                .with("sslMode", sslMode().getValue())
                .with(getConnectionTimeZonePropertyName(), resolveConnectionTimeZone(configuration))
                .with("allowLoadLocalInfile", Boolean.FALSE.toString())
                .with("allowUrlInLocalInfile", Boolean.FALSE.toString())
                .with("autoDeserialize", Boolean.FALSE.toString())
                .without("queryInterceptors");

        if (sslModeEnabled()) {
            if (!Strings.isNullOrBlank(sslTrustStore())) {
                builder.with("trustCertificateKeyStoreUrl", "file:" + sslTrustStore());
            }
            if (!Objects.isNull(sslTrustStorePassword())) {
                builder.with("trustCertificateKeyStorePassword", String.valueOf(sslTrustStorePassword()));
            }
            if (!Strings.isNullOrBlank(sslKeyStore())) {
                builder.with("clientCertificateKeyStoreUrl", "file:" + sslKeyStore());
            }
            if (!Objects.isNull(sslKeyStorePassword())) {
                builder.with("clientCertificateKeyStorePassword", String.valueOf(sslKeyStorePassword()));
            }
        }

        return JdbcConfiguration.adapt(builder.build());
    }

    protected Duration getConnectionTimeout(Configuration configuration) {
        return Duration.ofMillis(configuration.getLong(BinlogConnectorConfig.CONNECTION_TIMEOUT_MS));
    }

    protected abstract String getConnectionTimeZonePropertyName();

    protected abstract String resolveConnectionTimeZone(Configuration configuration);

    protected abstract ConnectionFactory createFactory(Configuration configuration);

}
