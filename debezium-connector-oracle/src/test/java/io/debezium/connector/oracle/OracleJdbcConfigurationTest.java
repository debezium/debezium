/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.jdbc.DualOracleConnectionFactory;
import io.debezium.connector.oracle.jdbc.OracleConnectionFactory;
import io.debezium.connector.oracle.jdbc.OracleConnectionFactoryProvider;
import io.debezium.connector.oracle.jdbc.OracleJdbcConfiguration;
import io.debezium.connector.oracle.jdbc.StandardOracleConnectionFactory;
import io.debezium.doc.FixFor;
import io.debezium.junit.logging.LogInterceptor;

import ch.qos.logback.classic.Level;

/**
 * Tests Oracle JDBC Connection Factory's resolution of different setups
 *
 * @author Chris Cranford
 */
public class OracleJdbcConfigurationTest {

    private LogInterceptor providerLogInterceptor;
    private LogInterceptor standardFactoryLogInterceptor;

    @BeforeEach
    public void beforeEachTest() {
        this.providerLogInterceptor = new LogInterceptor(OracleConnectionFactoryProvider.class);
        this.providerLogInterceptor.setLoggerLevel(OracleConnectionFactoryProvider.class, Level.DEBUG);

        this.standardFactoryLogInterceptor = new LogInterceptor(StandardOracleConnectionFactory.class);
    }

    @Test
    @FixFor("debezium/dbz#2257")
    public void shouldResolveStandardConnectionFactoryForPrimaryCaptureMode() {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                .with(OracleConnectorConfig.HOSTNAME, "primary")
                .with(OracleConnectorConfig.DATABASE_NAME, "primarydb")
                .build();

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        final OracleConnectionFactory factory = OracleConnectionFactoryProvider.create(connectorConfig);
        assertThat(factory).isInstanceOf(StandardOracleConnectionFactory.class);
        assertThat(providerLogInterceptor.containsMessage("Using STANDARD connection factory")).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2257")
    public void shouldResolveStandardConnectionFactoryWhenCaptureModeIsExplicitlyPrimary() {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                .with(OracleConnectorConfig.CAPTURE_MODE, "primary")
                .with(OracleConnectorConfig.HOSTNAME, "primary")
                .with(OracleConnectorConfig.DATABASE_NAME, "primarydb")
                .build();

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        final OracleConnectionFactory factory = OracleConnectionFactoryProvider.create(connectorConfig);
        assertThat(factory).isInstanceOf(StandardOracleConnectionFactory.class);
        assertThat(providerLogInterceptor.containsMessage("Using STANDARD connection factory")).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2257")
    public void shouldNotResolvePrimaryAsReadOnlyByDefault() {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                .with(OracleConnectorConfig.HOSTNAME, "primary")
                .with(OracleConnectorConfig.DATABASE_NAME, "primarydb")
                .build();

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        final OracleConnectionFactory factory = OracleConnectionFactoryProvider.create(connectorConfig);
        assertThat(factory).isInstanceOf(StandardOracleConnectionFactory.class);
        assertThat(standardFactoryLogInterceptor.containsMessage("Primary connection is read-only.")).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2257")
    public void shouldResolvePrimaryAsReadOnlyForPrimaryCaptureMode() {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                .with(OracleConnectorConfig.HOSTNAME, "primary")
                .with(OracleConnectorConfig.DATABASE_NAME, "primarydb")
                .with(OracleConnectorConfig.LOG_MINING_READ_ONLY, "true")
                .build();

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        final OracleConnectionFactory factory = OracleConnectionFactoryProvider.create(connectorConfig);
        assertThat(factory).isInstanceOf(StandardOracleConnectionFactory.class);
        assertThat(standardFactoryLogInterceptor.containsMessage("Primary connection is read-only.")).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2257")
    public void shouldResolvePrimaryAsReadOnlyForPhysicalStandbyCaptureMode() {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                .with(OracleConnectorConfig.CAPTURE_MODE, "physical_standby")
                .with(OracleConnectorConfig.LOG_MINING_READ_ONLY, "true")
                .with(OracleConnectorConfig.HOSTNAME, "primary")
                .with(OracleConnectorConfig.DATABASE_NAME, "primarydb")
                .with(OracleConnectorConfig.SECONDARY_HOSTNAME, "secondary")
                .with(OracleConnectorConfig.SECONDARY_DATABASE, "secondarydb")
                .with(OracleConnectorConfig.SECONDARY_PORT, "1522")
                .build();

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        final OracleConnectionFactory factory = OracleConnectionFactoryProvider.create(connectorConfig);
        assertThat(factory).isInstanceOf(DualOracleConnectionFactory.class);
        assertThat(standardFactoryLogInterceptor.containsMessage("Primary connection is read-only.")).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2257")
    public void shouldNotResolvePrimaryAsReadOnlyForPhysicalStandbyCaptureMode() {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                .with(OracleConnectorConfig.CAPTURE_MODE, "physical_standby")
                .with(OracleConnectorConfig.HOSTNAME, "primary")
                .with(OracleConnectorConfig.DATABASE_NAME, "primarydb")
                .with(OracleConnectorConfig.SECONDARY_HOSTNAME, "secondary")
                .with(OracleConnectorConfig.SECONDARY_DATABASE, "secondarydb")
                .with(OracleConnectorConfig.SECONDARY_PORT, "1522")
                .build();

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        final OracleConnectionFactory factory = OracleConnectionFactoryProvider.create(connectorConfig);
        assertThat(factory).isInstanceOf(DualOracleConnectionFactory.class);
        assertThat(standardFactoryLogInterceptor.containsMessage("Primary connection is read-only.")).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2257")
    public void shouldResolveDualConnectionFactoryForPhysicalStandbyCaptureMode() {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                .with(OracleConnectorConfig.CAPTURE_MODE, "physical_standby")
                .with(OracleConnectorConfig.HOSTNAME, "primary")
                .with(OracleConnectorConfig.DATABASE_NAME, "primarydb")
                .with(OracleConnectorConfig.SECONDARY_HOSTNAME, "secondary")
                .with(OracleConnectorConfig.SECONDARY_DATABASE, "secondarydb")
                .build();

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        final OracleConnectionFactory factory = OracleConnectionFactoryProvider.create(connectorConfig);
        assertThat(factory).isInstanceOf(DualOracleConnectionFactory.class);
        assertThat(providerLogInterceptor.containsMessage("Using DUAL connection factory - Streams from Physical Standby")).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2257")
    public void shouldResolveDualConnectionFactoryForDownstreamCaptureMode() {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                .with(OracleConnectorConfig.CAPTURE_MODE, "downstream")
                .with(OracleConnectorConfig.HOSTNAME, "primary")
                .with(OracleConnectorConfig.DATABASE_NAME, "primarydb")
                .with(OracleConnectorConfig.SECONDARY_HOSTNAME, "secondary")
                .with(OracleConnectorConfig.SECONDARY_DATABASE, "secondarydb")
                .build();

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        final OracleConnectionFactory factory = OracleConnectionFactoryProvider.create(connectorConfig);
        assertThat(factory).isInstanceOf(DualOracleConnectionFactory.class);
        assertThat(providerLogInterceptor.containsMessage("Using DUAL connection factory - Streams from Downstream Mining Instance")).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2257")
    public void shouldResolveSecondaryPortBasedOnPrimaryPort() {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                .with(OracleConnectorConfig.CAPTURE_MODE, "physical_standby")
                .with(OracleConnectorConfig.HOSTNAME, "primary")
                .with(OracleConnectorConfig.DATABASE_NAME, "primarydb")
                .with(OracleConnectorConfig.SECONDARY_HOSTNAME, "secondary")
                .with(OracleConnectorConfig.SECONDARY_DATABASE, "secondarydb")
                .build();

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        final OracleConnectionFactory factory = OracleConnectionFactoryProvider.create(connectorConfig);
        assertThat(factory).isInstanceOf(DualOracleConnectionFactory.class);
        assertThat(standardFactoryLogInterceptor.containsMessage("Primary connection is read-only.")).isFalse();
        assertThat(providerLogInterceptor.containsMessage("Secondary connection properties: {hostname=secondary, dbname=secondarydb}")).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2257")
    public void shouldResolveSecondaryPortFromConfiguration() {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                .with(OracleConnectorConfig.CAPTURE_MODE, "physical_standby")
                .with(OracleConnectorConfig.HOSTNAME, "primary")
                .with(OracleConnectorConfig.DATABASE_NAME, "primarydb")
                .with(OracleConnectorConfig.SECONDARY_HOSTNAME, "secondary")
                .with(OracleConnectorConfig.SECONDARY_DATABASE, "secondarydb")
                .with(OracleConnectorConfig.SECONDARY_PORT, "1522")
                .build();

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        final OracleConnectionFactory factory = OracleConnectionFactoryProvider.create(connectorConfig);
        assertThat(factory).isInstanceOf(DualOracleConnectionFactory.class);
        assertThat(standardFactoryLogInterceptor.containsMessage("Primary connection is read-only.")).isFalse();
        assertThat(providerLogInterceptor.containsMessage("Secondary connection properties: {hostname=secondary, dbname=secondarydb, port=1522}")).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2257")
    public void shouldStripSecondaryPrefixedKeysFromResolvedSecondaryConfig() {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                .with(OracleConnectorConfig.CAPTURE_MODE, "physical_standby")
                .with(OracleConnectorConfig.HOSTNAME, "primary")
                .with(OracleConnectorConfig.DATABASE_NAME, "primarydb")
                .with(OracleConnectorConfig.SECONDARY_HOSTNAME, "secondary")
                .with(OracleConnectorConfig.SECONDARY_DATABASE, "secondarydb")
                .with(OracleConnectorConfig.SECONDARY_PORT, "1522")
                .build();

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        OracleConnectionFactoryProvider.create(connectorConfig);
        assertThat(providerLogInterceptor.getLogEntriesThatContainsMessage("Secondary connection properties:"))
                .isNotEmpty()
                .noneMatch(entry -> entry.contains("secondary."));
    }

    @Test
    @FixFor("debezium/dbz#2257")
    public void shouldResolveSecondaryUsingUrlInsteadOfHostname() {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                .with(OracleConnectorConfig.CAPTURE_MODE, "physical_standby")
                .with(OracleConnectorConfig.HOSTNAME, "primary")
                .with(OracleConnectorConfig.DATABASE_NAME, "primarydb")
                .with(OracleConnectorConfig.SECONDARY_URL, "jdbc:oracle:thin:@secondary:1521/secondarydb")
                .with(OracleConnectorConfig.SECONDARY_DATABASE, "secondarydb")
                .build();

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        final OracleConnectionFactory factory = OracleConnectionFactoryProvider.create(connectorConfig);
        assertThat(factory).isInstanceOf(DualOracleConnectionFactory.class);
        assertThat(providerLogInterceptor.containsMessage("url=jdbc:oracle:thin:@secondary:1521/secondarydb")).isTrue();
        assertThat(providerLogInterceptor.containsMessage("hostname=secondary")).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2257")
    public void shouldResolveSecondaryUrlOverHostnameWhenBothSpecified() {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                .with(OracleConnectorConfig.CAPTURE_MODE, "physical_standby")
                .with(OracleConnectorConfig.HOSTNAME, "primary")
                .with(OracleConnectorConfig.DATABASE_NAME, "primarydb")
                .with(OracleConnectorConfig.SECONDARY_URL, "jdbc:oracle:thin:@secondary:1521/secondarydb")
                .with(OracleConnectorConfig.SECONDARY_HOSTNAME, "secondary-host")
                .with(OracleConnectorConfig.SECONDARY_DATABASE, "secondarydb")
                .build();

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        final OracleConnectionFactory factory = OracleConnectionFactoryProvider.create(connectorConfig);
        assertThat(factory).isInstanceOf(DualOracleConnectionFactory.class);
        assertThat(providerLogInterceptor.containsMessage("url=jdbc:oracle:thin:@secondary:1521/secondarydb")).isTrue();
        assertThat(providerLogInterceptor.containsMessage("hostname=secondary-host")).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2257")
    public void shouldResolveSecondaryWithoutHostnameOrUrlFallsToPrimary() {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                .with(OracleConnectorConfig.CAPTURE_MODE, "physical_standby")
                .with(OracleConnectorConfig.HOSTNAME, "primary")
                .with(OracleConnectorConfig.DATABASE_NAME, "primarydb")
                .with(OracleConnectorConfig.SECONDARY_DATABASE, "secondarydb")
                .build();

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        final OracleConnectionFactory factory = OracleConnectionFactoryProvider.create(connectorConfig);
        assertThat(factory).isInstanceOf(DualOracleConnectionFactory.class);
        assertThat(providerLogInterceptor.containsMessage("hostname=primary")).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2257")
    public void shouldResolveSecondaryDatabaseNameFromConfiguration() {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                .with(OracleConnectorConfig.CAPTURE_MODE, "physical_standby")
                .with(OracleConnectorConfig.HOSTNAME, "primary")
                .with(OracleConnectorConfig.DATABASE_NAME, "primarydb")
                .with(OracleConnectorConfig.SECONDARY_HOSTNAME, "secondary")
                .with(OracleConnectorConfig.SECONDARY_DATABASE, "secondarydb")
                .build();

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        final OracleConnectionFactory factory = OracleConnectionFactoryProvider.create(connectorConfig);
        assertThat(factory).isInstanceOf(DualOracleConnectionFactory.class);
        assertThat(providerLogInterceptor.containsMessage("dbname=secondarydb")).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2257")
    public void shouldResolveSecondaryDatabaseNameFromPrimaryWhenNotSpecified() {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "myserver")
                .with(OracleConnectorConfig.CAPTURE_MODE, "physical_standby")
                .with(OracleConnectorConfig.HOSTNAME, "primary")
                .with(OracleConnectorConfig.DATABASE_NAME, "primarydb")
                .with(OracleConnectorConfig.SECONDARY_HOSTNAME, "secondary")
                .build();

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        final OracleConnectionFactory factory = OracleConnectionFactoryProvider.create(connectorConfig);
        assertThat(factory).isInstanceOf(DualOracleConnectionFactory.class);
        assertThat(providerLogInterceptor.containsMessage("dbname=primarydb")).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2257")
    public void shouldReturnNullForSecondaryFieldsWhenNotConfigured() {
        final OracleJdbcConfiguration jdbcConfig = OracleJdbcConfiguration.adapt(
                Configuration.create()
                        .with("hostname", "primary")
                        .with("dbname", "primarydb")
                        .build());

        assertThat(jdbcConfig.getSecondaryHostname()).isNull();
        assertThat(jdbcConfig.getSecondaryDatabaseName()).isNull();
        assertThat(jdbcConfig.getSecondaryUrl()).isNull();
        assertThat(jdbcConfig.getSecondaryPort()).isEqualTo(0);
    }

    @Test
    @FixFor("debezium/dbz#2257")
    public void shouldReturnZeroForSecondaryPortWhenNotConfigured() {
        final OracleJdbcConfiguration jdbcConfig = OracleJdbcConfiguration.adapt(
                Configuration.create()
                        .with("hostname", "primary")
                        .with("dbname", "primarydb")
                        .build());

        assertThat(jdbcConfig.getSecondaryPort()).isEqualTo(0);
    }

    @Test
    @FixFor("debezium/dbz#2257")
    public void shouldReturnConfiguredSecondaryPort() {
        final OracleJdbcConfiguration jdbcConfig = OracleJdbcConfiguration.adapt(
                Configuration.create()
                        .with("secondary.port", "1522")
                        .build());

        assertThat(jdbcConfig.getSecondaryPort()).isEqualTo(1522);
    }

    @Test
    @FixFor("debezium/dbz#2257")
    public void shouldAdaptExistingOracleJdbcConfigurationWithoutWrapping() {
        final OracleJdbcConfiguration original = OracleJdbcConfiguration.adapt(
                Configuration.create()
                        .with("hostname", "primary")
                        .with("dbname", "primarydb")
                        .build());

        final OracleJdbcConfiguration adapted = OracleJdbcConfiguration.adapt(original);
        assertThat(adapted).isSameAs(original);
    }

    @Test
    @FixFor("debezium/dbz#2257")
    public void shouldAdaptPlainConfigurationToOracleJdbcConfiguration() {
        final Configuration plain = Configuration.create()
                .with("hostname", "primary")
                .with("dbname", "primarydb")
                .with("url", "jdbc:oracle:thin:@primary:1521/primarydb")
                .build();

        final OracleJdbcConfiguration adapted = OracleJdbcConfiguration.adapt(plain);
        assertThat(adapted).isNotNull();
        assertThat(adapted.getHostname()).isEqualTo("primary");
        assertThat(adapted.getDatabase()).isEqualTo("primarydb");
        assertThat(adapted.getUrl()).isEqualTo("jdbc:oracle:thin:@primary:1521/primarydb");
    }

    @Test
    @FixFor("debezium/dbz#2257")
    public void shouldAdaptWithSubsetWithNoSecondaryConfiguration() {
        final Configuration config = Configuration.create()
                .with("database.hostname", "primary")
                .with("database.dbname", "primarydb")
                .with("database.port", "1521")
                .build();

        final OracleJdbcConfiguration jdbcConfig = OracleJdbcConfiguration.adaptWithSubset(config);
        assertThat(jdbcConfig.getHostname()).isEqualTo("primary");
        assertThat(jdbcConfig.getDatabase()).isEqualTo("primarydb");
        assertThat(jdbcConfig.getSecondaryHostname()).isNull();
        assertThat(jdbcConfig.getSecondaryDatabaseName()).isNull();
        assertThat(jdbcConfig.getSecondaryUrl()).isNull();
        assertThat(jdbcConfig.getSecondaryPort()).isEqualTo(0);
    }

    @Test
    @FixFor("debezium/dbz#2257")
    public void shouldAdaptWithSubsetMergingPrefixedConfigurations() {
        final Configuration config = Configuration.create()
                .with("database.hostname", "primary")
                .with("database.dbname", "primarydb")
                .with("database.port", "1521")
                .with("database.url", "jdbc:oracle:thin:@primary:1521/primarydb")
                .with("secondary.hostname", "secondary")
                .with("secondary.dbname", "secondarydb")
                .with("secondary.port", "1522")
                .build();

        final OracleJdbcConfiguration jdbcConfig = OracleJdbcConfiguration.adaptWithSubset(config);
        assertThat(jdbcConfig.getHostname()).isEqualTo("primary");
        assertThat(jdbcConfig.getDatabase()).isEqualTo("primarydb");
        assertThat(jdbcConfig.getPort()).isEqualTo(1521);
        assertThat(jdbcConfig.getUrl()).isEqualTo("jdbc:oracle:thin:@primary:1521/primarydb");
        assertThat(jdbcConfig.getSecondaryHostname()).isEqualTo("secondary");
        assertThat(jdbcConfig.getSecondaryDatabaseName()).isEqualTo("secondarydb");
        assertThat(jdbcConfig.getSecondaryPort()).isEqualTo(1522);
    }
}
