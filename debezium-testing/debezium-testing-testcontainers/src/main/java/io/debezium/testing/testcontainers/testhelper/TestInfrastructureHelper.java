/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers.testhelper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.startupcheck.MinimumDurationRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;

import io.debezium.testing.testcontainers.DebeziumContainer;
import io.debezium.testing.testcontainers.MongoDbReplicaSet;
import io.debezium.testing.testcontainers.OracleContainer;
import io.debezium.testing.testcontainers.util.MoreStartables;

public class TestInfrastructureHelper {

    public static final String KAFKA_HOSTNAME = "kafka-dbz";
    public static final String LEGACY_KAFKA_HOSTNAME = "kafka";
    public static final String KAFKA_BOOTSTRAP_SERVERS = KAFKA_HOSTNAME + ":9092," + LEGACY_KAFKA_HOSTNAME + ":9092";
    public static final int CI_CONTAINER_STARTUP_TIME = 90;
    private static final String DEBEZIUM_CONTAINER_IMAGE_VERSION_LATEST = "latest";

    public enum DATABASE {
        POSTGRES,
        MYSQL,
        SQLSERVER,
        MONGODB,
        ORACLE,
        MARIADB,
        NONE,
        DEBEZIUM_ONLY
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(TestInfrastructureHelper.class);

    private static final Network NETWORK = Network.newNetwork();

    private static final Pattern VERSION_PATTERN = Pattern.compile("^[1-9]\\d*\\.\\d+");

    private static final GenericContainer<?> KAFKA_CONTAINER = new GenericContainer<>(
            DockerImageName.parse("quay.io/debezium/kafka:" + DEBEZIUM_CONTAINER_IMAGE_VERSION_LATEST).asCompatibleSubstituteFor("kafka"))
            .withNetworkAliases(KAFKA_HOSTNAME, LEGACY_KAFKA_HOSTNAME)
            .withNetwork(NETWORK)
            .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@" + KAFKA_HOSTNAME + ":9093")
            .withEnv("CLUSTER_ID", "5Yr1SIgYQz-b-dgRabWx4g")
            .withEnv("NODE_ID", "1");

    private static DebeziumContainer DEBEZIUM_CONTAINER = null;
    private static final PostgreSQLContainer<?> POSTGRES_CONTAINER = new PostgreSQLContainer<>(
            DockerImageName.parse("quay.io/debezium/example-postgres:" + DEBEZIUM_CONTAINER_IMAGE_VERSION_LATEST).asCompatibleSubstituteFor("postgres"))
            .withNetwork(NETWORK)
            .withNetworkAliases("postgres");

    private static final MySQLContainer<?> MYSQL_CONTAINER = new MySQLContainer<>(
            DockerImageName.parse("quay.io/debezium/example-mysql:" + DEBEZIUM_CONTAINER_IMAGE_VERSION_LATEST).asCompatibleSubstituteFor("mysql"))
            .withNetwork(NETWORK)
            .withUsername("mysqluser")
            .withPassword("mysqlpw")
            .withEnv("MYSQL_ROOT_PASSWORD", "debezium")
            .withNetworkAliases("mysql");

    private static final MariaDBContainer<?> MARIADB_CONTAINER = new MariaDBContainer<>(
            DockerImageName.parse("quay.io/debezium/example-mariadb:" + DEBEZIUM_CONTAINER_IMAGE_VERSION_LATEST).asCompatibleSubstituteFor("mariadb"))
            .withNetwork(NETWORK)
            .withUsername("mariadbuser")
            .withPassword("mariadbpw")
            .withEnv("MARIADB_ROOT_PASSWORD", "debezium")
            .withNetworkAliases("mariadb");

    private static final MongoDbReplicaSet MONGODB_REPLICA = MongoDbReplicaSet.replicaSet()
            .name("rs0")
            .memberCount(1)
            .network(NETWORK)
            .imageName(DockerImageName.parse("mirror.gcr.io/library/mongo:5.0"))
            .startupTimeout(Duration.ofSeconds(CI_CONTAINER_STARTUP_TIME))
            .build();

    private static final MSSQLServerContainer<?> SQL_SERVER_CONTAINER = new MSSQLServerContainer<>(DockerImageName.parse("mcr.microsoft.com/mssql/server:2019-latest"))
            .withNetwork(NETWORK)
            .withNetworkAliases("sqlserver")
            .withEnv("SA_PASSWORD", "Password!")
            .withEnv("MSSQL_PID", "Standard")
            .withEnv("MSSQL_AGENT_ENABLED", "true")
            .withPassword("Password!")
            .withInitScript("initialize-sqlserver-database.sql")
            .acceptLicense()
            .waitingFor(new LogMessageWaitStrategy()
                    .withRegEx(".*SQL Server is now ready for client connections\\..*\\s")
                    .withTimes(1)
                    .withStartupTimeout(Duration.of(CI_CONTAINER_STARTUP_TIME * 3, ChronoUnit.SECONDS)))
            .withStartupCheckStrategy(new MinimumDurationRunningStartupCheckStrategy(Duration.ofSeconds(10)))
            .withConnectTimeoutSeconds(300);

    private static final OracleContainer ORACLE_CONTAINER = (OracleContainer) new OracleContainer()
            .withNetwork(NETWORK)
            .withNetworkAliases("oracledb")
            .withLogConsumer(new Slf4jLogConsumer(LOGGER));

    public static Network getNetwork() {
        return NETWORK;
    }

    private static Startable getDatabaseStartable(DATABASE database) {
        return switch (database) {
            case POSTGRES -> POSTGRES_CONTAINER;
            case MYSQL -> MYSQL_CONTAINER;
            case MONGODB -> MONGODB_REPLICA;
            case SQLSERVER -> SQL_SERVER_CONTAINER;
            case ORACLE -> ORACLE_CONTAINER;
            case MARIADB -> MARIADB_CONTAINER;
            default -> null;
        };
    }

    public static String parseDebeziumVersion(String connectorVersion) {
        var matcher = VERSION_PATTERN.matcher(connectorVersion);
        if (matcher.find()) {
            return matcher.toMatchResult().group();
        }
        else {
            throw new RuntimeException("Cannot parse version: " + connectorVersion);
        }
    }

    public static void stopContainers() {
        Stream<Startable> containers = Stream.of(DEBEZIUM_CONTAINER, ORACLE_CONTAINER, SQL_SERVER_CONTAINER, MONGODB_REPLICA,
                MYSQL_CONTAINER, POSTGRES_CONTAINER,
                MARIADB_CONTAINER,
                KAFKA_CONTAINER);
        MoreStartables.deepStopSync(containers);
        DEBEZIUM_CONTAINER = null;
    }

    public static void startContainers(DATABASE database) {
        final Startable dbStartable = getDatabaseStartable(database);

        if ("true".equals(System.getenv("CI"))) {
            Stream.of(KAFKA_CONTAINER, DEBEZIUM_CONTAINER, dbStartable).forEach(container -> {
                if (container instanceof GenericContainer<?> && !(container instanceof OracleContainer)) {
                    ((GenericContainer<?>) container).withStartupTimeout(Duration.ofSeconds(CI_CONTAINER_STARTUP_TIME));
                }
            });
            KAFKA_CONTAINER.withStartupTimeout(Duration.ofSeconds(CI_CONTAINER_STARTUP_TIME));
        }

        // In CI, parallel startup may start Connect before Kafka is attached to the network,
        // resulting in intermittent DNS failures for bootstrap servers.
        if (!KAFKA_CONTAINER.isRunning()) {
            KAFKA_CONTAINER.start();
        }

        if (dbStartable != null) {
            MoreStartables.deepStartSync(Stream.of(dbStartable));
        }

        logContainersBeforeConnectStartup();

        MoreStartables.deepStartSync(Stream.of(DEBEZIUM_CONTAINER));
    }

    private static void logContainersBeforeConnectStartup() {
        final var dockerClient = DockerClientFactory.lazyClient();
        final var dockerContainers = dockerClient.listContainersCmd().withShowAll(true).exec();

        LOGGER.info("Container inventory before Kafka Connect startup: {} container(s)", dockerContainers.size());
        for (var container : dockerContainers) {
            final String containerId = container.getId();
            final String shortContainerId = containerId.length() > 12 ? containerId.substring(0, 12) : containerId;
            final String names = container.getNames() == null ? "" : Arrays.toString(container.getNames());
            LOGGER.info("Container id={}, image={}, state={}, status={}, names={}",
                    shortContainerId, container.getImage(), container.getState(), container.getStatus(), names);

            try {
                final Process process = new ProcessBuilder("docker", "logs", "--tail", "200", containerId)
                        .redirectErrorStream(true)
                        .start();
                boolean completed = process.waitFor(15, TimeUnit.SECONDS);
                if (!completed) {
                    process.destroyForcibly();
                    LOGGER.warn("Timed out while reading logs for container {}", shortContainerId);
                    continue;
                }
                final String logs;
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                    StringBuilder logsBuilder = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        logsBuilder.append(line).append(System.lineSeparator());
                    }
                    logs = logsBuilder.toString().trim();
                }
                if (logs.isBlank()) {
                    LOGGER.info("Container {} logs: <empty>", shortContainerId);
                }
                else {
                    LOGGER.info("Container {} logs:\n{}", shortContainerId, logs);
                }
            }
            catch (Exception e) {
                LOGGER.warn("Unable to read logs for container {}", shortContainerId, e);
            }
        }
    }

    private static void waitForDebeziumContainerIsStopped() {
        Awaitility.await()
                .atMost(DebeziumContainer.waitTimeForRecords() * 5L, TimeUnit.SECONDS)
                .until(() -> !TestInfrastructureHelper.getDebeziumContainer().isRunning());
    }

    public static void setupDebeziumContainer(String connectorVersion, String restExtensionClasses, String debeziumContainerImageVersion) {
        if (null != DEBEZIUM_CONTAINER && DEBEZIUM_CONTAINER.isRunning()) {
            DEBEZIUM_CONTAINER.stop();
            waitForDebeziumContainerIsStopped();
        }
        final String registry = debeziumContainerImageVersion.startsWith("1.2") ? "" : "quay.io/";
        final String debeziumVersion = debeziumContainerImageVersion.startsWith("1.2") ? "1.2.5.Final" : connectorVersion;
        String baseImageName = registry + "debezium/connect:nightly";
        DEBEZIUM_CONTAINER = new DebeziumContainer(new ImageFromDockerfile("quay.io/debezium/connect-rest-test:" + debeziumVersion)
                .withFileFromPath(".", Paths.get(System.getProperty("project.build.directory")))
                .withFileFromPath("Dockerfile", Paths.get(System.getProperty("project.basedir") + "/src/test/resources/Dockerfile.rest.test"))
                .withBuildArg("BASE_IMAGE", baseImageName)
                .withBuildArg("DEBEZIUM_VERSION", debeziumVersion))
                .withEnv("ENABLE_DEBEZIUM_SCRIPTING", "true")
                .withNetwork(NETWORK)
                .withKafka(KAFKA_CONTAINER.getNetwork(), KAFKA_BOOTSTRAP_SERVERS)
                .withLogConsumer(new Slf4jLogConsumer(LOGGER))
                .enableJMX()
                .dependsOn(KAFKA_CONTAINER);
        if (null != restExtensionClasses && !restExtensionClasses.isEmpty()) {
            DEBEZIUM_CONTAINER.withEnv("CONNECT_REST_EXTENSION_CLASSES", restExtensionClasses);
        }
    }

    public static void setupSqlServerTDEncryption() throws IOException, InterruptedException {
        SQL_SERVER_CONTAINER.execInContainer(
                "/opt/mssql-tools18/bin/sqlcmd",
                "-S", "localhost",
                "-U", "SA",
                "-P", "Password!",
                "-i", "/opt/mssql-tools18/bin/setup-sqlserver-database-with-encryption.sql",
                "-N", "-C");
    }

    public static void defaultDebeziumContainer(String debeziumContainerImageVersion) {
        if (null != DEBEZIUM_CONTAINER && DEBEZIUM_CONTAINER.isRunning()) {
            DEBEZIUM_CONTAINER.stop();
            waitForDebeziumContainerIsStopped();
        }
        if (null == debeziumContainerImageVersion) {
            debeziumContainerImageVersion = DEBEZIUM_CONTAINER_IMAGE_VERSION_LATEST;
        }
        else {
            debeziumContainerImageVersion = parseDebeziumVersion(debeziumContainerImageVersion);
        }
        final String registry = debeziumContainerImageVersion.startsWith("1.2") ? "" : "quay.io/";
        String imageName = registry + "debezium/connect:" + debeziumContainerImageVersion;
        DEBEZIUM_CONTAINER = new DebeziumContainer(DockerImageName.parse(imageName))
                .withEnv("ENABLE_DEBEZIUM_SCRIPTING", "true")
                .withNetwork(NETWORK)
                .withKafka(KAFKA_CONTAINER.getNetwork(), KAFKA_BOOTSTRAP_SERVERS)
                .withLogConsumer(new Slf4jLogConsumer(LOGGER))
                .enableJMX()
                .dependsOn(KAFKA_CONTAINER);
    }

    public static DebeziumContainer getDebeziumContainer() {
        return DEBEZIUM_CONTAINER;
    }

    public static OracleContainer getOracleContainer() {
        return ORACLE_CONTAINER;
    }

}
