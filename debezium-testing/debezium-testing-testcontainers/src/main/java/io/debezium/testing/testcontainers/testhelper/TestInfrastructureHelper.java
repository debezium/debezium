/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers.testhelper;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;

import io.debezium.testing.testcontainers.Connector;
import io.debezium.testing.testcontainers.DebeziumContainer;
import io.debezium.testing.testcontainers.MongoDbReplicaSet;
import io.debezium.testing.testcontainers.OracleContainer;
import io.debezium.testing.testcontainers.util.MoreStartables;

public class TestInfrastructureHelper {

    public static final String KAFKA_HOSTNAME = "kafka-dbz";
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
            .withNetworkAliases(KAFKA_HOSTNAME)
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
            .imageName(DockerImageName.parse("mongo:5.0"))
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

    private static Supplier<Stream<Startable>> getContainers(DATABASE database) {
        final Startable dbStartable = switch (database) {
            case POSTGRES -> POSTGRES_CONTAINER;
            case MYSQL -> MYSQL_CONTAINER;
            case MONGODB -> MONGODB_REPLICA;
            case SQLSERVER -> SQL_SERVER_CONTAINER;
            case ORACLE -> ORACLE_CONTAINER;
            case MARIADB -> MARIADB_CONTAINER;
            default -> null;
        };

        if (null != dbStartable) {
            return () -> Stream.of(KAFKA_CONTAINER, dbStartable, DEBEZIUM_CONTAINER);
        }
        if (DATABASE.DEBEZIUM_ONLY.equals(database)) {
            return () -> Stream.of(DEBEZIUM_CONTAINER);
        }
        return () -> Stream.of(KAFKA_CONTAINER, DEBEZIUM_CONTAINER);
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
        final Supplier<Stream<Startable>> containers = getContainers(database);

        if ("true".equals(System.getenv("CI"))) {
            containers.get().forEach(container -> {
                if (container instanceof GenericContainer<?> && !(container instanceof OracleContainer)) {
                    ((GenericContainer<?>) container).withStartupTimeout(Duration.ofSeconds(CI_CONTAINER_STARTUP_TIME));
                }
            });
        }
        MoreStartables.deepStartSync(containers.get());
    }

    public static void copyIntoContainer(DATABASE database, String filePathToTransfer, String containerPath) {
        final Supplier<Stream<Startable>> containers = getContainers(database);

        if ("true".equals(System.getenv("CI"))) {
            containers.get().forEach(container -> {
                if (container instanceof GenericContainer<?>) {
                    if (((GenericContainer<?>) container).isRunning()) {
                        LOGGER.warn("Container {} is running. Copy into is not permitted", ((GenericContainer<?>) container).getContainerName());
                    }
                    ((GenericContainer<?>) container).withCopyToContainer(Transferable.of(readBytesFromResource(filePathToTransfer)), containerPath);
                }
            });
        }
    }

    public static void setupDebeziumContainer(String connectorVersion, String restExtensionClassses) {
        setupDebeziumContainer(connectorVersion, restExtensionClassses, DEBEZIUM_CONTAINER_IMAGE_VERSION_LATEST);
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
                .withKafka(KAFKA_CONTAINER.getNetwork(), KAFKA_HOSTNAME + ":9092")
                .withLogConsumer(new Slf4jLogConsumer(LOGGER))
                .enableJMX()
                .dependsOn(KAFKA_CONTAINER);
        if (null != restExtensionClasses && !restExtensionClasses.isEmpty()) {
            DEBEZIUM_CONTAINER.withEnv("CONNECT_REST_EXTENSION_CLASSES", restExtensionClasses);
        }
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
                .withKafka(KAFKA_CONTAINER.getNetwork(), KAFKA_HOSTNAME + ":9092")
                .withLogConsumer(new Slf4jLogConsumer(LOGGER))
                .enableJMX()
                .dependsOn(KAFKA_CONTAINER);
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

    public static void defaultDebeziumContainer() {
        defaultDebeziumContainer(null);
    }

    public static GenericContainer<?> getKafkaContainer() {
        return KAFKA_CONTAINER;
    }

    public static DebeziumContainer getDebeziumContainer() {
        return DEBEZIUM_CONTAINER;
    }

    public static PostgreSQLContainer<?> getPostgresContainer() {
        return POSTGRES_CONTAINER;
    }

    public static MySQLContainer<?> getMySqlContainer() {
        return MYSQL_CONTAINER;
    }

    public static MongoDbReplicaSet getMongoDbContainer() {
        return MONGODB_REPLICA;
    }

    public static MSSQLServerContainer<?> getSqlServerContainer() {
        return SQL_SERVER_CONTAINER;
    }

    public static OracleContainer getOracleContainer() {
        return ORACLE_CONTAINER;
    }

    public static MariaDBContainer getMariaDbContainer() {
        return MARIADB_CONTAINER;
    }

    public static void waitForConnectorTaskStatus(String connectorName, int taskNumber, Connector.State state) {
        Awaitility.await()
                // this needs to be set to at least a minimum of ~65-70 seconds because PostgreSQL now
                // retries on certain failure conditions with a 10s between them.
                .atMost(120, TimeUnit.SECONDS)
                .until(() -> TestInfrastructureHelper.getDebeziumContainer().getConnectorTaskState(connectorName, taskNumber) == state);
    }

    private static byte[] readBytesFromResource(String path) {
        try {
            return TestInfrastructureHelper.class.getResourceAsStream(path).readAllBytes();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
