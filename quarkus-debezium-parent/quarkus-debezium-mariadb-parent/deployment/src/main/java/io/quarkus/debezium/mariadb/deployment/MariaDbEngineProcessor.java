/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.mariadb.deployment;

import java.io.IOException;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.Optional;

import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;

import io.debezium.connector.binlog.charset.BinlogCharsetRegistry;
import io.debezium.connector.binlog.snapshot.lock.MinimalAtLeastOnceSnapshotLock;
import io.debezium.connector.mariadb.MariaDbConnector;
import io.debezium.connector.mariadb.MariaDbConnectorTask;
import io.debezium.connector.mariadb.MariaDbSourceInfoStructMaker;
import io.debezium.connector.mariadb.Module;
import io.debezium.connector.mariadb.charset.MariaDbCharsetRegistry;
import io.debezium.connector.mariadb.snapshot.lock.DefaultSnapshotLock;
import io.debezium.connector.mariadb.snapshot.lock.ExtendedSnapshotLock;
import io.debezium.connector.mariadb.snapshot.lock.MinimalSnapshotLock;
import io.debezium.connector.mariadb.snapshot.lock.NoneSnapshotLock;
import io.debezium.connector.mariadb.snapshot.query.SelectAllSnapshotQuery;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.runtime.configuration.DebeziumEngineConfiguration;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;
import io.quarkus.datasource.common.runtime.DataSourceUtil;
import io.quarkus.datasource.common.runtime.DatabaseKind;
import io.quarkus.datasource.deployment.spi.DevServicesDatasourceConfigurationHandlerBuildItem;
import io.quarkus.datasource.deployment.spi.DevServicesDatasourceContainerConfig;
import io.quarkus.datasource.deployment.spi.DevServicesDatasourceProvider;
import io.quarkus.datasource.deployment.spi.DevServicesDatasourceProviderBuildItem;
import io.quarkus.debezium.agroal.configuration.AgroalDatasourceConfiguration;
import io.quarkus.debezium.deployment.QuarkusEngineProcessor;
import io.quarkus.debezium.deployment.items.DebeziumConnectorBuildItem;
import io.quarkus.debezium.deployment.items.DebeziumExtensionNameBuildItem;
import io.quarkus.debezium.engine.MariaDbEngineProducer;
import io.quarkus.deployment.IsNormal;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.NativeImageEnableAllCharsetsBuildItem;
import io.quarkus.deployment.builditem.nativeimage.NativeImageResourceBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.dev.devservices.DevServicesConfig;
import io.quarkus.deployment.pkg.steps.NativeOrNativeSourcesBuild;
import io.quarkus.devservices.common.ContainerShutdownCloseable;
import io.quarkus.runtime.LaunchMode;

public class MariaDbEngineProcessor implements QuarkusEngineProcessor<AgroalDatasourceConfiguration> {

    private static final String MARIADB = Module.name();

    @BuildStep
    @Override
    public DebeziumExtensionNameBuildItem debeziumExtensionNameBuildItem() {
        return new DebeziumExtensionNameBuildItem(MARIADB);
    }

    @BuildStep
    @Override
    public DebeziumConnectorBuildItem engine() {
        return new DebeziumConnectorBuildItem(MARIADB, MariaDbEngineProducer.class);
    }

    @BuildStep(onlyIf = NativeOrNativeSourcesBuild.class)
    @Override
    public void registerClassesThatAreLoadedThroughReflection(BuildProducer<ReflectiveClassBuildItem> reflectiveClassBuildItemBuildProducer) {

        reflectiveClassBuildItemBuildProducer.produce(ReflectiveClassBuildItem.builder(
                "io.debezium.connector.mariadb.charset.MariaDbCharsetRegistry$CharacterSetMappings",
                "io.debezium.connector.mariadb.charset.MariaDbCharsetRegistry$CharacterSetMapping",
                "io.debezium.connector.mariadb.charset.MariaDbCharsetRegistry$CharacterSetMapping",
                "io.debezium.connector.mariadb.charset.MariaDbCharsetRegistry$CollationMapping")
                .build());

        reflectiveClassBuildItemBuildProducer.produce(ReflectiveClassBuildItem.builder(
                SchemaHistory.class,
                KafkaSchemaHistory.class,
                MariaDbConnector.class,
                MariaDbSourceInfoStructMaker.class,
                MariaDbConnectorTask.class,
                MinimalAtLeastOnceSnapshotLock.class,
                DefaultSnapshotLock.class,
                ExtendedSnapshotLock.class,
                MinimalSnapshotLock.class,
                NoneSnapshotLock.class,
                MariaDbCharsetRegistry.class,
                BinlogCharsetRegistry.class,
                SelectAllSnapshotQuery.class)
                .reason(getClass().getName())
                .build());
    }

    @BuildStep(onlyIf = NativeOrNativeSourcesBuild.class)
    void registerCharsets(BuildProducer<NativeImageResourceBuildItem> resources) {
        resources.produce(new NativeImageResourceBuildItem("charset_mappings.json"));
    }

    @Override
    public Class<AgroalDatasourceConfiguration> quarkusDatasourceConfiguration() {
        return AgroalDatasourceConfiguration.class;
    }

    @BuildStep(onlyIf = NativeOrNativeSourcesBuild.class)
    public NativeImageEnableAllCharsetsBuildItem enableAllCharsets() {
        return new NativeImageEnableAllCharsetsBuildItem();
    }

    @BuildStep
    DevServicesDatasourceConfigurationHandlerBuildItem devDbHandler() {
        return DevServicesDatasourceConfigurationHandlerBuildItem.jdbc(DatabaseKind.MARIADB);
    }

    @BuildStep(onlyIfNot = IsNormal.class, onlyIf = DevServicesConfig.Enabled.class)
    void devservices(BuildProducer<DevServicesDatasourceProviderBuildItem> devServicesProducer, DebeziumEngineConfiguration debeziumEngineConfiguration) {

        var mariadb = debeziumEngineConfiguration.devservices().get("mariadb");
        var allServices = debeziumEngineConfiguration.devservices().get("*");

        if (mariadb != null && !mariadb.enabled().orElse(true)) {
            return;
        }

        if (allServices != null && !allServices.enabled().orElse(true)) {
            return;
        }

        devServicesProducer.produce(new DevServicesDatasourceProviderBuildItem(DatabaseKind.MARIADB,
                new DevServicesDatasourceProvider() {

                    @Override
                    public RunningDevServicesDatasource startDatabase(Optional<String> username, Optional<String> password, String datasourceName,
                                                                      DevServicesDatasourceContainerConfig containerConfig, LaunchMode launchMode,
                                                                      Optional<Duration> startupTimeout) {

                        String effectiveUsername = containerConfig.getUsername().orElse(username.orElse(DebeziumMariaDbContainer.USER));
                        String effectivePassword = containerConfig.getPassword().orElse(password.orElse(DebeziumMariaDbContainer.PASSWORD));
                        String effectiveDbName = containerConfig.getDbName().orElse(
                                DataSourceUtil.isDefault(datasourceName) ? DebeziumMariaDbContainer.DATABASE : datasourceName);

                        DebeziumMariaDbContainer container = new DebeziumMariaDbContainer(effectiveUsername, effectivePassword, effectiveDbName);
                        container.start();

                        return new RunningDevServicesDatasource(container.getContainerId(),
                                container.getConnectionInfo(),
                                container.getConnectionInfo(),
                                effectiveUsername,
                                effectivePassword,
                                new ContainerShutdownCloseable(container, DebeziumMariaDbContainer.SERVICE_NAME));
                    }
                }));
    }

    private static class DebeziumMariaDbContainer<SELF extends MariaDBContainer<SELF>> extends MariaDBContainer<SELF> {

        public static final String USER = "debezium";
        public static final String PASSWORD = "dbz";
        public static final String IMAGE = "mirror.gcr.io/library/mariadb:11.4.3";
        public static final String LOCALHOST = "127.0.0.1";
        public static final String DATABASE = "debezium";
        public static final String CONNECTION_STRING = "jdbc:mariadb://" + LOCALHOST + ":3306/debezium";
        public static final String SERVICE_NAME = "debezium-devservices-mariadb";

        private final MariaDBContainer<SELF> container;
        private final String username;
        private final String database;

        private DebeziumMariaDbContainer(String user, String password, String database) {
            this.username = user;
            this.database = database;
            this.container = new MariaDBContainer<SELF>(
                    DockerImageName.parse(IMAGE).asCompatibleSubstituteFor("mariadb"))
                    .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
                            .withPortBindings(new PortBinding(Ports.Binding.bindPort(3306), new ExposedPort(3306))))
                    .withUsername(user)
                    .withPassword(password)
                    .withCopyToContainer(Transferable.of("""
                            [mysqld]
                            skip-host-cache
                            skip-name-resolve
                            user=mysql
                            symbolic-links=0
                            server-id         = 223344
                            log_bin           = mysql-bin
                            binlog_expire_logs_seconds  = 86400
                            binlog_format     = row
                            default_authentication_plugin = mysql_native_password
                            """), "/etc/mysql/conf.d/mariadb.cnf")
                    .withEnv("MARIADB_ROOT_PASSWORD", password);
        }

        @Override
        public void start() {
            container.start();

            try {
                container.execInContainer("mariadb",
                        "-uroot",
                        "-p" + container.getPassword(),
                        "-e",
                        MessageFormat.format("""
                                CREATE DATABASE {0};
                                GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT  ON *.* TO ''{1}'';
                                GRANT ALL PRIVILEGES ON {0}.* TO ''{1}''@''%'';
                                """, database, username));
            }
            catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        public String getConnectionInfo() {
            return container.getJdbcUrl();
        }

        @Override
        public String getContainerId() {
            return container.getContainerId();
        }

        @Override
        public void close() {
            container.stop();
        }

    }

}
