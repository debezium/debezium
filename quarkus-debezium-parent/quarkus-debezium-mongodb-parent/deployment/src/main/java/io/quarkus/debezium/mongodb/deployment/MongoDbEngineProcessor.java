/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.mongodb.deployment;

import jakarta.inject.Singleton;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;

import io.debezium.connector.mongodb.Module;
import io.debezium.connector.mongodb.MongoDbConnector;
import io.debezium.connector.mongodb.MongoDbConnectorTask;
import io.debezium.connector.mongodb.MongoDbSourceInfoStructMaker;
import io.debezium.connector.mongodb.connection.DefaultMongoDbAuthProvider;
import io.debezium.connector.mongodb.snapshot.query.SelectAllSnapshotQuery;
import io.debezium.runtime.configuration.DebeziumEngineConfiguration;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.quarkus.arc.deployment.SyntheticBeanBuildItem;
import io.quarkus.debezium.configuration.MongoDbDatasourceConfiguration;
import io.quarkus.debezium.configuration.MongoDbDatasourceRecorder;
import io.quarkus.debezium.deployment.items.DebeziumConnectorBuildItem;
import io.quarkus.debezium.engine.MongoDbEngineProducer;
import io.quarkus.deployment.IsNormal;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.DevServicesResultBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.Startable;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.dev.devservices.DevServicesConfig;
import io.quarkus.deployment.pkg.steps.NativeOrNativeSourcesBuild;

import java.util.Map;

public class MongoDbEngineProcessor {
    public static final String MONGODB = Module.name();

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem("debezium-" + MONGODB);
    }

    @BuildStep
    public DebeziumConnectorBuildItem engine() {
        return new DebeziumConnectorBuildItem(MONGODB, MongoDbEngineProducer.class);
    }

    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    public void generateDatasourceConfig(
                                         MongoDbDatasourceRecorder mongoDbDatasourceRecorder,
                                         BuildProducer<SyntheticBeanBuildItem> producer) {
        producer.produce(SyntheticBeanBuildItem
                .configure(MongoDbDatasourceConfiguration.class)
                .scope(Singleton.class)
                .supplier(mongoDbDatasourceRecorder.convert("default", false))
                .setRuntimeInit()
                .named(MONGODB + "an_item")
                .done());
    }

    @BuildStep(onlyIfNot = IsNormal.class, onlyIf = DevServicesConfig.Enabled.class)
    void configure(BuildProducer<DevServicesResultBuildItem> devServicesProducer, DebeziumEngineConfiguration debeziumEngineConfiguration) {

        var mongoDb = debeziumEngineConfiguration.devservices().get("mongodb");
        var allServices = debeziumEngineConfiguration.devservices().get("*");

        if (mongoDb != null && !mongoDb.enabled().orElse(true)) {
            return;
        }

        if (allServices != null && !allServices.enabled().orElse(true)) {
            return;
        }

        devServicesProducer.produce(DevServicesResultBuildItem
                .owned()
                .name(DebeziumMongoDBContainer.SERVICE_NAME)
                .config(Map.of("quarkus.mongodb.connection-string", DebeziumMongoDBContainer.CONNECTION_STRING))
                .startable(DebeziumMongoDBContainer::new)
                .build());
    }

    @BuildStep(onlyIf = NativeOrNativeSourcesBuild.class)
    void registerClassesThatAreLoadedThroughReflection(BuildProducer<ReflectiveClassBuildItem> reflectiveClasses) {
        reflectiveClasses.produce(ReflectiveClassBuildItem.builder(
                DefaultTopicNamingStrategy.class,
                MongoDbConnector.class,
                MongoDbConnectorTask.class,
                MongoDbSourceInfoStructMaker.class,
                SelectAllSnapshotQuery.class,
                DefaultMongoDbAuthProvider.class)
                .reason(getClass().getName())
                .build());
    }

    private static class DebeziumMongoDBContainer implements Startable {

        public static final String USER = "debezium";
        public static final String PASSWORD = "dbz";
        public static final String DATABASE = "debezium";
        public static final String IMAGE = "debezium-mongo:local4";
        public static final String LOCALHOST = "127.0.0.1";
        public static final String CONNECTION_STRING = "mongodb://" + USER + ":" + PASSWORD + "@" + LOCALHOST + ":27017/?replicaSet=rs0";
        public static final String SERVICE_NAME = "debezium-devservices-mongodb";

        public static final GenericContainer container = new GenericContainer<>(IMAGE)
                .withExposedPorts(27017)
                .withNetwork(Network.SHARED)
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
                        .withPortBindings(new PortBinding(Ports.Binding.bindPort(27017), new ExposedPort(27017))))
                .withEnv(Map.of(
                        "HOSTNAME", LOCALHOST,
                        "MONGO_INITDB_ROOT_USERNAME", USER,
                        "MONGO_INITDB_ROOT_PASSWORD", PASSWORD,
                        "MONGO_INITDB_DATABASE", DATABASE));

        @Override
        public void start() {
            container.start();
        }

        @Override
        public String getConnectionInfo() {
            return CONNECTION_STRING;
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
