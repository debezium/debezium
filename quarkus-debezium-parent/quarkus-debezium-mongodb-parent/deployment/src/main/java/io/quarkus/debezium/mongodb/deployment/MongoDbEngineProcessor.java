/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.mongodb.deployment;

import jakarta.inject.Singleton;

import io.debezium.connector.mongodb.Module;
import io.debezium.connector.mongodb.MongoDbConnector;
import io.debezium.connector.mongodb.MongoDbConnectorTask;
import io.debezium.connector.mongodb.MongoDbSourceInfoStructMaker;
import io.debezium.connector.mongodb.connection.DefaultMongoDbAuthProvider;
import io.debezium.connector.mongodb.snapshot.query.SelectAllSnapshotQuery;
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
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.dev.devservices.DevServicesConfig;
import io.quarkus.deployment.pkg.steps.NativeOrNativeSourcesBuild;

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
    void configure(BuildProducer<DevServicesResultBuildItem> devServicesProducer) {
        // TODO: create a mongodb dev service with all the CDC capabilities
    }

    @BuildStep(onlyIf = NativeOrNativeSourcesBuild.class)
    void registerClassesThatAreLoadedThroughReflection(BuildProducer<ReflectiveClassBuildItem> reflectiveClasses) {
        reflectiveClasses.produce(ReflectiveClassBuildItem.builder(
                MongoDbConnector.class,
                MongoDbConnectorTask.class,
                MongoDbSourceInfoStructMaker.class,
                SelectAllSnapshotQuery.class,
                DefaultMongoDbAuthProvider.class)
                .reason(getClass().getName())
                .build());
    }
}
