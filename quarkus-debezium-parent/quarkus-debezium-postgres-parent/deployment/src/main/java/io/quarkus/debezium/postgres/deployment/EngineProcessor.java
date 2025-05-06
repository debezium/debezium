/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.postgres.deployment;

import static io.quarkus.debezium.postgres.deployment.ClassesInConfigurationHandler.PREDICATE;
import static io.quarkus.debezium.postgres.deployment.ClassesInConfigurationHandler.TRANSFORM;

import org.apache.kafka.common.security.authenticator.SaslClientAuthenticator;
import org.apache.kafka.connect.json.JsonConverter;

import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.connector.postgresql.PostgresConnectorTask;
import io.debezium.connector.postgresql.PostgresSourceInfoStructMaker;
import io.debezium.connector.postgresql.snapshot.lock.NoSnapshotLock;
import io.debezium.connector.postgresql.snapshot.lock.SharedSnapshotLock;
import io.debezium.connector.postgresql.snapshot.query.SelectAllSnapshotQuery;
import io.debezium.embedded.async.ConvertingAsyncEngineBuilderFactory;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.spi.OffsetCommitPolicy;
import io.debezium.pipeline.notification.channels.LogNotificationChannel;
import io.debezium.pipeline.notification.channels.SinkNotificationChannel;
import io.debezium.pipeline.notification.channels.jmx.JmxNotificationChannel;
import io.debezium.pipeline.signal.actions.StandardActionProvider;
import io.debezium.pipeline.signal.channels.FileSignalChannel;
import io.debezium.pipeline.signal.channels.KafkaSignalChannel;
import io.debezium.pipeline.signal.channels.SourceSignalChannel;
import io.debezium.pipeline.signal.channels.jmx.JmxSignalChannel;
import io.debezium.pipeline.signal.channels.process.InProcessSignalChannel;
import io.debezium.pipeline.txmetadata.DefaultTransactionMetadataFactory;
import io.debezium.schema.SchemaTopicNamingStrategy;
import io.debezium.snapshot.lock.NoLockingSupport;
import io.debezium.snapshot.mode.AlwaysSnapshotter;
import io.debezium.snapshot.mode.ConfigurationBasedSnapshotter;
import io.debezium.snapshot.mode.InitialOnlySnapshotter;
import io.debezium.snapshot.mode.InitialSnapshotter;
import io.debezium.snapshot.mode.NeverSnapshotter;
import io.debezium.snapshot.mode.NoDataSnapshotter;
import io.debezium.snapshot.mode.RecoverySnapshotter;
import io.debezium.snapshot.mode.SchemaOnlyRecoverySnapshotter;
import io.debezium.snapshot.mode.SchemaOnlySnapshotter;
import io.debezium.snapshot.mode.WhenNeededSnapshotter;
import io.debezium.snapshot.spi.SnapshotLock;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.arc.processor.DotNames;
import io.quarkus.datasource.deployment.spi.DevServicesDatasourceResultBuildItem;
import io.quarkus.debezium.configuration.DebeziumEngineConfiguration;
import io.quarkus.debezium.producer.PostgresEngineProducer;
import io.quarkus.debezium.recorder.DebeziumRecorder;
import io.quarkus.deployment.IsNormal;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.ApplicationStartBuildItem;
import io.quarkus.deployment.builditem.DevServicesResultBuildItem;
import io.quarkus.deployment.builditem.ExecutorBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.ShutdownContextBuildItem;
import io.quarkus.deployment.builditem.nativeimage.NativeImageConfigBuildItem;
import io.quarkus.deployment.builditem.nativeimage.NativeImageResourceBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.dev.devservices.DevServicesConfig;
import io.quarkus.deployment.pkg.steps.NativeOrNativeSourcesBuild;

public class EngineProcessor {

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem("debezium-postgres");
    }

    @BuildStep
    void engine(BuildProducer<AdditionalBeanBuildItem> additionalBeanProducer) {
        additionalBeanProducer.produce(AdditionalBeanBuildItem
                .builder()
                .addBeanClasses(PostgresEngineProducer.class)
                .setUnremovable()
                .setDefaultScope(DotNames.APPLICATION_SCOPED)
                .build());
    }

    @BuildStep(onlyIfNot = IsNormal.class, onlyIf = DevServicesConfig.Enabled.class)
    void configure(BuildProducer<DevServicesResultBuildItem> devServicesProducer,
                   DevServicesDatasourceResultBuildItem devServicesDatasourceResultBuildItem) {
        DevServicesDatasourceResultBuildItem.DbResult datasource = devServicesDatasourceResultBuildItem.getDefaultDatasource();

        if (datasource == null) {
            return;
        }

        if (!datasource.getDbType().equals("postgresql")) {
            return;
        }

        devServicesProducer.produce(new DevServicesResultBuildItem("debezium-postgres",
                "debezium",
                QuarkusDatasource.generateDebeziumConfiguration(datasource.getConfigProperties())));
    }

    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    void startEngine(ApplicationStartBuildItem ignore,
                     DebeziumRecorder recorder,
                     ExecutorBuildItem executorBuildItem,
                     ShutdownContextBuildItem shutdownContextBuildItem) {

        recorder.startEngine(executorBuildItem.getExecutorProxy(), shutdownContextBuildItem);
    }

    @BuildStep(onlyIf = NativeOrNativeSourcesBuild.class)
    void registerClassesThatAreLoadedThroughReflection(BuildProducer<ReflectiveClassBuildItem> reflectiveClasses,
                                                       DebeziumEngineConfiguration debeziumEngineConfiguration) {

        TRANSFORM.extract(debeziumEngineConfiguration.configuration())
                .forEach(transform -> reflectiveClasses.produce(ReflectiveClassBuildItem
                        .builder(transform)
                        .reason(getClass().getName())
                        .build()));

        PREDICATE.extract(debeziumEngineConfiguration.configuration())
                .forEach(predicate -> reflectiveClasses.produce(ReflectiveClassBuildItem
                        .builder(predicate)
                        .reason(getClass().getName())
                        .build()));

        reflectiveClasses.produce(ReflectiveClassBuildItem.builder(
                DebeziumEngine.BuilderFactory.class,
                ConvertingAsyncEngineBuilderFactory.class,
                SaslClientAuthenticator.class,
                JsonConverter.class,
                PostgresConnector.class,
                PostgresSourceInfoStructMaker.class,
                DefaultTransactionMetadataFactory.class,
                SchemaTopicNamingStrategy.class,
                OffsetCommitPolicy.class,
                PostgresConnectorTask.class,
                SinkNotificationChannel.class,
                LogNotificationChannel.class,
                JmxNotificationChannel.class,
                SnapshotLock.class,
                NoLockingSupport.class,
                NoSnapshotLock.class,
                SharedSnapshotLock.class,
                SelectAllSnapshotQuery.class,
                AlwaysSnapshotter.class,
                InitialSnapshotter.class,
                InitialOnlySnapshotter.class,
                NoDataSnapshotter.class,
                RecoverySnapshotter.class,
                WhenNeededSnapshotter.class,
                NeverSnapshotter.class,
                SchemaOnlySnapshotter.class,
                SchemaOnlyRecoverySnapshotter.class,
                ConfigurationBasedSnapshotter.class,
                SourceSignalChannel.class,
                KafkaSignalChannel.class,
                FileSignalChannel.class,
                JmxSignalChannel.class,
                InProcessSignalChannel.class,
                StandardActionProvider.class,
                OffsetCommitPolicy.class,
                OffsetCommitPolicy.PeriodicCommitOffsetPolicy.class).reason(getClass().getName())
                .build());
    }

    @BuildStep
    NativeImageConfigBuildItem nativeImageConfiguration() {
        return NativeImageConfigBuildItem.builder()
                .addRuntimeInitializedClass("org.apache.kafka.common.security.authenticator.SaslClientAuthenticator")
                .build();
    }

    @BuildStep
    void registerNativeImageResources(BuildProducer<NativeImageResourceBuildItem> resources) {
        resources.produce(new NativeImageResourceBuildItem("META-INF/services/*"));
    }
}
