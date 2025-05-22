/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment.engine;

import static io.quarkus.debezium.deployment.dotnames.DebeziumDotNames.CapturingDotName.CAPTURING;
import static io.quarkus.debezium.deployment.engine.ClassesInConfigurationHandler.PREDICATE;
import static io.quarkus.debezium.deployment.engine.ClassesInConfigurationHandler.TRANSFORM;
import static io.quarkus.deployment.annotations.ExecutionTime.RUNTIME_INIT;

import java.util.Collection;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.common.security.authenticator.SaslClientAuthenticator;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.transforms.predicates.TopicNameMatches;

import io.debezium.connector.common.BaseSourceTask;
import io.debezium.embedded.async.ConvertingAsyncEngineBuilderFactory;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
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
import io.debezium.runtime.configuration.DebeziumEngineConfiguration;
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
import io.debezium.transforms.ExtractNewRecordState;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.arc.deployment.BeanContainerBuildItem;
import io.quarkus.arc.deployment.BeanDiscoveryFinishedBuildItem;
import io.quarkus.arc.deployment.SyntheticBeanBuildItem;
import io.quarkus.arc.deployment.UnremovableBeanBuildItem;
import io.quarkus.arc.deployment.UnremovableBeanBuildItem.BeanClassAnnotationExclusion;
import io.quarkus.arc.processor.DotNames;
import io.quarkus.debezium.deployment.dotnames.DebeziumDotNames;
import io.quarkus.debezium.deployment.items.DebeziumConnectorBuildItem;
import io.quarkus.debezium.deployment.items.DebeziumGeneratedInvokerBuildItem;
import io.quarkus.debezium.deployment.items.DebeziumMediatorBuildItem;
import io.quarkus.debezium.engine.DebeziumRecorder;
import io.quarkus.debezium.engine.capture.CapturingInvoker;
import io.quarkus.debezium.engine.capture.CapturingSourceRecordInvokerRegistryProducer;
import io.quarkus.debezium.engine.capture.DynamicCapturingInvokerSupplier;
import io.quarkus.deployment.GeneratedClassGizmoAdaptor;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.ExecutorBuildItem;
import io.quarkus.deployment.builditem.GeneratedClassBuildItem;
import io.quarkus.deployment.builditem.ShutdownContextBuildItem;
import io.quarkus.deployment.builditem.nativeimage.NativeImageConfigBuildItem;
import io.quarkus.deployment.builditem.nativeimage.NativeImageResourceBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.pkg.steps.NativeOrNativeSourcesBuild;
import io.quarkus.deployment.recording.RecorderContext;

public class EngineProcessor {

    @BuildStep
    void engine(BuildProducer<AdditionalBeanBuildItem> additionalBeanProducer,
                List<DebeziumConnectorBuildItem> debeziumConnectorBuildItems) {
        debeziumConnectorBuildItems
                .forEach(item -> additionalBeanProducer
                        .produce(AdditionalBeanBuildItem
                                .builder()
                                .addBeanClasses(item.producer())
                                .setUnremovable()
                                .setDefaultScope(DotNames.APPLICATION_SCOPED)
                                .build()));

        additionalBeanProducer.produce(AdditionalBeanBuildItem
                .builder()
                .addBeanClasses(CapturingSourceRecordInvokerRegistryProducer.class)
                .setDefaultScope(DotNames.APPLICATION_SCOPED)
                .setUnremovable()
                .build());
    }

    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    void startEngine(BeanContainerBuildItem beanContainerBuildItem,
                     DebeziumRecorder recorder,
                     ExecutorBuildItem executorBuildItem,
                     ShutdownContextBuildItem shutdownContextBuildItem) {

        recorder.startEngine(executorBuildItem.getExecutorProxy(), shutdownContextBuildItem, beanContainerBuildItem.getValue());
    }

    @BuildStep(onlyIf = NativeOrNativeSourcesBuild.class)
    NativeImageConfigBuildItem nativeImageConfiguration() {
        return NativeImageConfigBuildItem.builder()
                .addRuntimeInitializedClass("org.apache.kafka.common.security.authenticator.SaslClientAuthenticator")
                .build();
    }

    @BuildStep(onlyIf = NativeOrNativeSourcesBuild.class)
    void registerNativeImageResources(BuildProducer<NativeImageResourceBuildItem> resources) {
        resources.produce(new NativeImageResourceBuildItem("META-INF/services/io.debezium.embedded.async.ConvertingAsyncEngineBuilderFactory"));
        resources.produce(new NativeImageResourceBuildItem("META-INF/services/io.debezium.engine.DebeziumEngine$BuilderFactory"));
        resources.produce(new NativeImageResourceBuildItem("META-INF/services/io.debezium.snapshot.spi.SnapshotLock"));
        resources.produce(new NativeImageResourceBuildItem("META-INF/services/io.debezium.snapshot.spi.SnapshotQuery"));
        resources.produce(new NativeImageResourceBuildItem("META-INF/services/io.debezium.spi.snapshot.Snapshotter"));
        resources.produce(new NativeImageResourceBuildItem("META-INF/services/io.debezium.spi.snapshot.Snapshotter"));
        resources.produce(new NativeImageResourceBuildItem("META-INF/services/org.apache.kafka.connect.source.SourceConnector"));
        resources.produce(new NativeImageResourceBuildItem("META-INF/services/io.debezium.pipeline.signal.channels.SignalChannelReader"));
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
                DefaultTransactionMetadataFactory.class,
                SchemaTopicNamingStrategy.class,
                OffsetCommitPolicy.class,
                BaseSourceTask.class,
                SinkNotificationChannel.class,
                LogNotificationChannel.class,
                JmxNotificationChannel.class,
                SnapshotLock.class,
                NoLockingSupport.class,
                AlwaysSnapshotter.class,
                InitialSnapshotter.class,
                InitialOnlySnapshotter.class,
                NoDataSnapshotter.class,
                RecoverySnapshotter.class,
                WhenNeededSnapshotter.class,
                NeverSnapshotter.class,
                ExtractNewRecordState.class,
                TopicNameMatches.class,
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
                SourceTask.class,
                OffsetCommitPolicy.PeriodicCommitOffsetPolicy.class)
                .reason(getClass().getName())
                .build());
    }

    @BuildStep
    public void generateInvokers(List<DebeziumMediatorBuildItem> mediatorBuildItems,
                                 BuildProducer<GeneratedClassBuildItem> generatedClassBuildItemBuildProducer,
                                 BuildProducer<ReflectiveClassBuildItem> reflectiveClassBuildItemBuildProducer,
                                 BuildProducer<DebeziumGeneratedInvokerBuildItem> debeziumGeneratedInvokerBuildItemBuildProducer) {
        InvokerGenerator invokerGenerator = new InvokerGenerator(new GeneratedClassGizmoAdaptor(generatedClassBuildItemBuildProducer,
                true));

        mediatorBuildItems.forEach(item -> {
            InvokerMetaData metadata = invokerGenerator.generate(item.getMethodInfo(),
                    item.getBean());
            debeziumGeneratedInvokerBuildItemBuildProducer.produce(new DebeziumGeneratedInvokerBuildItem(metadata.invokerClassName(),
                    metadata.mediator(), metadata.getShortIdentifier()));
            reflectiveClassBuildItemBuildProducer.produce(ReflectiveClassBuildItem.builder(metadata.invokerClassName()).build());
        });
    }

    @BuildStep
    @Record(RUNTIME_INIT)
    public void injectInvokers(
                               DynamicCapturingInvokerSupplier dynamicCapturingInvokerSupplier,
                               RecorderContext recorderContext,
                               List<DebeziumGeneratedInvokerBuildItem> debeziumGeneratedInvokerBuildItems,
                               BuildProducer<SyntheticBeanBuildItem> syntheticBeanBuildItemBuildProducer) {
        debeziumGeneratedInvokerBuildItems.forEach(item -> syntheticBeanBuildItemBuildProducer.produce(
                SyntheticBeanBuildItem.configure(CapturingInvoker.class)
                        .setRuntimeInit()
                        .scope(ApplicationScoped.class)
                        .unremovable()
                        .supplier(dynamicCapturingInvokerSupplier.createInvoker(
                                recorderContext.classProxy(item.getMediator().getImplClazz().name().toString()),
                                (Class<? extends CapturingInvoker<RecordChangeEvent<SourceRecord>>>) recorderContext.classProxy(item.getGeneratedClassName())))
                        .named(DynamicCapturingInvokerSupplier.BASE_NAME + item.getMediator().getImplClazz().name() + item.getId())
                        .done()));
    }

    @BuildStep
    public void extractMediators(BuildProducer<DebeziumMediatorBuildItem> mediatorBuildItemBuildProducer,
                                 BeanDiscoveryFinishedBuildItem beanDiscoveryFinished) {
        beanDiscoveryFinished
                .beanStream()
                .classBeans()
                .stream()
                .filter(DebeziumDotNames.CapturingDotName::filter)
                .flatMap(beanInfo -> beanInfo
                        .getTarget()
                        .map(target -> target.asClass().methods())
                        .stream()
                        .flatMap(Collection::stream)
                        .filter(DebeziumDotNames.CapturingDotName::filter)
                        .map(methodInfo -> new DebeziumMediatorBuildItem(beanInfo, methodInfo)))
                .forEach(mediatorBuildItemBuildProducer::produce);
    }

    @BuildStep
    public List<UnremovableBeanBuildItem> removalExclusion() {
        return List.of(new UnremovableBeanBuildItem(new BeanClassAnnotationExclusion(CAPTURING)));
    }
}
