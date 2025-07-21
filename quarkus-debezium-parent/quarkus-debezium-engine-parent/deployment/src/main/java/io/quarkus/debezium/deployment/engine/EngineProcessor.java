/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment.engine;

import static io.quarkus.debezium.deployment.engine.ClassesInConfigurationHandler.POST_PROCESSOR;
import static io.quarkus.debezium.deployment.engine.ClassesInConfigurationHandler.PREDICATE;
import static io.quarkus.debezium.deployment.engine.ClassesInConfigurationHandler.TRANSFORM;
import static io.quarkus.deployment.annotations.ExecutionTime.RUNTIME_INIT;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.common.security.authenticator.SaslClientAuthenticator;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.transforms.predicates.TopicNameMatches;
import org.jboss.jandex.AnnotationValue;
import org.jboss.jandex.DotName;
import org.jboss.jandex.Type;

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
import io.debezium.processors.spi.PostProcessor;
import io.debezium.runtime.FieldFilterStrategy;
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
import io.debezium.snapshot.mode.WhenNeededSnapshotter;
import io.debezium.snapshot.spi.SnapshotLock;
import io.debezium.transforms.ExtractNewRecordState;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.arc.deployment.BeanContainerBuildItem;
import io.quarkus.arc.deployment.BeanDiscoveryFinishedBuildItem;
import io.quarkus.arc.deployment.SyntheticBeanBuildItem;
import io.quarkus.arc.deployment.UnremovableBeanBuildItem;
import io.quarkus.arc.deployment.UnremovableBeanBuildItem.BeanClassAnnotationExclusion;
import io.quarkus.arc.processor.BeanInfo;
import io.quarkus.arc.processor.DotNames;
import io.quarkus.debezium.deployment.dotnames.DebeziumDotNames;
import io.quarkus.debezium.deployment.items.DebeziumConnectorBuildItem;
import io.quarkus.debezium.deployment.items.DebeziumGeneratedCustomConverterBuildItem;
import io.quarkus.debezium.deployment.items.DebeziumGeneratedInvokerBuildItem;
import io.quarkus.debezium.deployment.items.DebeziumGeneratedPostProcessorBuildItem;
import io.quarkus.debezium.deployment.items.DebeziumMediatorBuildItem;
import io.quarkus.debezium.engine.DebeziumRecorder;
import io.quarkus.debezium.engine.DefaultStateHandler;
import io.quarkus.debezium.engine.capture.*;
import io.quarkus.debezium.engine.converter.custom.DynamicCustomConverterSupplier;
import io.quarkus.debezium.engine.post.processing.ArcPostProcessorFactory;
import io.quarkus.debezium.engine.post.processing.DynamicPostProcessingSupplier;
import io.quarkus.debezium.engine.relational.converter.QuarkusCustomConverter;
import io.quarkus.debezium.heartbeat.ArcHeartbeatFactory;
import io.quarkus.debezium.heartbeat.QuarkusHeartbeatEmitter;
import io.quarkus.debezium.notification.DefaultNotificationHandler;
import io.quarkus.debezium.notification.QuarkusNotificationChannel;
import io.quarkus.debezium.notification.SnapshotHandler;
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
                .addBeanClasses(
                        CapturingSourceRecordInvokerRegistryProducer.class,
                        CapturingEventInvokerRegistryProducer.class)
                .setDefaultScope(DotNames.APPLICATION_SCOPED)
                .setUnremovable()
                .build());

        // Beans that will be injected by child modules
        additionalBeanProducer.produce(AdditionalBeanBuildItem
                .builder()
                .addBeanClasses(DefaultStateHandler.class)
                .build());

        additionalBeanProducer.produce(AdditionalBeanBuildItem.builder()
                .addBeanClasses(
                        DefaultNotificationHandler.class,
                        SnapshotHandler.class,
                        QuarkusNotificationChannel.class,
                        QuarkusHeartbeatEmitter.class)
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
        resources.produce(new NativeImageResourceBuildItem("META-INF/services/io.debezium.pipeline.notification.channels.NotificationChannel"));
        resources.produce(new NativeImageResourceBuildItem("META-INF/services/io.debezium.processors.PostProcessorProducer"));
        resources.produce(new NativeImageResourceBuildItem("META-INF/services/io.debezium.heartbeat.DebeziumHeartbeatFactory"));
    }

    @BuildStep(onlyIf = NativeOrNativeSourcesBuild.class)
    void registerClassesThatAreLoadedThroughReflection(
                                                       BuildProducer<ReflectiveClassBuildItem> reflectiveClasses,
                                                       DebeziumEngineConfiguration debeziumEngineConfiguration) {

        List<String> classesAnnotatedWithInjectService = DebeziumDotNames.ANNOTATED_WITH_INJECT_SERVICE
                .stream()
                .map(DotName::toString)
                .toList();

        reflectiveClasses.produce(ReflectiveClassBuildItem
                .builder(classesAnnotatedWithInjectService.toArray(new String[0]))
                .reason(DebeziumDotNames.DEBEZIUM_ENGINE_PROCESSOR.toString())
                .constructors(false).methods().build());

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

        POST_PROCESSOR.extract(debeziumEngineConfiguration.configuration())
                .forEach(postProcessor -> reflectiveClasses.produce(ReflectiveClassBuildItem
                        .builder(postProcessor)
                        .reason(getClass().getName())
                        .build()));

        reflectiveClasses.produce(ReflectiveClassBuildItem.builder(
                ArcHeartbeatFactory.class,
                ArcPostProcessorFactory.class,
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
                ConfigurationBasedSnapshotter.class,
                SourceSignalChannel.class,
                KafkaSignalChannel.class,
                FileSignalChannel.class,
                JmxSignalChannel.class,
                InProcessSignalChannel.class,
                StandardActionProvider.class,
                OffsetCommitPolicy.class,
                SourceTask.class,
                QuarkusNotificationChannel.class,
                OffsetCommitPolicy.PeriodicCommitOffsetPolicy.class)
                .reason(getClass().getName())
                .build());
    }

    @BuildStep
    public void generateInvokers(List<DebeziumMediatorBuildItem> mediatorBuildItems,
                                 BuildProducer<GeneratedClassBuildItem> generatedClassBuildItemBuildProducer,
                                 BuildProducer<ReflectiveClassBuildItem> reflectiveClassBuildItemBuildProducer,
                                 BuildProducer<DebeziumGeneratedInvokerBuildItem> debeziumGeneratedInvokerBuildItemBuildProducer,
                                 BuildProducer<DebeziumGeneratedCustomConverterBuildItem> debeziumGeneratedCustomConverterBuildItemBuildProducer,
                                 BuildProducer<DebeziumGeneratedPostProcessorBuildItem> debeziumGeneratedPostProcessorBuildItemmBuildProducer) {
        GeneratedClassGizmoAdaptor classOutput = new GeneratedClassGizmoAdaptor(generatedClassBuildItemBuildProducer,
                true);

        CapturingInvokerGenerator capturingInvokerGenerator = new DefaultCapturingInvokerGenerator(new CapturingEventGenerator(classOutput),
                new InvokerGenerator(classOutput));

        PostProcessorGenerator postProcessorGenerator = new PostProcessorGenerator(classOutput);
        CustomConverterGenerator customConverterGenerator = new CustomConverterGenerator(classOutput);

        Map<Type, DebeziumMediatorBuildItem> fieldFilterStrategyByClassName = mediatorBuildItems.stream()
                .filter(item -> item.getDotName().equals(DebeziumDotNames.FIELD_FILTER_STRATEGY))
                .collect(Collectors.toMap(item -> item.getBean().getProviderType(), item -> item));

        mediatorBuildItems.forEach(item -> {
            if (item.getDotName().equals(DebeziumDotNames.CAPTURING)) {
                GeneratedClassMetaData metadata = capturingInvokerGenerator.generate(item.getMethodInfo(),
                        item.getBean());
                debeziumGeneratedInvokerBuildItemBuildProducer.produce(new DebeziumGeneratedInvokerBuildItem(metadata.generatedClassName(),
                        metadata.mediator(), metadata.getShortIdentifier(), metadata.clazz()));
                reflectiveClassBuildItemBuildProducer.produce(ReflectiveClassBuildItem.builder(metadata.generatedClassName()).build());
            }

            if (item.getDotName().equals(DebeziumDotNames.POST_PROCESSING)) {
                GeneratedClassMetaData metaData = postProcessorGenerator.generate(item.getMethodInfo(), item.getBean());

                debeziumGeneratedPostProcessorBuildItemmBuildProducer.produce(new DebeziumGeneratedPostProcessorBuildItem(
                        metaData.generatedClassName(),
                        metaData.mediator(),
                        metaData.getShortIdentifier()));

                reflectiveClassBuildItemBuildProducer.produce(ReflectiveClassBuildItem.builder(metaData.generatedClassName()).build());
            }

            if (item.getDotName().equals(DebeziumDotNames.CUSTOM_CONVERTER)) {
                BeanInfo filter = Optional.ofNullable(item.getMethodInfo().annotation(DebeziumDotNames.CUSTOM_CONVERTER).value("filter"))
                        .map(AnnotationValue::asClass)
                        .map(fieldFilterStrategyByClassName::get)
                        .map(DebeziumMediatorBuildItem::getBean)
                        .orElse(null);

                GeneratedConverterClassMetaData metaData = customConverterGenerator.generate(item.getMethodInfo(), item.getBean(), filter);
                debeziumGeneratedCustomConverterBuildItemBuildProducer.produce(new DebeziumGeneratedCustomConverterBuildItem(
                        metaData.generatedClassName(),
                        metaData.binder(),
                        metaData.getShortIdentifier(),
                        metaData.filter()));

                reflectiveClassBuildItemBuildProducer.produce(ReflectiveClassBuildItem.builder(metaData.generatedClassName()).build());
            }

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
                SyntheticBeanBuildItem.configure(item.getClazz())
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
    @Record(RUNTIME_INIT)
    public void injectPostProcessors(
                                     RecorderContext recorderContext,
                                     BuildProducer<SyntheticBeanBuildItem> syntheticBeanBuildItemBuildProducer,
                                     DynamicPostProcessingSupplier dynamicPostProcessingSupplier,
                                     List<DebeziumGeneratedPostProcessorBuildItem> debeziumGeneratedPostProcessorBuildItems) {
        debeziumGeneratedPostProcessorBuildItems.forEach(item -> syntheticBeanBuildItemBuildProducer.produce(
                SyntheticBeanBuildItem.configure(PostProcessor.class)
                        .setRuntimeInit()
                        .scope(ApplicationScoped.class)
                        .unremovable()
                        .supplier(dynamicPostProcessingSupplier.createPostProcessor(
                                recorderContext.classProxy(item.getMediator().getImplClazz().name().toString()),
                                (Class<? extends PostProcessor>) recorderContext.classProxy(item.getGeneratedClassName())))
                        .named(DynamicPostProcessingSupplier.BASE_NAME + item.getMediator().getImplClazz().name() + item.getId())
                        .done()));
    }

    @BuildStep
    @Record(RUNTIME_INIT)
    public void injectCustomConverters(
                                       RecorderContext recorderContext,
                                       BuildProducer<SyntheticBeanBuildItem> syntheticBeanBuildItemBuildProducer,
                                       DynamicCustomConverterSupplier dynamicCustomConverterSupplier,
                                       List<DebeziumGeneratedCustomConverterBuildItem> debeziumGeneratedCustomConverterBuildItems) {
        debeziumGeneratedCustomConverterBuildItems.forEach(item -> syntheticBeanBuildItemBuildProducer.produce(
                SyntheticBeanBuildItem.configure(QuarkusCustomConverter.class)
                        .setRuntimeInit()
                        .scope(ApplicationScoped.class)
                        .unremovable()
                        .supplier(dynamicCustomConverterSupplier.createQuarkusCustomConverter(
                                recorderContext.classProxy(item.getBinder().getImplClazz().name().toString()),
                                item.getFilter() != null ? recorderContext.classProxy(item.getFilter().getImplClazz().name().toString()) : null,
                                (Class<? extends QuarkusCustomConverter>) recorderContext.classProxy(item.getGeneratedClassName())))
                        .named(DynamicCustomConverterSupplier.BASE_NAME + item.getBinder().getImplClazz().name() + item.getId())
                        .done()));
    }

    @BuildStep
    public void extractMediators(BuildProducer<DebeziumMediatorBuildItem> mediatorBuildItemBuildProducer,
                                 BeanDiscoveryFinishedBuildItem beanDiscoveryFinished) {

        DebeziumDotNames dbz = new DebeziumDotNames();

        beanDiscoveryFinished
                .beanStream()
                .classBeans()
                .stream()
                .filter(dbz::filter)
                .flatMap(beanInfo -> beanInfo
                        .getTarget()
                        .map(target -> target.asClass().methods())
                        .stream()
                        .flatMap(Collection::stream)
                        .filter(dbz::filter)
                        .map(methodInfo -> new DebeziumMediatorBuildItem(beanInfo, methodInfo, dbz.get(methodInfo))))
                .forEach(mediatorBuildItemBuildProducer::produce);

        beanDiscoveryFinished
                .beanStream()
                .classBeans()
                .filter(info -> info.getImplClazz().interfaceNames().contains(DebeziumDotNames.FIELD_FILTER_STRATEGY))
                .stream()
                .map(info -> new DebeziumMediatorBuildItem(info, null, DebeziumDotNames.FIELD_FILTER_STRATEGY))
                .forEach(mediatorBuildItemBuildProducer::produce);
    }

    @BuildStep
    public List<UnremovableBeanBuildItem> removalExclusion() {
        return DebeziumDotNames.dotNames
                .stream()
                .map(dotName -> new UnremovableBeanBuildItem(new BeanClassAnnotationExclusion(dotName)))
                .toList();
    }

    @BuildStep
    public UnremovableBeanBuildItem avoidRemovalIfNotReferenced() {
        return UnremovableBeanBuildItem.beanTypes(FieldFilterStrategy.class);
    }
}
