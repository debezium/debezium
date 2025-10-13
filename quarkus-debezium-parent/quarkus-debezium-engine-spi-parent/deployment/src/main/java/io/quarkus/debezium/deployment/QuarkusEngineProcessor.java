/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment;

import java.util.function.Supplier;

import jakarta.inject.Singleton;

import io.debezium.runtime.ConnectorProducer;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.configuration.QuarkusDatasourceConfiguration;
import io.debezium.runtime.recorder.DatasourceRecorder;
import io.quarkus.arc.deployment.SyntheticBeanBuildItem;
import io.quarkus.debezium.deployment.items.DebeziumConnectorBuildItem;
import io.quarkus.debezium.deployment.items.DebeziumExtensionNameBuildItem;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.pkg.steps.NativeOrNativeSourcesBuild;

/**
 * The {@link QuarkusEngineProcessor} is the processor that guides what beans should be instantiated to create
 * a working Debezium Extension for Quarkus. The following interface doesn't provide guidelines to create a Quarkus DevServices
 * @param <C> is the {@link QuarkusDatasourceConfiguration} implemented for the Extension
 */
public interface QuarkusEngineProcessor<C extends QuarkusDatasourceConfiguration> {

    /**
     * Defines the name of the connectors that gets displayed in the log during application bootstrap
     * @return {@link DebeziumExtensionNameBuildItem}
     */
    @BuildStep
    DebeziumExtensionNameBuildItem debeziumExtensionNameBuildItem();

    /**
     * generates the {@link DebeziumConnectorBuildItem} that contains the Debezium Extension {@link ConnectorProducer} which creates the {@link DebeziumConnectorRegistry}
     * that is delegated to manage engines that are instantiated for the current connector
     * @return {@link DebeziumConnectorBuildItem}
     */
    @BuildStep
    DebeziumConnectorBuildItem engine();

    /**
     * instruments graalVM to add classes that are loaded through reflections. This build step is necessary for a correct native build
     * @param reflectiveClassBuildItemBuildProducer
     */
    @BuildStep(onlyIf = NativeOrNativeSourcesBuild.class)
    void registerClassesThatAreLoadedThroughReflection(BuildProducer<ReflectiveClassBuildItem> reflectiveClassBuildItemBuildProducer);

    /**
     *
     * @return the Class {@link QuarkusDatasourceConfiguration} used by the Extension
     */
    Class<C> quarkusDatasourceConfiguration();

    /**
     * Due to Quarkus limitations, we cannot use this method as direct build step (the bytecode generated for the record {@link DatasourceRecorder}
     * doesn't work in case of generics). The suggestion is to call the following method in a {@link BuildStep} in which we inject the {@link DatasourceRecorder}
     * and all the necessary items to generate a correct {@link SyntheticBeanBuildItem} that contains a {@link QuarkusDatasourceConfiguration} for the extension
     * @param supplier is the Supplier<C> returned by the {@link DatasourceRecorder}
     * @param producer the Quarkus {@link SyntheticBeanBuildItem} producer
     * @param name qualifier used for the bean
     */
    default void produceQuarkusDatasourceConfiguration(Supplier<C> supplier, BuildProducer<SyntheticBeanBuildItem> producer, String name) {
        producer.produce(SyntheticBeanBuildItem
                .configure(quarkusDatasourceConfiguration())
                .scope(Singleton.class)
                .supplier(supplier)
                .setRuntimeInit()
                .named(name)
                .done());
    }

}
