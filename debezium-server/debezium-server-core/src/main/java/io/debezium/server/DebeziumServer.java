/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.ChangeConsumer;
import io.debezium.engine.format.Avro;
import io.debezium.engine.format.Json;
import io.debezium.engine.format.SerializationFormat;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.health.Liveness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.inject.Inject;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * <p>The entry point of the Quarkus-based standalone server. The server is configured via Quarkus/Microprofile Configuration sources
 * and provides few out-of-the-box target implementations.</p>
 * <p>The implementation uses CDI to find all classes that implements {@link DebeziumEngine.ChangeConsumer} interface.
 * The candidate classes should be annotated with {@code @Named} annotation and should be {@code Dependent}.</p>
 * <p>The configuration option {@code debezium.consumer} provides a name of the consumer that should be used and the value
 * must match to exactly one of the implementation classes.</p>
 *
 * @author Jiri Pechanec
 *
 */
@ApplicationScoped
@Startup
public class DebeziumServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumServer.class);

    private static final String PROP_PREFIX = "debezium.";
    private static final String PROP_SOURCE_PREFIX = PROP_PREFIX + "source.";
    private static final String PROP_SINK_PREFIX = PROP_PREFIX + "sink.";
    private static final String PROP_FORMAT_PREFIX = PROP_PREFIX + "format.";
    private static final String PROP_TRANSFORMS_PREFIX = PROP_PREFIX + "transforms.";
    private static final String PROP_KEY_FORMAT_PREFIX = PROP_FORMAT_PREFIX + "key.";
    private static final String PROP_VALUE_FORMAT_PREFIX = PROP_FORMAT_PREFIX + "value.";

    private static final String PROP_TRANSFORMS = PROP_PREFIX + "transforms";
    private static final String PROP_SINK_TYPE = PROP_SINK_PREFIX + "type";
    private static final String PROP_KEY_FORMAT = PROP_FORMAT_PREFIX + "key";
    private static final String PROP_VALUE_FORMAT = PROP_FORMAT_PREFIX + "value";
    private static final String PROP_TERMINATION_WAIT = PROP_PREFIX + "termination.wait";

    private static final String FORMAT_JSON = Json.class.getSimpleName().toLowerCase();
    private static final String FORMAT_AVRO = Avro.class.getSimpleName().toLowerCase();

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @Inject
    BeanManager beanManager;

    @Inject
    @Liveness
    ConnectorLifecycle health;

    private Bean<DebeziumEngine.ChangeConsumer<ChangeEvent<?, ?>>> consumerBean;
    private CreationalContext<ChangeConsumer<ChangeEvent<?, ?>>> consumerBeanCreationalContext;
    private DebeziumEngine.ChangeConsumer<ChangeEvent<?, ?>> consumer;
    private DebeziumEngine<?> engine;

    @SuppressWarnings("unchecked")
    @PostConstruct
    public void start() {
        final Config config = ConfigProvider.getConfig();
        final String name = config.getValue(PROP_SINK_TYPE, String.class);

        final Set<Bean<?>> beans = beanManager.getBeans(name).stream()
                .filter(x -> DebeziumEngine.ChangeConsumer.class.isAssignableFrom(x.getBeanClass()))
                .collect(Collectors.toSet());
        LOGGER.debug("Found {} candidate consumer(s)", beans.size());

        if (beans.size() == 0) {
            throw new DebeziumException("No Debezium consumer named '" + name + "' is available");
        }
        else if (beans.size() > 1) {
            throw new DebeziumException("Multiple Debezium consumers named '" + name + "' were found");
        }

        consumerBean = (Bean<DebeziumEngine.ChangeConsumer<ChangeEvent<?, ?>>>) beans.iterator().next();
        consumerBeanCreationalContext = beanManager.createCreationalContext(consumerBean);
        consumer = consumerBean.create(consumerBeanCreationalContext);
        LOGGER.info("Consumer '{}' instantiated", consumer.getClass().getName());

        final Class<? extends SerializationFormat<?>> keyFormat = getFormat(config, PROP_KEY_FORMAT);
        final Class<? extends SerializationFormat<?>> valueFormat = getFormat(config, PROP_VALUE_FORMAT);
        final Properties props = new Properties();
        configToProperties(config, props, PROP_SOURCE_PREFIX, "");
        configToProperties(config, props, PROP_FORMAT_PREFIX, "key.converter.");
        configToProperties(config, props, PROP_FORMAT_PREFIX, "value.converter.");
        configToProperties(config, props, PROP_KEY_FORMAT_PREFIX, "key.converter.");
        configToProperties(config, props, PROP_VALUE_FORMAT_PREFIX, "value.converter.");
        final Optional<String> transforms = config.getOptionalValue(PROP_TRANSFORMS, String.class);
        if (transforms.isPresent()) {
            props.setProperty("transforms", transforms.get());
            configToProperties(config, props, PROP_TRANSFORMS_PREFIX, "transforms.");
        }
        props.setProperty("name", name);
        LOGGER.debug("Configuration for DebeziumEngine: {}", props);

        DebeziumEngine.Builder<?> builder = null;
        // TODO - apply variance and covariance rules on Debezium API to
        // support direct assignment to DebeziumEngine.Builder<ChangeEvent<?, ?>>
        if (keyFormat == Json.class && valueFormat == Json.class) {
            builder = createJsonJson(consumer);
        }
        else if (keyFormat == Json.class && valueFormat == Avro.class) {
            builder = createJsonAvro(consumer);
        }
        else if (keyFormat == Avro.class && valueFormat == Avro.class) {
            builder = createAvroAvro(consumer);
        }
        engine = builder
                .using(props)
                .using((DebeziumEngine.ConnectorCallback) health)
                .using((DebeziumEngine.CompletionCallback) health)
                .build();

        executor.execute(() -> engine.run());
        LOGGER.info("Engine executor started");
    }

    @SuppressWarnings("unchecked")
    private DebeziumEngine.Builder<?> createJsonJson(DebeziumEngine.ChangeConsumer<?> consumer) {
        return DebeziumEngine.create(Json.class, Json.class)
                .notifying((DebeziumEngine.ChangeConsumer<ChangeEvent<String, String>>) consumer);
    }

    @SuppressWarnings("unchecked")
    private DebeziumEngine.Builder<?> createAvroAvro(DebeziumEngine.ChangeConsumer<?> consumer) {
        return DebeziumEngine.create(Avro.class, Avro.class)
                .notifying((DebeziumEngine.ChangeConsumer<ChangeEvent<byte[], byte[]>>) consumer);
    }

    @SuppressWarnings("unchecked")
    private DebeziumEngine.Builder<?> createJsonAvro(DebeziumEngine.ChangeConsumer<?> consumer) {
        return DebeziumEngine.create(Json.class, Avro.class)
                .notifying((DebeziumEngine.ChangeConsumer<ChangeEvent<String, byte[]>>) consumer);
    }

    private void configToProperties(Config config, Properties props, String oldPrefix, String newPrefix) {
        for (String name : config.getPropertyNames()) {
            if (name.startsWith(oldPrefix)) {
                props.setProperty(newPrefix + name.substring(oldPrefix.length()), config.getValue(name, String.class));
            }
        }
    }

    private Class<? extends SerializationFormat<?>> getFormat(Config config, String property) {
        final String formatName = config.getOptionalValue(property, String.class).orElse(FORMAT_JSON);
        if (FORMAT_JSON.equals(formatName)) {
            return Json.class;
        }
        else if (FORMAT_AVRO.equals(formatName)) {
            return Avro.class;
        }
        throw new DebeziumException("Unknown format '" + formatName + "' for option " + "'" + property + "'");
    }

    public void stop(@Observes ShutdownEvent event) {
        try {
            LOGGER.info("Received request to stop the engine");
            final Config config = ConfigProvider.getConfig();
            engine.close();
            executor.shutdown();
            executor.awaitTermination(config.getOptionalValue(PROP_TERMINATION_WAIT, Integer.class).orElse(10), TimeUnit.SECONDS);
        }
        catch (Exception e) {
            LOGGER.error("Exception while shuttting down Debezium", e);
        }
        consumerBean.destroy(consumer, consumerBeanCreationalContext);
    }

    /**
     * For test purposes only
     */
    DebeziumEngine.ChangeConsumer<?> getConsumer() {
        return consumer;
    }
}
