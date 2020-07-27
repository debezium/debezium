/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.health.Liveness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.ChangeConsumer;
import io.debezium.engine.format.Avro;
import io.debezium.engine.format.Json;
import io.debezium.engine.format.Protobuf;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;

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
    private static final String FORMAT_PROTOBUF = Protobuf.class.getSimpleName().toLowerCase();

    private ExecutorService executor = Executors.newSingleThreadExecutor();

    @Inject
    BeanManager beanManager;

    @Inject
    @Liveness
    ConnectorLifecycle health;

    private Bean<DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>>> consumerBean;
    private CreationalContext<ChangeConsumer<ChangeEvent<Object, Object>>> consumerBeanCreationalContext;
    private DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> consumer;
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

        consumerBean = (Bean<DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>>>) beans.iterator().next();
        consumerBeanCreationalContext = beanManager.createCreationalContext(consumerBean);
        consumer = consumerBean.create(consumerBeanCreationalContext);
        LOGGER.info("Consumer '{}' instantiated", consumer.getClass().getName());

        final Class<Any> keyFormat = (Class<Any>) getFormat(config, PROP_KEY_FORMAT);
        final Class<Any> valueFormat = (Class<Any>) getFormat(config, PROP_VALUE_FORMAT);
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

        engine = DebeziumEngine.create(keyFormat, valueFormat)
                .notifying(consumer)
                .using(props)
                .using((DebeziumEngine.ConnectorCallback) health)
                .using((DebeziumEngine.CompletionCallback) health)
                .build();

        executor.execute(() -> engine.run());
        LOGGER.info("Engine executor started");
    }

    private void configToProperties(Config config, Properties props, String oldPrefix, String newPrefix) {
        for (String name : config.getPropertyNames()) {
            if (name.startsWith(oldPrefix)) {
                props.setProperty(newPrefix + name.substring(oldPrefix.length()), config.getValue(name, String.class));
            }
        }
    }

    private Class<?> getFormat(Config config, String property) {
        final String formatName = config.getOptionalValue(property, String.class).orElse(FORMAT_JSON);
        if (FORMAT_JSON.equals(formatName)) {
            return Json.class;
        }
        else if (FORMAT_AVRO.equals(formatName)) {
            return Avro.class;
        }
        else if (FORMAT_PROTOBUF.equals(formatName)) {
            return Protobuf.class;
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
