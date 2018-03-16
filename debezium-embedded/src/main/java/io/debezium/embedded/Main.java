/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Configuration.Builder;
import io.debezium.embedded.EmbeddedEngine.CompletionCallback;
import io.debezium.embedded.EmbeddedEngine.ConnectorCallback;
import io.debezium.util.Clock;

/**
 * <p>
 * A pre-made class that could be used to run a microservice whose responsibility is to react to
 * change data capture events. The class is configured via Microprofile Config API which translates
 * into a combination of system properties, environment variables and a contents of {@code microprofile-config.properties}
 * file.
 * </p>
 * <p>
 * It is expected that the consumer provides an implementation of the {@link ChangeEventHandler} interface
 * that will receive all captured change data. The implementation must be located on the classpath and
 * the name of the class should be set in {@code io.debezium.embedded.handler} config property.
 * </p>
 * <p>
 * If the engine is running inside a runtime supporting Microprofile Health API then it is possible to obtain
 * an instance {@link HealthCheck} object via {@link #getHealthCheck()} method which could be then for example
 * expose via CDI producer.
 *
 * @author Jiri Pechanec
 *
 */
public class Main implements Runnable {

    public static final String HANDLER_CLASS_CONF_NAME = "io.debezium.embedded.handler";

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private final Configuration engineConfig;
    private ChangeEventHandler handler;
    private final AtomicBoolean isRunning = new AtomicBoolean();
    private final HealthCheck healthCheck;

    @SuppressWarnings("unchecked")
    public Main() {
        final Config config = ConfigProvider.getConfig();
        final Builder configBuilder = Configuration.create();
        final String engineName = config.getValue(EmbeddedEngine.ENGINE_NAME.name(), String.class);

        config.getPropertyNames().forEach(x -> configBuilder.with(x, config.getValue(x, String.class)));
        engineConfig = configBuilder.build();

        final Class<ChangeEventHandler> handlerClass = (Class<ChangeEventHandler>)config.getValue(HANDLER_CLASS_CONF_NAME, Class.class);
        try {
            handler = handlerClass.newInstance();
        }
        catch (InstantiationException | IllegalAccessException e) {
            throw new IllegalStateException("Could not instantiate a change event handler '" + handlerClass.getName() + "'");
        }
        
        healthCheck = () -> HealthCheckResponse.named(engineName).state(isRunning.get()).build();
    }

    /**
     * The main method that instantiates {@link EmbeddedEngine} and is responsible for lifecycle handling.
     */
    public void run() {
        final EmbeddedEngine engine = EmbeddedEngine.create()
                .using(engineConfig)
                .using(this.getClass().getClassLoader())
                .using(Clock.SYSTEM)
                .notifying(handler::handle)
                .using(new ConnectorCallback() {

                    @Override
                    public void taskStarted() {
                        isRunning.set(true);
                    }

                    @Override
                    public void taskStopped() {
                        isRunning.set(false);
                    }
                    
                })
                .using(new CompletionCallback() {
                    @Override
                    public void handle(boolean success, String message, Throwable error) {
                        if (success) {
                            LOG.info("Debezium engine has finished successfully");
                        }
                        else {
                            LOG.error("Error in Debezium engine", error);
                        }
                        isRunning.set(false);
                    }
                })
                .build();

        final Thread mainThread = Thread.currentThread();
        final AtomicBoolean waitForMainTermination = new AtomicBoolean(true); 
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown requested");
            engine.stop();
            try {
                if (waitForMainTermination.get()) {
                    mainThread.join();
                }
            }
            catch (InterruptedException e) {
            }
            LOG.info("Shutdown hook completed");
        }));
        engine.run();
        waitForMainTermination.set(false);
    }

    public static void main(String[] args) {
        new Main().run();
    }

    /**
     * @return Microprofile Health {@link HealthCheck} object that is {@code UP} if a connector task is started
     */
    public HealthCheck getHealthCheck() {
        return healthCheck;
    }
}
