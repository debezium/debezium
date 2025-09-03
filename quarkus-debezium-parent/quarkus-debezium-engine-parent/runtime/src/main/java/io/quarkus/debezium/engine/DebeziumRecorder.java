/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import io.debezium.runtime.CaptureGroup;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.quarkus.arc.runtime.BeanContainer;
import io.quarkus.runtime.ShutdownContext;
import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class DebeziumRecorder {

    private final Map<CaptureGroup, AtomicInteger> captureGroups = new ConcurrentHashMap<>();

    public void startEngine(ShutdownContext context, BeanContainer container) {
        DebeziumConnectorRegistry debeziumConnectorRegistry = container.beanInstance(DebeziumConnectorRegistry.class);

        debeziumConnectorRegistry
                .engines()
                .stream()
                .map(debezium -> new DebeziumRunner(generateThreadFactory(debezium), debezium))
                .forEach(runner -> {
                    runner.start();
                    context.addShutdownTask(runner::shutdown);
                });
    }

    private ThreadFactory generateThreadFactory(Debezium debezium) {
        return runnable -> {
            int num = captureGroups
                    .computeIfAbsent(debezium.captureGroup(), ignore -> new AtomicInteger(0))
                    .incrementAndGet();

            captureGroups.put(debezium.captureGroup(), new AtomicInteger(num));
            return new Thread(runnable, "dbz-" + debezium.captureGroup().id() + "-" + num);
        };
    }

}
