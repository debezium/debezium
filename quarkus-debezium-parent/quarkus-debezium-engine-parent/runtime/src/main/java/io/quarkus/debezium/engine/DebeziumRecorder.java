/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import io.debezium.runtime.DebeziumConnectorRegistry;
import io.quarkus.arc.runtime.BeanContainer;
import io.quarkus.runtime.ShutdownContext;
import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class DebeziumRecorder {

    public void startEngine(ShutdownContext context, BeanContainer container) {
        DebeziumConnectorRegistry debeziumConnectorRegistry = container.beanInstance(DebeziumConnectorRegistry.class);

        debeziumConnectorRegistry
                .engines()
                .stream()
                .map(debezium -> new DebeziumRunner(DebeziumThreadHandler.getThreadFactory(debezium), debezium))
                .forEach(runner -> {
                    runner.start();
                    context.addShutdownTask(runner::shutdown);
                });
    }

}
