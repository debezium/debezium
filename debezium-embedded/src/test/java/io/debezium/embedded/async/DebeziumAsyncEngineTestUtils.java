/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.simple.SimpleSourceConnector;

/**
 * Utility classes and functions useful for testing implementations of {@link DebeziumEngine}.
 */
public class DebeziumAsyncEngineTestUtils {
    public static class InterruptedConnector extends SimpleSourceConnector {

        @Override
        public Class<? extends Task> taskClass() {
            return InterruptedTask.class;
        }
    }

    public static class InterruptedTask extends SimpleSourceConnector.SimpleConnectorTask {

        @Override
        public List<SourceRecord> poll() throws InterruptedException {
            throw new InterruptedException();
        }
    }

    public static class NoOpConnector extends SimpleSourceConnector {

        @Override
        public Class<? extends Task> taskClass() {
            return NoOpTask.class;
        }
    }

    public static class NoOpTask extends SimpleSourceConnector.SimpleConnectorTask {

        @Override
        public List<SourceRecord> poll() throws InterruptedException {
            return new ArrayList<SourceRecord>();
        }
    }

    public static class MultiTaskSimpleSourceConnector extends SimpleSourceConnector {

        private Map<String, String> config;

        @Override
        public void start(Map<String, String> props) {
            config = props;
        }

        @Override
        public List<Map<String, String>> taskConfigs(int maxTasks) {
            List<Map<String, String>> configs = new ArrayList<>();
            for (int i = 0; i < maxTasks; i++) {
                configs.add(config);
            }
            return configs;
        }
    }

    public static class RandomlyFailingDuringStartConnector extends MultiTaskSimpleSourceConnector {

        @Override
        public Class<? extends Task> taskClass() {
            return RandomlyFailingDuringStartTask.class;
        }
    }

    public static class RandomlyFailingDuringStartTask extends SimpleSourceConnector.SimpleConnectorTask {

        Random rand = new Random();

        @Override
        public void start(Map<String, String> props) {
            if (rand.nextBoolean()) {
                try {
                    // Give other tasks chance to start
                    Thread.sleep(100);
                }
                catch (InterruptedException e) {
                    throw new IllegalStateException("Unexpected interrupted exception");
                }
                throw new IllegalStateException("Exception during start of the task");
            }
        }
    }
}
