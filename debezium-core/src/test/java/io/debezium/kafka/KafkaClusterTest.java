/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.junit.SkipLongRunning;
import io.debezium.junit.SkipTestRule;
import io.debezium.util.Collect;
import io.debezium.util.Stopwatch;
import io.debezium.util.Testing;

import kafka.server.KafkaConfig;

/**
 * @author Randall Hauch
 */
public class KafkaClusterTest {

    @Rule
    public TestRule skipTestRule = new SkipTestRule();

    private KafkaCluster cluster;
    private File dataDir;

    @Before
    public void beforeEach() {
        dataDir = Testing.Files.createTestingDirectory("cluster");
        cluster = new KafkaCluster().usingDirectory(dataDir)
                .deleteDataPriorToStartup(true)
                .deleteDataUponShutdown(true)
                .withKafkaConfiguration(Collect.propertiesOf(KafkaConfig.ZkSessionTimeoutMsProp(), "20000"));
    }

    @After
    public void afterEach() {
        cluster.shutdown();
        Testing.Files.delete(dataDir);
    }

    @Test
    @SkipLongRunning
    public void shouldStartClusterWithOneBrokerAndRemoveData() throws Exception {
        cluster.addBrokers(1).startup();
        cluster.onEachDirectory(this::assertValidDataDirectory);
        cluster.shutdown();
        cluster.onEachDirectory(this::assertDoesNotExist);
    }

    @Test
    @SkipLongRunning
    public void shouldStartClusterWithMultipleBrokerAndRemoveData() throws Exception {
        cluster.addBrokers(3).startup();
        cluster.onEachDirectory(this::assertValidDataDirectory);
        cluster.shutdown();
        cluster.onEachDirectory(this::assertDoesNotExist);
    }

    @Test
    @SkipLongRunning
    public void shouldStartClusterWithOneBrokerAndLeaveData() throws Exception {
        cluster.deleteDataUponShutdown(false).addBrokers(1).startup();
        cluster.onEachDirectory(this::assertValidDataDirectory);
        cluster.shutdown();
        cluster.onEachDirectory(this::assertValidDataDirectory);
    }

    @Test
    @SkipLongRunning
    public void shouldStartClusterWithMultipleBrokerAndLeaveData() throws Exception {
        cluster.deleteDataUponShutdown(false).addBrokers(3).startup();
        cluster.onEachDirectory(this::assertValidDataDirectory);
        cluster.shutdown();
        cluster.onEachDirectory(this::assertValidDataDirectory);
    }

    @Test
    @SkipLongRunning
    public void shouldStartClusterAndAllowProducersAndConsumersToUseIt() throws Exception {
        Testing.Debug.enable();
        final String topicName = "topicA";
        final CountDownLatch completion = new CountDownLatch(2);
        final int numMessages = 100;
        final AtomicLong messagesRead = new AtomicLong(0);

        // Start a cluster and create a topic ...
        cluster.addBrokers(1).startup();
        cluster.createTopics(topicName);

        // Consume messages asynchronously ...
        Stopwatch sw = Stopwatch.reusable().start();
        cluster.useTo().consumeIntegers(topicName, numMessages, 10, TimeUnit.SECONDS, completion::countDown, (key, value) -> {
            messagesRead.incrementAndGet();
            return true;
        });

        // Produce some messages asynchronously ...
        cluster.useTo().produceIntegers(topicName, numMessages, 1, completion::countDown);

        // Wait for both to complete ...
        if (completion.await(10, TimeUnit.SECONDS)) {
            sw.stop();
            Testing.debug("Both consumer and producer completed normally in " + sw.durations());
        }
        else {
            Testing.debug("Consumer and/or producer did not completed normally");
        }

        assertThat(messagesRead.get()).isEqualTo(numMessages);
    }

    @Test
    public void shouldStartClusterAndAllowInteractiveProductionAndAutomaticConsumersToUseIt() throws Exception {
        Testing.Debug.enable();
        final String topicName = "topicA";
        final CountDownLatch completion = new CountDownLatch(1);
        final int numMessages = 3;
        final AtomicLong messagesRead = new AtomicLong(0);

        // Start a cluster and create a topic ...
        cluster.addBrokers(1).startup();
        cluster.createTopics(topicName);

        // Consume messages asynchronously ...
        Stopwatch sw = Stopwatch.reusable().start();
        cluster.useTo().consumeIntegers(topicName, numMessages, 10, TimeUnit.SECONDS, completion::countDown, (key, value) -> {
            messagesRead.incrementAndGet();
            return true;
        });

        // Produce some messages interactively ...
        cluster.useTo()
                .createProducer("manual", new StringSerializer(), new IntegerSerializer())
                .write(topicName, "key1", 1)
                .write(topicName, "key2", 2)
                .write(topicName, "key3", 3)
                .close();

        // Wait for the consumer to to complete ...
        if (completion.await(10, TimeUnit.SECONDS)) {
            sw.stop();
            Testing.debug("The consumer completed normally in " + sw.durations());
        }
        else {
            Testing.debug("Consumer did not completed normally");
        }

        assertThat(messagesRead.get()).isEqualTo(numMessages);
    }

    @Test
    @SkipLongRunning
    public void shouldStartClusterAndAllowAsynchronousProductionAndAutomaticConsumersToUseIt() throws Exception {
        Testing.Debug.enable();
        final String topicName = "topicA";
        final CountDownLatch completion = new CountDownLatch(2);
        final int numMessages = 3;
        final AtomicLong messagesRead = new AtomicLong(0);

        // Start a cluster and create a topic ...
        cluster.addBrokers(1).startup();
        cluster.createTopics(topicName);

        // Consume messages asynchronously ...
        Stopwatch sw = Stopwatch.reusable().start();
        cluster.useTo().consumeIntegers(topicName, numMessages, 10, TimeUnit.SECONDS, completion::countDown, (key, value) -> {
            messagesRead.incrementAndGet();
            return true;
        });

        // Produce some messages interactively ...
        cluster.useTo().produce("manual", new StringSerializer(), new IntegerSerializer(), produer -> {
            produer.write(topicName, "key1", 1);
            produer.write(topicName, "key2", 2);
            produer.write(topicName, "key3", 3);
            completion.countDown();
        });

        // Wait for the consumer to to complete ...
        if (completion.await(10, TimeUnit.SECONDS)) {
            sw.stop();
            Testing.debug("The consumer completed normally in " + sw.durations());
        }
        else {
            Testing.debug("Consumer did not completed normally");
        }
        assertThat(messagesRead.get()).isEqualTo(numMessages);
    }

    @Test
    public void shouldSetClusterConfigProperty() throws Exception {
        Properties config = new Properties();
        config.put("foo", "bar");
        KafkaCluster kafkaCluster = new KafkaCluster().withKafkaConfiguration(config);

        Field kafkaConfigField = KafkaCluster.class.getDeclaredField("kafkaConfig");
        kafkaConfigField.setAccessible(true);
        Properties kafkaConfig = (Properties) kafkaConfigField.get(kafkaCluster);
        assertThat(kafkaConfig).hasSize(1);
    }

    @Test
    public void shouldSetServerConfigProperty() throws Exception {
        Properties config = new Properties();
        config.put("foo", "bar");
        KafkaCluster kafkaCluster = new KafkaCluster().withKafkaConfiguration(config).addBrokers(1);

        Field kafkaServersField = KafkaCluster.class.getDeclaredField("kafkaServers");
        kafkaServersField.setAccessible(true);
        ConcurrentMap<Integer, KafkaServer> kafkaServers = (ConcurrentMap<Integer, KafkaServer>) kafkaServersField.get(kafkaCluster);
        Properties serverConfig = kafkaServers.values().iterator().next().config();
        assertThat(serverConfig.get("foo")).isEqualTo("bar");
    }

    protected void assertValidDataDirectory(File dir) {
        assertThat(dir.exists()).isTrue();
        assertThat(dir.isDirectory()).isTrue();
        assertThat(dir.canWrite()).isTrue();
        assertThat(dir.canRead()).isTrue();
        assertThat(Testing.Files.inTestDataDir(dir)).isTrue();
    }

    protected void assertDoesNotExist(File dir) {
        assertThat(dir.exists()).isFalse();
    }
}
