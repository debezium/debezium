/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.rocketmq;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import javax.enterprise.event.Observes;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import io.debezium.server.TestConfigSource;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@QuarkusTestResource(RocketMqTestResourceLifecycleManager.class)
public class RocketMqIT {

    private static final int MESSAGE_COUNT = 4;

    private static DefaultLitePullConsumer consumer = null;

    {
        Testing.Files.delete(TestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(RocketMqTestConfigSource.OFFSET_STORE_PATH);
    }

    @AfterAll
    static void stop() {
        if (consumer != null) {
            consumer.shutdown();
        }
    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw new RuntimeException(event.getError().get());
        }
    }

    @Test
    public void testRocketMQ() throws Exception {
        // start consumer
        consumer = new DefaultLitePullConsumer("consumer-group");
        consumer.setNamesrvAddr(RocketMqTestResourceLifecycleManager.getNameSrvAddr());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(RocketMqTestConfigSource.TOPIC_NAME, "*");
        consumer.start();

        // consume record
        final List<MessageExt> records = new ArrayList<>();
        Awaitility.await().atMost(Duration.ofSeconds(RocketMqTestConfigSource.waitForSeconds())).until(() -> {
            records.addAll(this.consumer.poll(5000));
            return records.size() >= MESSAGE_COUNT;
        });
        assertThat(records.size()).isGreaterThanOrEqualTo(MESSAGE_COUNT);
    }
}
