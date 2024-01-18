/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.rocketmq;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Before;
import org.junit.Test;

public class ZeroMessageQueueSelectorTest {

    private static List<MessageQueue> messageQueues;

    @Before
    public void before() {
        messageQueues = new ArrayList<>();
        messageQueues.add(new MessageQueue("topic", "brokerName", 0));
        messageQueues.add(new MessageQueue("topic", "brokerName", 1));
    }

    @Test
    public void testSelectQueue() {
        MessageQueue messageQueue = new ZeroMessageQueueSelector().select(messageQueues, null, null);
        assertEquals(0, messageQueue.getQueueId());
    }
}
