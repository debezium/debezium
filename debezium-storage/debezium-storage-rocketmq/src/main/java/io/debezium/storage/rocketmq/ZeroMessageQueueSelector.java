/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.rocketmq;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Always select MessageQueue with queue id 0
 */
public class ZeroMessageQueueSelector implements MessageQueueSelector {
    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        return mqs.stream().filter(messageQueue -> (messageQueue.getQueueId() == 0)).collect(Collectors.toSet()).stream().findFirst().get();
    }
}
