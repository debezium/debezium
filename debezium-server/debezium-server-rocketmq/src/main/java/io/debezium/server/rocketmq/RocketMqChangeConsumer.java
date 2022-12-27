/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.rocketmq;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.CustomConsumerBuilder;

/**
 * rocketmq change consumer
 */
@Named("rocketmq")
@Dependent
public class RocketMqChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocketMqChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.rocketmq.";
    private static final String PROP_PRODUCER_PREFIX = PROP_PREFIX + "producer.";

    // acl config
    private static final String PROP_PRODUCER_ACL_ENABLE = PROP_PRODUCER_PREFIX + "acl.enabled";
    private static final String PROP_PRODUCER_ACCESS_KEY = PROP_PRODUCER_PREFIX + "access.key";
    private static final String PROP_PRODUCER_SECRET_KEY = PROP_PRODUCER_PREFIX + "secret.key";
    // common config
    private static final String PROP_PRODUCER_NAME_SRV_ADDR = PROP_PRODUCER_PREFIX + "name.srv.addr";
    private static final String PROP_PRODUCER_GROUP = PROP_PRODUCER_PREFIX + "group";
    private static final String PROP_PRODUCER_MAX_MESSAGE_SIZE = PROP_PRODUCER_PREFIX + "max.message.size";
    private static final String PROP_PRODUCER_SEND_MSG_TIMEOUT = PROP_PRODUCER_PREFIX + "send.msg.timeout";

    private DefaultMQProducer mqProducer;

    @Inject
    @CustomConsumerBuilder
    Instance<DefaultMQProducer> customRocketMqProducer;

    @PostConstruct
    void connect() {

        if (customRocketMqProducer.isResolvable()) {
            mqProducer = customRocketMqProducer.get();
            startProducer();
            LOGGER.info("Obtained custom configured RocketMqProducer '{}'", mqProducer);
            return;
        }

        final Config config = ConfigProvider.getConfig();
        Map<String, Object> props = getConfigSubset(config, PROP_PRODUCER_PREFIX);
        // Init rocketmq producer
        RPCHook rpcHook = null;
        Optional<Boolean> aclEnable = config.getOptionalValue(PROP_PRODUCER_ACL_ENABLE, Boolean.class);
        if (aclEnable.isPresent() && aclEnable.get()) {
            if (config.getOptionalValue(PROP_PRODUCER_ACCESS_KEY, String.class).isEmpty()
                    || config.getOptionalValue(PROP_PRODUCER_SECRET_KEY, String.class).isEmpty()) {
                throw new DebeziumException("When acl.enabled is true, access key and secret key cannot be empty");
            }
            rpcHook = new AclClientRPCHook(
                    new SessionCredentials(
                            config.getValue(PROP_PRODUCER_ACCESS_KEY, String.class),
                            config.getValue(PROP_PRODUCER_SECRET_KEY, String.class)));
        }
        this.mqProducer = new DefaultMQProducer(rpcHook);
        this.mqProducer.setNamesrvAddr(config.getValue(PROP_PRODUCER_NAME_SRV_ADDR, String.class));
        this.mqProducer.setInstanceName(createUniqInstance(config.getValue(PROP_PRODUCER_NAME_SRV_ADDR, String.class)));
        this.mqProducer.setProducerGroup(config.getValue(PROP_PRODUCER_GROUP, String.class));

        if (config.getOptionalValue(PROP_PRODUCER_SEND_MSG_TIMEOUT, Integer.class).isPresent()) {
            this.mqProducer.setSendMsgTimeout(config.getValue(PROP_PRODUCER_SEND_MSG_TIMEOUT, Integer.class));
        }

        if (config.getOptionalValue(PROP_PRODUCER_MAX_MESSAGE_SIZE, Integer.class).isPresent()) {
            this.mqProducer.setMaxMessageSize(config.getValue(PROP_PRODUCER_MAX_MESSAGE_SIZE, Integer.class));
        }
        this.mqProducer.setLanguage(LanguageCode.JAVA);
        startProducer();
    }

    private void startProducer() {
        try {
            this.mqProducer.start();
            LOGGER.info("Consumer started...");
        }
        catch (MQClientException e) {
            throw new DebeziumException(e);
        }
    }

    private String createUniqInstance(String prefix) {
        return prefix.concat("-").concat(UUID.randomUUID().toString());
    }

    @PreDestroy
    void close() {
        // Closed rocketmq producer
        LOGGER.info("Consumer destroy...");
        if (mqProducer != null) {
            mqProducer.shutdown();
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(records.size());
        for (ChangeEvent<Object, Object> record : records) {
            try {
                LOGGER.trace("Received event '{}'", record);
                final String topicName = streamNameMapper.map(record.destination());
                mqProducer.send(new Message(topicName, null, getString(record.key()), getBytes(record.value())),
                        new SendCallback() {
                            @Override
                            public void onSuccess(SendResult sendResult) {
                                LOGGER.trace("Sent message with offset: {}", sendResult.getQueueOffset());
                                latch.countDown();
                            }

                            @Override
                            public void onException(Throwable throwable) {
                                LOGGER.error("Failed to send record to {}:", record.destination(), throwable);
                                throw new DebeziumException(throwable);
                            }
                        });
                committer.markProcessed(record);
            }
            catch (Exception e) {
                throw new DebeziumException(e);
            }
        }
        latch.await();
        committer.markBatchFinished();
    }
}
