/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.rocketmq.history;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.admin.TopicOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.document.DocumentReader;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.history.AbstractSchemaHistory;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.storage.rocketmq.RocketMqAdminUtil;
import io.debezium.storage.rocketmq.RocketMqConfig;
import io.debezium.storage.rocketmq.ZeroMessageQueueSelector;

@NotThreadSafe
public class RocketMqSchemaHistory extends AbstractSchemaHistory {
    public static final Field TOPIC = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "rocketmq.topic")
            .withDisplayName("Database schema history topic name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The name of the topic for the database schema history")
            .withValidation(RocketMqSchemaHistory.forRocketMq(Field::isRequired));
    /**
     * rocketmq name server addr
     */
    public static final Field NAME_SRV_ADDR = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "rocketmq.name.srv.addr")
            .withDisplayName("NameServer address")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("RocketMQ service discovery service nameserver address configuration")
            .withValidation(Field::isRequired);
    public static final Field ROCKETMQ_ACL_ENABLE = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "rocketmq.acl.enabled")
            .withDisplayName("Access control list enabled")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(false)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("RocketMQ access control enable configuration, default is 'false'");
    public static final Field ROCKETMQ_ACCESS_KEY = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "rocketmq.access.key")
            .withDisplayName("RocketMQ access key")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("RocketMQ access key. If " + ROCKETMQ_ACL_ENABLE + " is true, the value cannot be empty");
    public static final Field ROCKETMQ_SECRET_KEY = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "rocketmq.secret.key")
            .withDisplayName("RocketMQ secret key")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("RocketMQ secret key. If " + ROCKETMQ_ACL_ENABLE + " is true, the value cannot be empty");
    public static final Field RECOVERY_POLL_ATTEMPTS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "rocketmq.recovery.attempts")
            .withDisplayName("Max attempts to recovery database schema history")
            .withType(ConfigDef.Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.ADVANCED, 0))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The number of attempts in a row that no data are returned from RocketMQ before recover " +
                    "completes. "
                    + "The maximum amount of time to wait after receiving no data is (recovery.attempts) x (recovery.poll.interval.ms).")
            .withDefault(60)
            .withValidation(Field::isInteger);
    public static final Field RECOVERY_POLL_INTERVAL_MS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING
            + "rocketmq.recovery.poll.interval.ms")
            .withDisplayName("Poll interval during database schema history recovery (ms)")
            .withType(ConfigDef.Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.ADVANCED, 1))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The number of milliseconds to wait while polling for persisted data during recovery.")
            .withDefault(1000)
            .withValidation(Field::isLong);
    public static final Field STORE_RECORD_TIMEOUT_MS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING
            + "rocketmq.store.record.timeout.ms")
            .withDisplayName("Timeout for sending messages to RocketMQ")
            .withType(ConfigDef.Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.ADVANCED, 1))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Timeout for sending messages to RocketMQ.")
            .withDefault(60 * 1000)
            .withValidation(Field::isLong);
    private final static Logger LOGGER = LoggerFactory.getLogger(RocketMqSchemaHistory.class);
    private static final Integer MESSAGE_QUEUE = 0;
    private final DocumentReader reader = DocumentReader.defaultReader();
    private String topicName;
    private String dbHistoryName;
    private DefaultMQProducer producer;
    private RocketMqConfig rocketMqConfig;
    private int maxRecoveryAttempts;
    private Long pollInterval;
    private Long sendingTimeout;

    private static Field.Validator forRocketMq(final Field.Validator validator) {
        return (config, field, problems) -> {
            final String history = config.getString(HistorizedRelationalDatabaseConnectorConfig.SCHEMA_HISTORY);
            return RocketMqSchemaHistory.class.getName().equals(history) ? validator.validate(config, field, problems) : 0;
        };
    }

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener, boolean useCatalogBeforeSchema) {
        super.configure(config, comparator, listener, useCatalogBeforeSchema);
        this.topicName = config.getString(TOPIC);
        this.dbHistoryName = config.getString(SchemaHistory.NAME, UUID.randomUUID().toString());
        this.maxRecoveryAttempts = config.getInteger(RECOVERY_POLL_ATTEMPTS);
        this.pollInterval = config.getLong(RECOVERY_POLL_INTERVAL_MS);
        this.sendingTimeout = config.getLong(STORE_RECORD_TIMEOUT_MS);
        LOGGER.info("Configure to store the debezium database history {} to rocketmq topic {}",
                dbHistoryName, topicName);
        // Check acl config
        boolean aclEnabled = config.getBoolean(RocketMqSchemaHistory.ROCKETMQ_ACL_ENABLE);
        String accessKey = config.getString(RocketMqSchemaHistory.ROCKETMQ_ACCESS_KEY);
        String secretKey = config.getString(RocketMqSchemaHistory.ROCKETMQ_SECRET_KEY);
        if (aclEnabled && (accessKey == null || secretKey == null)) {
            throw new SchemaHistoryException(
                    "if " + ROCKETMQ_ACL_ENABLE + " true,the configuration " + ROCKETMQ_ACCESS_KEY + " and " + ROCKETMQ_SECRET_KEY + " cannot be empty");
        }

        // build config
        this.rocketMqConfig = RocketMqConfig.newBuilder()
                .aclEnable(config.getBoolean(RocketMqSchemaHistory.ROCKETMQ_ACL_ENABLE))
                .accessKey(config.getString(RocketMqSchemaHistory.ROCKETMQ_ACCESS_KEY))
                .secretKey(config.getString(RocketMqSchemaHistory.ROCKETMQ_SECRET_KEY))
                .namesrvAddr(config.getString(RocketMqSchemaHistory.NAME_SRV_ADDR))
                .groupId(dbHistoryName)
                .build();
    }

    @Override
    public void initializeStorage() {
        super.initializeStorage();
        LOGGER.info("try to create history topic: {}!", this.topicName);
        TopicConfig topicConfig = new TopicConfig(this.topicName, 1, 1, PermName.PERM_READ | PermName.PERM_WRITE);
        RocketMqAdminUtil.createTopic(rocketMqConfig, topicConfig);
    }

    @Override
    public synchronized void start() {
        super.start();
        try {
            // Check and create group
            Set<String> consumerGroupSet = RocketMqAdminUtil.fetchAllConsumerGroup(rocketMqConfig);
            if (!consumerGroupSet.contains(rocketMqConfig.getGroupId())) {
                RocketMqAdminUtil.createGroup(rocketMqConfig, rocketMqConfig.getGroupId());
            }
            // Start rocketmq producer
            this.producer = RocketMqAdminUtil.initDefaultMqProducer(rocketMqConfig);
            this.producer.start();
        }
        catch (MQClientException e) {
            throw new SchemaHistoryException(e);
        }
    }

    @Override
    protected void storeRecord(HistoryRecord record) throws SchemaHistoryException {
        if (this.producer == null) {
            throw new IllegalStateException("No producer is available. Ensure that 'initializeStorage()'"
                    + " is called before storing database schema history records.");
        }
        LOGGER.trace("Storing record into database schema history: {}", record);
        try {
            Message message = new Message(this.topicName, record.toString().getBytes());
            // Send messages synchronously until successful
            // Only sending success or exception will be returned
            SendResult sendResult = producer.send(message, new ZeroMessageQueueSelector(), null, this.sendingTimeout);
            switch (sendResult.getSendStatus()) {
                case SEND_OK:
                    LOGGER.debug("Stored record in topic '{}' partition {} at offset {} ",
                            message.getTopic(), sendResult.getMessageQueue(), sendResult.getMessageQueue());
                    break;
                default:
                    LOGGER.warn("Stored record in topic '{}' partition {} at offset {}, send status {}",
                            message.getTopic(), sendResult.getMessageQueue(), sendResult.getMessageQueue(),
                            sendResult.getSendStatus());
            }

        }
        catch (InterruptedException e) {
            LOGGER.error("Interrupted before record was written into database schema history: {}", record);
            Thread.currentThread().interrupt();
            throw new SchemaHistoryException(e);
        }
        catch (MQClientException | RemotingException | MQBrokerException e) {
            throw new SchemaHistoryException(e);
        }
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> records) {
        DefaultLitePullConsumer consumer = null;
        try {
            consumer = RocketMqAdminUtil.initDefaultLitePullConsumer(rocketMqConfig, false);
            consumer.start();

            // Select message queue
            MessageQueue messageQueue = new ZeroMessageQueueSelector().select(new ArrayList<>(consumer.fetchMessageQueues(topicName)), null, null);
            consumer.assign(Collections.singleton(messageQueue));
            consumer.seekToBegin(messageQueue);
            // Read all messages in the topic ...
            long lastProcessedOffset = -1;
            Long maxOffset = null;
            int recoveryAttempts = 0;

            do {
                if (recoveryAttempts > maxRecoveryAttempts) {
                    throw new IllegalStateException(
                            "The database schema history couldn't be recovered.");
                }
                // Get db schema history topic end offset
                maxOffset = getMaxOffsetOfSchemaHistoryTopic(maxOffset, messageQueue);
                LOGGER.debug("End offset of database schema history topic is {}", maxOffset);

                // Poll record from db schema history topic
                List<MessageExt> recoveredRecords = consumer.poll(pollInterval);
                int numRecordsProcessed = 0;

                for (MessageExt message : recoveredRecords) {
                    if (message.getQueueOffset() > lastProcessedOffset) {
                        HistoryRecord recordObj = new HistoryRecord(reader.read(message.getBody()));
                        LOGGER.trace("Recovering database history: {}", recordObj);
                        if (recordObj == null || !recordObj.isValid()) {
                            LOGGER.warn("Skipping invalid database history record '{}'. " +
                                    "This is often not an issue, but if it happens repeatedly please check the '{}' topic.",
                                    recordObj, topicName);
                        }
                        else {
                            records.accept(recordObj);
                            LOGGER.trace("Recovered database history: {}", recordObj);
                        }
                        lastProcessedOffset = message.getQueueOffset();
                        ++numRecordsProcessed;
                    }
                }
                if (numRecordsProcessed == 0) {
                    LOGGER.debug("No new records found in the database schema history; will retry");
                    recoveryAttempts++;
                }
                else {
                    LOGGER.debug("Processed {} records from database schema history", numRecordsProcessed);
                }

            } while (lastProcessedOffset < maxOffset - 1);

        }
        catch (MQClientException | MQBrokerException | IOException | RemotingException | InterruptedException ce) {
            throw new SchemaHistoryException(ce);
        }
        finally {
            if (consumer != null) {
                consumer.shutdown();
            }
        }
    }

    private Long getMaxOffsetOfSchemaHistoryTopic(Long previousEndOffset, MessageQueue messageQueue)
            throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        Map<MessageQueue, TopicOffset> minAndMaxOffsets = RocketMqAdminUtil.offsets(this.rocketMqConfig, topicName);
        Long maxOffset = minAndMaxOffsets.get(messageQueue).getMaxOffset();
        if (previousEndOffset != null && !previousEndOffset.equals(maxOffset)) {
            LOGGER.warn("Detected changed end offset of database schema history topic (previous: "
                    + previousEndOffset + ", current: " + maxOffset
                    + "). Make sure that the same history topic isn't shared by multiple connector instances.");
        }
        return maxOffset;
    }

    @Override
    public boolean exists() {
        boolean exists = false;
        if (this.storageExists()) {
            Map<MessageQueue, TopicOffset> minAndMaxOffset = RocketMqAdminUtil.offsets(this.rocketMqConfig,
                    topicName);
            for (MessageQueue messageQueue : minAndMaxOffset.keySet()) {
                if (MESSAGE_QUEUE == messageQueue.getQueueId()) {
                    exists = minAndMaxOffset.get(messageQueue).getMaxOffset() > minAndMaxOffset.get(messageQueue).getMinOffset();
                }
            }
        }
        return exists;
    }

    @Override
    public boolean storageExists() {
        // Check whether topic exists
        return RocketMqAdminUtil.topicExist(rocketMqConfig, this.topicName);
    }
}
