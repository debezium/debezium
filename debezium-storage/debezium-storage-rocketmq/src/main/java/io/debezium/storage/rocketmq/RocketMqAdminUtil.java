/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.rocketmq;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.admin.TopicOffset;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;

import io.debezium.DebeziumException;

/**
 * Tools for creating RocketMQ topic and group
 */
public class RocketMqAdminUtil {

    public static String createUniqInstance(String prefix) {
        return prefix.concat("-").concat(UUID.randomUUID().toString());
    }

    public static RPCHook getAclRPCHook(String accessKey, String secretKey) {
        return new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
    }

    public static DefaultLitePullConsumer initDefaultLitePullConsumer(RocketMqConfig config,
                                                                      boolean autoCommit)
            throws MQClientException {
        DefaultLitePullConsumer consumer = null;
        if (Objects.isNull(consumer)) {
            if (StringUtils.isBlank(config.getAccessKey()) && StringUtils.isBlank(config.getSecretKey())) {
                consumer = new DefaultLitePullConsumer(
                        config.getGroupId());
            }
            else {
                consumer = new DefaultLitePullConsumer(
                        config.getGroupId(),
                        getAclRPCHook(config.getAccessKey(), config.getSecretKey()));
            }
        }
        consumer.setNamesrvAddr(config.getNamesrvAddr());
        String uniqueName = createUniqInstance(config.getNamesrvAddr());
        consumer.setInstanceName(uniqueName);
        consumer.setUnitName(uniqueName);
        consumer.setAutoCommit(autoCommit);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        return consumer;
    }

    public static DefaultMQProducer initDefaultMqProducer(RocketMqConfig connectConfig) {
        RPCHook rpcHook = null;
        if (connectConfig.isAclEnable()) {
            rpcHook = new AclClientRPCHook(new SessionCredentials(connectConfig.getAccessKey(), connectConfig.getSecretKey()));
        }
        DefaultMQProducer producer = new DefaultMQProducer(rpcHook);
        producer.setNamesrvAddr(connectConfig.getNamesrvAddr());
        producer.setInstanceName(createUniqInstance(connectConfig.getNamesrvAddr()));
        producer.setProducerGroup(connectConfig.getGroupId());
        producer.setLanguage(LanguageCode.JAVA);
        return producer;
    }

    private static DefaultMQAdminExt startMqAdminTool(RocketMqConfig config) throws MQClientException {
        DefaultMQAdminExt admin;
        if (config.isAclEnable()) {
            admin = new DefaultMQAdminExt(new AclClientRPCHook(new SessionCredentials(config.getAccessKey(), config.getSecretKey())));
        }
        else {
            admin = new DefaultMQAdminExt();
        }
        admin.setNamesrvAddr(config.getNamesrvAddr());
        admin.setAdminExtGroup(config.getGroupId());
        admin.setInstanceName(createUniqInstance(config.getNamesrvAddr()));
        admin.start();
        return admin;
    }

    public static void createTopic(RocketMqConfig config, TopicConfig topicConfig) {
        DefaultMQAdminExt defaultMQAdminExt = null;
        try {
            defaultMQAdminExt = startMqAdminTool(config);
            ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
            HashMap<String, Set<String>> clusterAddrTable = clusterInfo.getClusterAddrTable();
            Set<String> clusterNameSet = clusterAddrTable.keySet();
            for (String clusterName : clusterNameSet) {
                Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String addr : masterSet) {
                    defaultMQAdminExt.createAndUpdateTopicConfig(addr, topicConfig);
                }
            }
        }
        catch (Exception e) {
            throw new DebeziumException("RocketMQ create schema history topic: " + topicConfig.getTopicName() + " " +
                    " failed", e);
        }
        finally {
            if (defaultMQAdminExt != null) {
                defaultMQAdminExt.shutdown();
            }
        }
    }

    public static boolean topicExist(RocketMqConfig config, String topic) {
        DefaultMQAdminExt defaultMQAdminExt = null;
        boolean foundTopicRouteInfo = false;
        try {
            defaultMQAdminExt = startMqAdminTool(config);
            TopicRouteData topicRouteData = defaultMQAdminExt.examineTopicRouteInfo(topic);
            if (topicRouteData != null) {
                foundTopicRouteInfo = true;
            }
        }
        catch (Exception e) {
            if (e instanceof MQClientException) {
                if (((MQClientException) e).getResponseCode() == ResponseCode.TOPIC_NOT_EXIST) {
                    foundTopicRouteInfo = false;
                }
                else {
                    throw new RuntimeException("Failed to get topic information", e);
                }
            }
            else {
                throw new RuntimeException("Failed to get topic information", e);
            }
        }
        finally {
            if (defaultMQAdminExt != null) {
                defaultMQAdminExt.shutdown();
            }
        }
        return foundTopicRouteInfo;
    }

    public static Set<String> fetchAllConsumerGroup(RocketMqConfig connectConfig) {
        Set<String> consumerGroupSet = new HashSet<>();
        DefaultMQAdminExt defaultMQAdminExt = null;
        try {
            defaultMQAdminExt = startMqAdminTool(connectConfig);
            ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
            for (BrokerData brokerData : clusterInfo.getBrokerAddrTable().values()) {
                SubscriptionGroupWrapper subscriptionGroupWrapper = defaultMQAdminExt.getAllSubscriptionGroup(brokerData.selectBrokerAddr(), 3000L);
                consumerGroupSet.addAll(subscriptionGroupWrapper.getSubscriptionGroupTable().keySet());
            }
        }
        catch (Exception e) {
            throw new DebeziumException("RocketMQ admin fetch all topic failed", e);
        }
        finally {
            if (defaultMQAdminExt != null) {
                defaultMQAdminExt.shutdown();
            }
        }
        return consumerGroupSet;
    }

    public static String createGroup(RocketMqConfig connectConfig, String group) {
        DefaultMQAdminExt defaultMQAdminExt = null;
        try {
            defaultMQAdminExt = startMqAdminTool(connectConfig);
            SubscriptionGroupConfig initConfig = new SubscriptionGroupConfig();
            initConfig.setGroupName(group);
            ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
            HashMap<String, Set<String>> clusterAddrTable = clusterInfo.getClusterAddrTable();
            Set<String> clusterNameSet = clusterAddrTable.keySet();
            for (String clusterName : clusterNameSet) {
                Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String addr : masterSet) {
                    defaultMQAdminExt.createAndUpdateSubscriptionGroupConfig(addr, initConfig);
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException("create subGroup: " + group + " failed", e);
        }
        finally {
            if (defaultMQAdminExt != null) {
                defaultMQAdminExt.shutdown();
            }
        }
        return group;
    }

    public static Map<MessageQueue, TopicOffset> offsets(RocketMqConfig config, String topic) {
        // Get database schema topic min and max offset
        DefaultMQAdminExt adminClient = null;
        try {
            adminClient = RocketMqAdminUtil.startMqAdminTool(config);
            TopicStatsTable topicStatsTable = adminClient.examineTopicStats(topic);
            return topicStatsTable.getOffsetTable();
        }
        catch (MQClientException | MQBrokerException | RemotingException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        finally {
            if (adminClient != null) {
                adminClient.shutdown();
            }
        }
    }

}
