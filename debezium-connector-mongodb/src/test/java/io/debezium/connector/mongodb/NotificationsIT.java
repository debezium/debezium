/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.JMX;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Percentage;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.MongoDbConnectorConfig.SnapshotMode;
import io.debezium.engine.DebeziumEngine;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.notification.AbstractNotificationsIT;
import io.debezium.pipeline.notification.Notification;
import io.debezium.pipeline.notification.channels.SinkNotificationChannel;
import io.debezium.pipeline.notification.channels.jmx.JmxNotificationChannelMXBean;

/**
 * Test to verify notifications for MongoDB.
 *
 * @author Mario Fiore Vitale
 */
public class NotificationsIT extends AbstractMongoConnectorIT {

    protected static final int ROW_COUNT = 1_000;

    private static final String DATABASE_NAME = "dbA";
    private static final String COLLECTION_NAME = "c1";
    private static final String FULL_COLLECTION_NAME = DATABASE_NAME + "." + COLLECTION_NAME;

    @Before
    public void before() {
        // Set up the replication context for connections ...
        context = new MongoDbTaskContext(config().build());

        TestHelper.cleanDatabase(mongo, DATABASE_NAME);
    }

    @After
    public void after() {
        TestHelper.cleanDatabase(mongo, DATABASE_NAME);
    }

    protected Class<MongoDbConnector> connectorClass() {
        return MongoDbConnector.class;
    }

    protected Configuration.Builder config() {
        return TestHelper.getConfiguration(mongo)
                .edit()
                .with(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST, DATABASE_NAME)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbA.c1,dbA.c2")
                .with(MongoDbConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 10)
                .with(CommonConnectorConfig.SNAPSHOT_MODE_TABLES, "dbA.c1,dbA.c2")
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL);
    }

    protected String fullDataCollectionName() {
        return FULL_COLLECTION_NAME;
    }

    protected void startConnector(Function<Configuration.Builder, Configuration.Builder> custConfig) {
        startConnector(custConfig, loggingCompletion());
    }

    protected void startConnector(Function<Configuration.Builder, Configuration.Builder> custConfig, DebeziumEngine.CompletionCallback callback) {
        final Configuration config = custConfig.apply(config()).build();
        start(connectorClass(), config, callback);

        waitForAvailableRecords(5, TimeUnit.SECONDS);
    }

    @Test
    public void notificationCorrectlySentOnItsTopic() {
        // Testing.Print.enable();

        storeDocuments("dbA", "c1", "simple_objects.json");
        storeDocuments("dbA", "c2", "simple_objects.json");

        startConnector(config -> config
                .with(SinkNotificationChannel.NOTIFICATION_TOPIC, "io.debezium.notification")
                .with(CommonConnectorConfig.NOTIFICATION_ENABLED_CHANNELS, "sink"));

        assertConnectorIsRunning();

        waitForAvailableRecords(500, TimeUnit.MILLISECONDS);

        List<SourceRecord> notifications = new ArrayList<>();

        Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {

            consumeAvailableRecords(r -> {
                if (r.topic().equals("io.debezium.notification")) {
                    notifications.add(r);
                }
            });
            return notifications.size() == 6;
        });

        assertThat(notifications).hasSize(6);
        SourceRecord sourceRecord = notifications.get(0);
        Assertions.assertThat(sourceRecord.topic()).isEqualTo("io.debezium.notification");
        Assertions.assertThat(((Struct) sourceRecord.value()).getString("aggregate_type")).isEqualTo("Initial Snapshot");
        Assertions.assertThat(((Struct) sourceRecord.value()).getString("type")).isEqualTo("STARTED");
        Assertions.assertThat(((Struct) sourceRecord.value()).getInt64("timestamp")).isCloseTo(Instant.now().toEpochMilli(), Percentage.withPercentage(1));

        assertTableNotificationsSentToTopic(notifications, "dbA.c1");
        assertTableNotificationsSentToTopic(notifications, "dbA.c2");

        sourceRecord = notifications.get(notifications.size() - 1);
        Assertions.assertThat(sourceRecord.topic()).isEqualTo("io.debezium.notification");
        Assertions.assertThat(((Struct) sourceRecord.value()).getString("aggregate_type")).isEqualTo("Initial Snapshot");
        Assertions.assertThat(((Struct) sourceRecord.value()).getString("type")).isEqualTo("COMPLETED");
        Assertions.assertThat(((Struct) sourceRecord.value()).getInt64("timestamp")).isCloseTo(Instant.now().toEpochMilli(), Percentage.withPercentage(1));
    }

    @Test
    public void notificationNotSentIfNoChannelIsConfigured() {
        // Testing.Print.enable();

        startConnector(config -> config.with(SinkNotificationChannel.NOTIFICATION_TOPIC, "io.debezium.notification"));
        assertConnectorIsRunning();

        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);

        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();
    }

    @Test
    public void reportErrorWhenSinkChannelIsEnabledAndNoTopicConfigurationProvided() {
        // Testing.Print.enable();

        LogInterceptor logInterceptor = new LogInterceptor("io.debezium.connector");
        startConnector(config -> config
                .with(CommonConnectorConfig.NOTIFICATION_ENABLED_CHANNELS, "sink"));

        Assertions.assertThat(logInterceptor.containsErrorMessage(
                "Connector configuration is not valid. The 'notification.sink.topic.name' value is invalid: Notification topic name must be provided when kafka notification channel is enabled"))
                .isTrue();
    }

    @Test
    public void notificationCorrectlySentOnJmx()
            throws ReflectionException, MalformedObjectNameException, InstanceNotFoundException, IntrospectionException, AttributeNotFoundException,
            MBeanException {

        // Testing.Print.enable();

        storeDocuments("dbA", "c1", "simple_objects.json");
        storeDocuments("dbA", "c2", "simple_objects.json");

        startConnector(config -> config
                .with(CommonConnectorConfig.NOTIFICATION_ENABLED_CHANNELS, "jmx"));

        assertConnectorIsRunning();

        // waitForSnapshotToBeCompleted("mongodb", "mongo1", "0", null);

        Awaitility.await().atMost(30, TimeUnit.SECONDS)
                .pollDelay(1, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> !readNotificationFromJmx().isEmpty());

        List<Notification> notifications = readNotificationFromJmx();

        notifications
                .forEach(notification -> System.out.println("[notificationCorrectlySentOnJmx]:" + notification.toString()));

        assertThat(notifications).hasSize(6);
        assertThat(notifications.get(0))
                .hasFieldOrPropertyWithValue("aggregateType", "Initial Snapshot")
                .hasFieldOrPropertyWithValue("type", "STARTED")
                .hasFieldOrProperty("timestamp");

        assertTableNotificationsSentToJmx(notifications, "dbA.c1");
        assertTableNotificationsSentToJmx(notifications, "dbA.c2");

        assertThat(notifications.get(notifications.size() - 1))
                .hasFieldOrPropertyWithValue("aggregateType", "Initial Snapshot")
                .hasFieldOrPropertyWithValue("type", "COMPLETED")
                .hasFieldOrProperty("timestamp");

        resetNotifications();

        notifications = readNotificationFromJmx();
        assertThat(notifications).hasSize(0);
    }

    @Test
    public void emittingDebeziumNotificationWillGenerateAJmxNotification()
            throws ReflectionException, MalformedObjectNameException, InstanceNotFoundException, IntrospectionException, AttributeNotFoundException,
            MBeanException, InterruptedException, JsonProcessingException {

        // Testing.Print.enable();
        ObjectMapper mapper = new ObjectMapper();

        startConnector(config -> config
                .with(CommonConnectorConfig.SNAPSHOT_DELAY_MS, 10000)
                .with(CommonConnectorConfig.NOTIFICATION_ENABLED_CHANNELS, "jmx"));

        List<javax.management.Notification> jmxNotifications = registerJmxNotificationListener();

        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted("mongodb", "mongo1", "0", null);

        MBeanNotificationInfo[] notifications = readJmxNotifications();

        assertThat(notifications).allSatisfy(mBeanNotificationInfo -> assertThat(mBeanNotificationInfo.getName()).isEqualTo(Notification.class.getName()));

        assertThat(jmxNotifications).hasSize(2);
        assertThat(jmxNotifications.get(0)).hasFieldOrPropertyWithValue("message", "Initial Snapshot generated a notification");

        Notification notification = mapper.readValue(jmxNotifications.get(0).getUserData().toString(), Notification.class);
        assertThat(notification)
                .hasFieldOrPropertyWithValue("aggregateType", "Initial Snapshot")
                .hasFieldOrPropertyWithValue("type", "STARTED")
                .hasFieldOrPropertyWithValue("additionalData", Map.of("connector_name", "mongo1"));
        assertThat(notification.getTimestamp()).isCloseTo(Instant.now().toEpochMilli(), Percentage.withPercentage(1));

        assertThat(jmxNotifications.get(1)).hasFieldOrPropertyWithValue("message", "Initial Snapshot generated a notification");
        notification = mapper.readValue(jmxNotifications.get(1).getUserData().toString(), Notification.class);
        assertThat(notification)
                .hasFieldOrPropertyWithValue("aggregateType", "Initial Snapshot")
                .hasFieldOrPropertyWithValue("type", "COMPLETED")
                .hasFieldOrPropertyWithValue("additionalData", Map.of("connector_name", "mongo1"));
        assertThat(notification.getTimestamp()).isCloseTo(Instant.now().toEpochMilli(), Percentage.withPercentage(1));
    }

    private void assertTableNotificationsSentToJmx(List<Notification> notifications, String tableName) {
        Optional<Notification> tableNotification;
        tableNotification = notifications.stream()
                .filter(v -> v.getType().equals("IN_PROGRESS") && v.getAdditionalData().containsValue(tableName))
                .findAny();
        assertThat(tableNotification.isPresent()).isTrue();
        assertThat(tableNotification.get().getAggregateType()).isEqualTo("Initial Snapshot");
        assertThat(tableNotification.get().getTimestamp()).isCloseTo(Instant.now().toEpochMilli(), Percentage.withPercentage(1));

        tableNotification = notifications.stream()
                .filter(v -> v.getType().equals("TABLE_SCAN_COMPLETED") && v.getAdditionalData().containsValue(tableName))
                .findAny();
        assertThat(tableNotification.isPresent()).isTrue();
        assertThat(tableNotification.get().getAggregateType()).isEqualTo("Initial Snapshot");
        assertThat(tableNotification.get().getTimestamp()).isCloseTo(Instant.now().toEpochMilli(), Percentage.withPercentage(1));

    }

    private void assertTableNotificationsSentToTopic(List<SourceRecord> notifications, String tableName) {
        Optional<Struct> tableNotification;
        tableNotification = notifications.stream()
                .map(s -> ((Struct) s.value()))
                .filter(v -> v.getString("type").equals("IN_PROGRESS") && v.getMap("additional_data").containsValue(tableName))
                .findAny();
        assertThat(tableNotification.isPresent()).isTrue();
        assertThat(tableNotification.get().getString("aggregate_type")).isEqualTo("Initial Snapshot");
        assertThat(tableNotification.get().getInt64("timestamp")).isCloseTo(Instant.now().toEpochMilli(), Percentage.withPercentage(1));

        tableNotification = notifications.stream()
                .map(s -> ((Struct) s.value()))
                .filter(v -> v.getString("type").equals("TABLE_SCAN_COMPLETED") && v.getMap("additional_data").containsValue(tableName))
                .findAny();
        assertThat(tableNotification.isPresent()).isTrue();
        assertThat(tableNotification.get().getString("aggregate_type")).isEqualTo("Initial Snapshot");
        assertThat(tableNotification.get().getInt64("timestamp")).isCloseTo(Instant.now().toEpochMilli(), Percentage.withPercentage(1));
    }

    private List<Notification> readNotificationFromJmx()
            throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException, IntrospectionException, AttributeNotFoundException, MBeanException {

        ObjectName notificationBean = getObjectName();
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        MBeanInfo mBeanInfo = server.getMBeanInfo(notificationBean);

        List<String> attributesNames = Arrays.stream(mBeanInfo.getAttributes()).map(MBeanAttributeInfo::getName).collect(Collectors.toList());
        assertThat(attributesNames).contains("Notifications");

        JmxNotificationChannelMXBean proxy = JMX.newMXBeanProxy(
                server,
                notificationBean,
                JmxNotificationChannelMXBean.class);

        return proxy.getNotifications();
    }

    private MBeanNotificationInfo[] readJmxNotifications()
            throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException, IntrospectionException, AttributeNotFoundException, MBeanException {

        ObjectName notificationBean = getObjectName();
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        MBeanInfo mBeanInfo = server.getMBeanInfo(notificationBean);

        return mBeanInfo.getNotifications();
    }

    private ObjectName getObjectName() throws MalformedObjectNameException {

        return new ObjectName(String.format("debezium.%s:type=management,context=notifications,server=%s", "mongodb", "mongo1"));
    }

    private List<javax.management.Notification> registerJmxNotificationListener()
            throws MalformedObjectNameException, InstanceNotFoundException {

        ObjectName notificationBean = getObjectName();
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        List<javax.management.Notification> receivedNotifications = new ArrayList<>();
        server.addNotificationListener(notificationBean, new AbstractNotificationsIT.ClientListener(), null, receivedNotifications);

        System.out.println("Added listener");
        return receivedNotifications;
    }

    private void resetNotifications()
            throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException, MBeanException {

        ObjectName notificationBean = getObjectName();
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        server.invoke(notificationBean, "reset", new Object[]{}, new String[]{});

    }

    public static class ClientListener implements NotificationListener {

        @Override
        public void handleNotification(javax.management.Notification notification, Object handback) {

            ((List<javax.management.Notification>) handback).add(notification);
        }
    }

    @Override
    protected int getMaximumEnqueuedRecordCount() {
        return ROW_COUNT * 3;
    }
}
