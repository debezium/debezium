/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.notification;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.notification.channels.SinkNotificationChannel;
import io.debezium.pipeline.notification.channels.jmx.JmxNotificationChannelMXBean;

public abstract class AbstractNotificationsIT<T extends SourceConnector> extends AbstractConnectorTest {

    protected abstract Class<T> connectorClass();

    protected abstract Configuration.Builder config();

    protected abstract String connector();

    protected abstract String server();

    protected String task() {
        return null;
    }

    protected String database() {
        return null;
    }

    protected void startConnector(Function<Configuration.Builder, Configuration.Builder> custConfig) {

        final Configuration config = custConfig.apply(config()).build();

        start(connectorClass(), config);
    }

    protected abstract String snapshotStatusResult();

    @Test
    public void notificationCorrectlySentOnItsTopic() {
        // Testing.Print.enable();

        startConnector(config -> config
                .with(SinkNotificationChannel.NOTIFICATION_TOPIC, "io.debezium.notification")
                .with(CommonConnectorConfig.NOTIFICATION_ENABLED_CHANNELS, "sink"));

        assertConnectorIsRunning();

        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);

        List<SourceRecord> notifications = new ArrayList<>();

        Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {

            consumeAvailableRecords(r -> {
                if (r.topic().equals("io.debezium.notification")) {
                    notifications.add(r);
                }
            });
            return notifications.size() == 2;
        });

        assertThat(notifications).hasSize(1);
        SourceRecord sourceRecord = notifications.get(0);
        Assertions.assertThat(sourceRecord.topic()).isEqualTo("io.debezium.notification");
        Assertions.assertThat(((Struct) sourceRecord.value()).getString("aggregate_type")).isEqualTo("Initial Snapshot");
        Assertions.assertThat(((Struct) sourceRecord.value()).getString("type")).isEqualTo("Status " + snapshotStatusResult());
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
            MBeanException, InterruptedException {

        // Testing.Print.enable();

        startConnector(config -> config
                .with(CommonConnectorConfig.NOTIFICATION_ENABLED_CHANNELS, "jmx"));

        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(connector(), server(), task(), database());

        Awaitility.await().atMost(30, TimeUnit.SECONDS)
                .pollDelay(1, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> !readNotificationFromJmx().isEmpty());

        List<Notification> notifications = readNotificationFromJmx();

        assertThat(notifications).hasSize(1);
        assertThat(notifications.get(0))
                .hasFieldOrPropertyWithValue("aggregateType", "Initial Snapshot")
                .hasFieldOrPropertyWithValue("type", "Status " + snapshotStatusResult());

        resetNotifications();

        notifications = readNotificationFromJmx();
        assertThat(notifications).hasSize(0);
    }

    @Test
    public void emittingDebeziumNotificationWillGenerateAJmxNotification()
            throws ReflectionException, MalformedObjectNameException, InstanceNotFoundException, IntrospectionException, AttributeNotFoundException,
            MBeanException, InterruptedException {

        // Testing.Print.enable();

        startConnector(config -> config
                .with(CommonConnectorConfig.SNAPSHOT_DELAY_MS, 2000)
                .with(CommonConnectorConfig.NOTIFICATION_ENABLED_CHANNELS, "jmx"));

        List<javax.management.Notification> jmxNotifications = registerJmxNotificationListener();

        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(connector(), server(), task(), database());

        MBeanNotificationInfo[] notifications = readJmxNotifications();

        assertThat(notifications)
                .hasOnlyOneElementSatisfying(mBeanNotificationInfo -> assertThat(mBeanNotificationInfo.getName()).isEqualTo(Notification.class.getName()));

        assertThat(jmxNotifications).hasSize(1);
        assertThat(jmxNotifications.get(0)).hasFieldOrPropertyWithValue("message", "Initial Snapshot generated a notification");
        assertThat(jmxNotifications.get(0).getUserData())
                .isEqualTo("{aggregateType='Initial Snapshot', type='Status COMPLETED', additionalData=null}");
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

        return new ObjectName(String.format("debezium.%s:type=management, context=notifications, server=%s", connector(), server()));
    }

    private List<javax.management.Notification> registerJmxNotificationListener()
            throws MalformedObjectNameException, InstanceNotFoundException {

        ObjectName notificationBean = getObjectName();
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        List<javax.management.Notification> receivedNotifications = new ArrayList<>();
        server.addNotificationListener(notificationBean, new ClientListener(), null, receivedNotifications);

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
}
