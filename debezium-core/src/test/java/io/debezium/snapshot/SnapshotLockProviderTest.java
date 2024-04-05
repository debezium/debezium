/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.snapshot;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import io.debezium.DebeziumException;
import io.debezium.annotation.ConnectorSpecific;
import io.debezium.bean.StandardBeanNames;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.connector.common.BaseSourceConnector;
import io.debezium.service.spi.ServiceRegistry;
import io.debezium.snapshot.spi.SnapshotLock;
import io.debezium.spi.schema.DataCollectionId;

@RunWith(MockitoJUnitRunner.class)
public class SnapshotLockProviderTest {

    @Test
    public void whenBothImplementationHasConnectorSpecificAnnotationTheRightOneWillBeSelected() {

        SnapshotLockProvider snapshotLockProvider = new SnapshotLockProvider(
                List.of(new SnapshotLockAnnotatedCustomA(), new SnapshotLockAnnotatedCustomB()));

        MockedObjects mockedObjects = getMockedObjects("myLock", MyConnectorA.class.getName());

        SnapshotLock service = snapshotLockProvider.createService(mockedObjects.configuration, mockedObjects.serviceRegistry);

        Assertions.assertThat(service.getClass().getName()).isEqualTo(SnapshotLockAnnotatedCustomA.class.getName());

    }

    @Test
    public void whenOneImplementationHasConnectorSpecificAnnotationThatMatchTheRunningConnectorItWillBeSelected() {

        SnapshotLockProvider snapshotLockProvider = new SnapshotLockProvider(
                List.of(new SnapshotLockCustomA(), new SnapshotLockAnnotatedCustomB()));

        MockedObjects mockedObjects = getMockedObjects("myLock", MyConnectorB.class.getName());

        SnapshotLock service = snapshotLockProvider.createService(mockedObjects.configuration, mockedObjects.serviceRegistry);

        Assertions.assertThat(service.getClass().getName()).isEqualTo(SnapshotLockAnnotatedCustomB.class.getName());

    }

    @Test
    public void whenNoImplementationHasConnectorSpecificAnnotationThenTheFirstNotAnnotatedOneWillBeSelected() {

        SnapshotLockProvider snapshotLockProvider = new SnapshotLockProvider(
                List.of(new SnapshotLockCustomA(), new SnapshotLockCustomB()));

        MockedObjects mockedObjects = getMockedObjects("myLock", MyConnectorB.class.getName());

        SnapshotLock service = snapshotLockProvider.createService(mockedObjects.configuration, mockedObjects.serviceRegistry);

        Assertions.assertThat(service.getClass().getName()).isEqualTo(SnapshotLockCustomA.class.getName());

    }

    @Test
    public void whenNoImplementationIsFoundThenAnExceptionIsThrown() {

        SnapshotLockProvider snapshotLockProvider = new SnapshotLockProvider(
                List.of(new SnapshotLockCustomA(), new SnapshotLockCustomB()));

        MockedObjects mockedObjects = getMockedObjects("notExisting", MyConnectorB.class.getName());

        assertThrows(DebeziumException.class, () -> snapshotLockProvider.createService(mockedObjects.configuration, mockedObjects.serviceRegistry));
    }

    private MockedObjects getMockedObjects(String snapshotMode, String connectorClassName) {

        CommonConnectorConfig commonConnectorConfig = mock(CommonConnectorConfig.class, Mockito.RETURNS_DEEP_STUBS);
        EnumeratedValue enumeratedValue = mock(EnumeratedValue.class);
        Mockito.<Optional<? extends EnumeratedValue>> when(commonConnectorConfig.getSnapshotLockingMode()).thenReturn(Optional.of(enumeratedValue));
        when(commonConnectorConfig.getSnapshotLockingMode().get().getValue()).thenReturn(snapshotMode);
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString("connector.class")).thenReturn(connectorClassName);

        ServiceRegistry serviceRegistry = mock(ServiceRegistry.class);
        BeanRegistry beanRegistry = mock(BeanRegistry.class);
        when(beanRegistry.lookupByName(StandardBeanNames.CONNECTOR_CONFIG, CommonConnectorConfig.class)).thenReturn(commonConnectorConfig);
        when(serviceRegistry.tryGetService(BeanRegistry.class)).thenReturn(beanRegistry);

        return new MockedObjects(configuration, serviceRegistry);
    }

    private static class MockedObjects {
        public final Configuration configuration;
        public final ServiceRegistry serviceRegistry;

        MockedObjects(Configuration configuration, ServiceRegistry serviceRegistry) {
            this.configuration = configuration;
            this.serviceRegistry = serviceRegistry;
        }
    }

    private class MyConnectorA extends BaseSourceConnector {

        @Override
        protected Map<String, ConfigValue> validateAllFields(Configuration config) {
            return null;
        }

        @Override
        public <T extends DataCollectionId> List<T> getMatchingCollections(Configuration config) {
            return null;
        }

        @Override
        public void start(Map<String, String> map) {

        }

        @Override
        public Class<? extends Task> taskClass() {
            return null;
        }

        @Override
        public List<Map<String, String>> taskConfigs(int i) {
            return null;
        }

        @Override
        public void stop() {

        }

        @Override
        public ConfigDef config() {
            return null;
        }

        @Override
        public String version() {
            return null;
        }
    }

    private class MyConnectorB extends BaseSourceConnector {

        @Override
        protected Map<String, ConfigValue> validateAllFields(Configuration config) {
            return null;
        }

        @Override
        public <T extends DataCollectionId> List<T> getMatchingCollections(Configuration config) {
            return null;
        }

        @Override
        public void start(Map<String, String> map) {

        }

        @Override
        public Class<? extends Task> taskClass() {
            return null;
        }

        @Override
        public List<Map<String, String>> taskConfigs(int i) {
            return null;
        }

        @Override
        public void stop() {

        }

        @Override
        public ConfigDef config() {
            return null;
        }

        @Override
        public String version() {
            return null;
        }
    }

    private class SnapshotLockCustomA extends AbstractSnapshotLock {
        @Override
        public String name() {
            return "myLock";
        }
    }

    @ConnectorSpecific(connector = MyConnectorA.class)
    private class SnapshotLockAnnotatedCustomA extends AbstractSnapshotLock {
        @Override
        public String name() {
            return "myLock";
        }
    }

    private class SnapshotLockCustomB extends AbstractSnapshotLock {
        @Override
        public String name() {
            return "myLock";
        }
    }

    @ConnectorSpecific(connector = MyConnectorB.class)
    private class SnapshotLockAnnotatedCustomB extends AbstractSnapshotLock {
        @Override
        public String name() {
            return "myLock";
        }
    }

    private abstract class AbstractSnapshotLock implements SnapshotLock {

        @Override
        public void configure(Map<String, ?> properties) {

        }

        @Override
        public Optional<String> tableLockingStatement(Duration lockTimeout, String tableId) {
            return Optional.empty();
        }
    }
}
