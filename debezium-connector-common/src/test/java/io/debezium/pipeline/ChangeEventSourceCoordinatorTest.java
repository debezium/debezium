/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.debezium.annotation.ConnectorSpecific;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.common.BaseSourceConnector;
import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.signal.actions.SignalAction;
import io.debezium.pipeline.signal.actions.SignalActionProvider;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.spi.Partition;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.snapshot.Snapshotter;

public class ChangeEventSourceCoordinatorTest {

    SnapshotterService snapshotterService;
    Snapshotter snapshotter;
    CommonConnectorConfig connectorConfig;
    ChangeEventSourceCoordinator coordinator;
    ChangeEventSource.ChangeEventSourceContext context;

    @BeforeEach
    public void before() {
        snapshotterService = mock(SnapshotterService.class);
        snapshotter = mock(Snapshotter.class);
        connectorConfig = mock(CommonConnectorConfig.class);
        when(connectorConfig.getLogicalName()).thenReturn("DummyConnector");
        coordinator = new ChangeEventSourceCoordinator(null, null, SourceConnector.class, connectorConfig, null,
                null, null, null, null, null, snapshotterService);
        context = mock(ChangeEventSource.ChangeEventSourceContext.class);
    }

    @Test
    public void testNotDelayStreamingIfSnapshotShouldNotStream() throws Exception {
        when(snapshotterService.getSnapshotter()).thenReturn(snapshotter);
        when(snapshotter.shouldStream()).thenReturn(false);

        coordinator.delayStreamingIfNeeded(context);

        verify(connectorConfig, never()).getStreamingDelay();
    }

    @Test
    public void testDelayStreamingIfSnapshotShouldStream() throws Exception {
        when(snapshotterService.getSnapshotter()).thenReturn(snapshotter);
        when(snapshotter.shouldStream()).thenReturn(true);
        when(connectorConfig.getStreamingDelay()).thenReturn(Duration.of(1, ChronoUnit.SECONDS));
        when(context.isRunning()).thenReturn(true);

        coordinator.delayStreamingIfNeeded(context);

        verify(connectorConfig, times(1)).getStreamingDelay();
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testRegisterSignalActionsWithConnectorSpecificAnnotation() {
        // Given: Two SignalActionProvider implementations with the same action key
        // but different @ConnectorSpecific annotations
        SignalProcessor signalProcessor = mock(SignalProcessor.class);
        EventDispatcher eventDispatcher = mock(EventDispatcher.class);

        CommonConnectorConfig connectorConfig = mock(CommonConnectorConfig.class);
        Configuration configuration = mock(Configuration.class);
        when(connectorConfig.getConfig()).thenReturn(configuration);
        when(configuration.getString("connector.class")).thenReturn(TestConnectorA.class.getName());

        // Create a real coordinator instance to call the method
        ChangeEventSourceCoordinator realCoordinator = new ChangeEventSourceCoordinator(
                null, null, SourceConnector.class, connectorConfig, null, null, null, null, null, null, null);

        // When: registerSignalActionsAndStartProcessor is called
        realCoordinator.registerSignalActionsAndStartProcessor(signalProcessor, eventDispatcher, realCoordinator, connectorConfig);

        // Then: Signal actions from the provider matching TestConnectorA and universal providers should be registered
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<SignalAction> actionCaptor = ArgumentCaptor.forClass(SignalAction.class);

        // Verify multiple actions are registered (connector-specific + universal providers including StandardActionProvider)
        verify(signalProcessor, atLeast(4)).registerSignalAction(keyCaptor.capture(), actionCaptor.capture());
        verify(signalProcessor, times(1)).start();

        // Verify that the connector-specific action from TestSignalActionProviderA is registered
        assertThat(keyCaptor.getAllValues()).contains("test-action");
        // Also verify universal provider actions are present (StandardActionProvider)
        assertThat(keyCaptor.getAllValues()).contains("log");
        assertThat(actionCaptor.getAllValues().stream()
                .anyMatch(action -> action instanceof TestSignalActionA)).isTrue();
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testRegisterSignalActionsSelectsCorrectProviderForConnectorB() {
        // Given: Configuration for TestConnectorB
        SignalProcessor signalProcessor = mock(SignalProcessor.class);
        EventDispatcher eventDispatcher = mock(EventDispatcher.class);

        CommonConnectorConfig connectorConfig = mock(CommonConnectorConfig.class);
        Configuration configuration = mock(Configuration.class);
        when(connectorConfig.getConfig()).thenReturn(configuration);
        when(configuration.getString("connector.class")).thenReturn(TestConnectorB.class.getName());

        // Create a real coordinator instance to call the method
        ChangeEventSourceCoordinator realCoordinator = new ChangeEventSourceCoordinator(
                null, null, SourceConnector.class, connectorConfig, null, null, null, null, null, null, null);

        // When: registerSignalActionsAndStartProcessor is called
        realCoordinator.registerSignalActionsAndStartProcessor(signalProcessor, eventDispatcher, realCoordinator, connectorConfig);

        // Then: Signal actions from the provider matching TestConnectorB and universal providers should be registered
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<SignalAction> actionCaptor = ArgumentCaptor.forClass(SignalAction.class);

        // Verify multiple actions are registered (connector-specific + universal providers including StandardActionProvider)
        verify(signalProcessor, atLeast(4)).registerSignalAction(keyCaptor.capture(), actionCaptor.capture());
        verify(signalProcessor, times(1)).start();

        // Verify that the connector-specific action from TestSignalActionProviderB is registered
        assertThat(keyCaptor.getAllValues()).contains("test-action");
        // Also verify universal provider actions are present (StandardActionProvider)
        assertThat(keyCaptor.getAllValues()).contains("log");
        assertThat(actionCaptor.getAllValues().stream()
                .anyMatch(action -> action instanceof TestSignalActionB)).isTrue();
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testRegisterSignalActionsWithUniversalProviderWithoutConnectorSpecific() {
        // Given: A connector without specific providers - should load universal providers
        SignalProcessor signalProcessor = mock(SignalProcessor.class);
        EventDispatcher eventDispatcher = mock(EventDispatcher.class);

        CommonConnectorConfig connectorConfig = mock(CommonConnectorConfig.class);
        Configuration configuration = mock(Configuration.class);
        when(connectorConfig.getConfig()).thenReturn(configuration);
        // Use TestConnectorD which doesn't have a specific provider
        when(configuration.getString("connector.class")).thenReturn(TestConnectorD.class.getName());

        // Create a real coordinator instance to call the method
        ChangeEventSourceCoordinator realCoordinator = new ChangeEventSourceCoordinator(
                null, null, SourceConnector.class, connectorConfig, null, null, null, null, null, null, null);

        // When: registerSignalActionsAndStartProcessor is called
        realCoordinator.registerSignalActionsAndStartProcessor(signalProcessor, eventDispatcher, realCoordinator, connectorConfig);

        // Then: Universal providers' actions should be registered
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<SignalAction> actionCaptor = ArgumentCaptor.forClass(SignalAction.class);

        // Verify multiple actions are registered (universal providers including StandardActionProvider)
        verify(signalProcessor, atLeast(3)).registerSignalAction(keyCaptor.capture(), actionCaptor.capture());
        verify(signalProcessor, times(1)).start();

        // Verify that StandardActionProvider (a universal provider) actions are registered
        // StandardActionProvider provides actions like "log", "execute-snapshot", etc.
        assertThat(keyCaptor.getAllValues()).contains("log", "execute-snapshot");
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testRegisterSignalActionsFailsWithDuplicateKeysWithoutConnectorSpecific() {
        // Given: Two providers WITHOUT @ConnectorSpecific annotation returning the same key
        SignalProcessor signalProcessor = mock(SignalProcessor.class);
        EventDispatcher eventDispatcher = mock(EventDispatcher.class);

        CommonConnectorConfig connectorConfig = mock(CommonConnectorConfig.class);
        Configuration configuration = mock(Configuration.class);
        when(connectorConfig.getConfig()).thenReturn(configuration);
        // Use TestConnectorC - providers C and D have no @ConnectorSpecific annotation
        when(configuration.getString("connector.class")).thenReturn(TestConnectorC.class.getName());

        // Create a real coordinator instance to call the method
        ChangeEventSourceCoordinator realCoordinator = new ChangeEventSourceCoordinator(
                null, null, SourceConnector.class, connectorConfig, null, null, null, null, null, null, null);

        // When/Then: registerSignalActionsAndStartProcessor should throw IllegalStateException
        // due to duplicate keys when both non-annotated providers are loaded
        assertThatThrownBy(() -> realCoordinator.registerSignalActionsAndStartProcessor(
                signalProcessor, eventDispatcher, realCoordinator, connectorConfig))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Duplicate key");
    }

    // Test connector classes
    private static class TestConnectorA extends BaseSourceConnector {
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

    private static class TestConnectorB extends BaseSourceConnector {
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

    private static class TestConnectorC extends BaseSourceConnector {
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

    private static class TestConnectorD extends BaseSourceConnector {
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

    // Test SignalActionProvider implementations with @ConnectorSpecific annotation
    @ConnectorSpecific(connector = TestConnectorA.class)
    public static class TestSignalActionProviderA implements SignalActionProvider {
        @Override
        public <P extends Partition> Map<String, SignalAction<P>> createActions(
                                                                                EventDispatcher<P, ? extends DataCollectionId> dispatcher,
                                                                                ChangeEventSourceCoordinator<P, ?> changeEventSourceCoordinator,
                                                                                CommonConnectorConfig connectorConfig) {

            Map<String, SignalAction<P>> actions = new HashMap<>();
            actions.put("test-action", new TestSignalActionA<>());
            return actions;
        }
    }

    @ConnectorSpecific(connector = TestConnectorB.class)
    public static class TestSignalActionProviderB implements SignalActionProvider {
        @Override
        public <P extends Partition> Map<String, SignalAction<P>> createActions(
                                                                                EventDispatcher<P, ? extends DataCollectionId> dispatcher,
                                                                                ChangeEventSourceCoordinator<P, ?> changeEventSourceCoordinator,
                                                                                CommonConnectorConfig connectorConfig) {

            Map<String, SignalAction<P>> actions = new HashMap<>();
            actions.put("test-action", new TestSignalActionB<>());
            return actions;
        }
    }

    // Test SignalActionProvider implementations WITHOUT @ConnectorSpecific annotation
    // These return actions with duplicate keys ONLY for TestConnectorC to test the duplicate key scenario
    public static class TestSignalActionProviderC implements SignalActionProvider {
        @Override
        public <P extends Partition> Map<String, SignalAction<P>> createActions(
                                                                                EventDispatcher<P, ? extends DataCollectionId> dispatcher,
                                                                                ChangeEventSourceCoordinator<P, ?> changeEventSourceCoordinator,
                                                                                CommonConnectorConfig connectorConfig) {

            Map<String, SignalAction<P>> actions = new HashMap<>();
            // Only return duplicate action if running with TestConnectorC, otherwise return empty to not interfere
            if (connectorConfig != null && connectorConfig.getConfig() != null
                    && TestConnectorC.class.getName().equals(connectorConfig.getConfig().getString("connector.class"))) {
                actions.put("duplicate-action", new TestSignalActionC<>());
            }
            return actions;
        }
    }

    public static class TestSignalActionProviderD implements SignalActionProvider {
        @Override
        public <P extends Partition> Map<String, SignalAction<P>> createActions(
                                                                                EventDispatcher<P, ? extends DataCollectionId> dispatcher,
                                                                                ChangeEventSourceCoordinator<P, ?> changeEventSourceCoordinator,
                                                                                CommonConnectorConfig connectorConfig) {

            Map<String, SignalAction<P>> actions = new HashMap<>();
            // Only return duplicate action if running with TestConnectorC, otherwise return empty to not interfere
            if (connectorConfig != null && connectorConfig.getConfig() != null
                    && TestConnectorC.class.getName().equals(connectorConfig.getConfig().getString("connector.class"))) {
                actions.put("duplicate-action", new TestSignalActionD<>());
            }
            return actions;
        }
    }

    // Universal provider without @ConnectorSpecific annotation (should be loaded for all connectors)
    // This provider returns an empty map to not interfere with other tests - we rely on StandardActionProvider for testing universal providers
    public static class TestSignalActionProviderE implements SignalActionProvider {
        @Override
        public <P extends Partition> Map<String, SignalAction<P>> createActions(
                                                                                EventDispatcher<P, ? extends DataCollectionId> dispatcher,
                                                                                ChangeEventSourceCoordinator<P, ?> changeEventSourceCoordinator,
                                                                                CommonConnectorConfig connectorConfig) {

            // Return empty map - StandardActionProvider is sufficient for testing universal providers
            return new HashMap<>();
        }
    }

    // Test SignalAction implementations
    public static class TestSignalActionA<P extends Partition> implements SignalAction<P> {
        @Override
        public boolean arrived(SignalPayload<P> signalPayload) {
            return true;
        }
    }

    public static class TestSignalActionB<P extends Partition> implements SignalAction<P> {
        @Override
        public boolean arrived(SignalPayload<P> signalPayload) {
            return true;
        }
    }

    public static class TestSignalActionC<P extends Partition> implements SignalAction<P> {
        @Override
        public boolean arrived(SignalPayload<P> signalPayload) {
            return true;
        }
    }

    public static class TestSignalActionD<P extends Partition> implements SignalAction<P> {
        @Override
        public boolean arrived(SignalPayload<P> signalPayload) {
            return true;
        }
    }

    public static class TestSignalActionE<P extends Partition> implements SignalAction<P> {
        @Override
        public boolean arrived(SignalPayload<P> signalPayload) {
            return true;
        }
    }

}
