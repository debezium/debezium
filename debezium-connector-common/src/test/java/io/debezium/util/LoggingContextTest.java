/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import io.debezium.pipeline.spi.Partition;
import io.debezium.util.LoggingContext.RootLoggingContext;

class LoggingContextTest {
    private ExecutorService executorService;

    @BeforeEach
    void setup() {
        executorService = Executors.newSingleThreadExecutor();
        MDC.clear();
    }

    @Test
    void shouldCreateLoggingContextForConnector() {
        // given
        final var connectorType = "type";
        final var connectorName = "connector";
        final var contextName = "test";

        // when
        final var previousContext = LoggingContext.forConnector(connectorType, connectorName, contextName);
        final var context = MDC.getCopyOfContextMap();
        previousContext.restore();

        // then
        assertThat(context).isEqualTo(Map.of(
                LoggingContext.CONNECTOR_TYPE, connectorType,
                LoggingContext.CONNECTOR_NAME, connectorName,
                LoggingContext.CONNECTOR_CONTEXT, contextName));
        assertThat(MDC.getCopyOfContextMap()).isEmpty();
    }

    @Test
    void shouldCreateLoggingContextForConnectorIncludingRootContext() {
        // given
        final var rootContextTag = "connector.context";
        final var rootContextValue = "[foo-connector|0]";
        MDC.put(rootContextTag, rootContextValue);

        final var connectorType = "sink";
        final var connectorName = "foo";
        final var taskId = "5";
        final var contextName = "root";
        final var partition = mock(Partition.class);
        doReturn(Map.of("foo", "bar", "bazz", "wazz")).when(partition).getLoggingContext();

        // when
        final var previousContext = LoggingContext.forConnector(connectorType, connectorName, taskId, contextName, partition);
        final var context = MDC.getCopyOfContextMap();
        previousContext.restore();

        // then
        assertThat(context).isEqualTo(Map.of(
                rootContextTag, rootContextValue,
                LoggingContext.CONNECTOR_TYPE, connectorType,
                LoggingContext.CONNECTOR_NAME, connectorName,
                LoggingContext.TASK_ID, taskId,
                LoggingContext.CONNECTOR_CONTEXT, contextName,
                "foo", "bar",
                "bazz", "wazz"));
        assertThat(MDC.getCopyOfContextMap()).isEqualTo(Map.of(rootContextTag, rootContextValue));
    }

    @Test
    void shouldCreateLoggingContextForOperation() {
        // given
        final var rootContextTag = "root";
        final var rootContextValue = "rootValue";
        MDC.put(rootContextTag, rootContextValue);

        final var connectorType = "source";
        final var connectorName = "temp-connector";
        final var contextName = "temporary";
        final var assertions = new SoftAssertions();

        // when
        LoggingContext.temporarilyForConnector(connectorType, connectorName, contextName, () -> {
            // then
            assertions.assertThat(MDC.getCopyOfContextMap()).isEqualTo(Map.of(
                    rootContextTag, rootContextValue,
                    LoggingContext.CONNECTOR_TYPE, connectorType,
                    LoggingContext.CONNECTOR_NAME, connectorName,
                    LoggingContext.CONNECTOR_CONTEXT, contextName));
        });
        assertions.assertThat(MDC.getCopyOfContextMap()).isEqualTo(Map.of(rootContextTag, rootContextValue));
        assertions.assertAll();
    }

    @Test
    void shouldInheritRootMdcContextInNewThread() {
        // given
        final var rootContextTag = "connector.context";
        final var rootContextValue = "[foo-connector|0]";
        MDC.put(rootContextTag, rootContextValue);
        final var assertions = new SoftAssertions();

        // when
        try (var rootLoggingContext = LoggingContext.initRootContext()) {
            final var future = CompletableFuture.runAsync(() -> {
                final var previousContext = LoggingContext.forConnector("foo", "bar", "bazz");
                final var context = MDC.getCopyOfContextMap();
                previousContext.restore();

                // then
                assertions.assertThat(context).isEqualTo(Map.of(
                        rootContextTag, rootContextValue,
                        LoggingContext.CONNECTOR_TYPE, "foo",
                        LoggingContext.CONNECTOR_NAME, "bar",
                        LoggingContext.CONNECTOR_CONTEXT, "bazz"));
                assertions.assertThat(MDC.getCopyOfContextMap()).isEqualTo(Collections.emptyMap());
            }, executorService);
            assertThat(future).succeedsWithin(10L, SECONDS);
            assertions.assertThat(MDC.getCopyOfContextMap()).isEqualTo(Map.of(rootContextTag, rootContextValue));
            assertions.assertAll();
        }
    }

    @Test
    void shouldRestorePreviousMdcContextExcludingRoot() {
        // given
        final var rootContextTag = "connector.context";
        final var rootContextValue = "[foo-connector|0]";
        MDC.put(rootContextTag, rootContextValue);
        final var assertions = new SoftAssertions();

        // when
        try (var rootLoggingContext = LoggingContext.initRootContext()) {
            final var future = CompletableFuture.runAsync(() -> {
                MDC.put("old", "bag");
                final var previousContext = LoggingContext.forConnector("foo", "bar", "bazz");
                final var context = MDC.getCopyOfContextMap();
                previousContext.restore();

                // then
                assertions.assertThat(context).isEqualTo(Map.of(
                        rootContextTag, rootContextValue,
                        LoggingContext.CONNECTOR_TYPE, "foo",
                        LoggingContext.CONNECTOR_NAME, "bar",
                        LoggingContext.CONNECTOR_CONTEXT, "bazz"));
                assertions.assertThat(MDC.getCopyOfContextMap()).isEqualTo(Map.of("old", "bag"));
            }, executorService);
            assertThat(future).succeedsWithin(10L, SECONDS);
            assertions.assertThat(MDC.getCopyOfContextMap()).isEqualTo(Map.of(rootContextTag, rootContextValue));
            assertions.assertAll();
        }
    }

    @Test
    void shouldInitRootContext() {
        // given
        MDC.setContextMap(Map.of("foo", "bar", "bazz", "wazz"));

        // when
        try (var rootLoggingContext = LoggingContext.initRootContext()) {

            // then
            assertThat(RootLoggingContext.valueCopy()).isEqualTo(Map.of("foo", "bar", "bazz", "wazz"));
        }
        assertThat(RootLoggingContext.valueCopy()).isEmpty();
    }

    @Test
    void shouldCreateNestedRootContext() {
        // given
        MDC.put("A", "1");
        try (var ctx1 = LoggingContext.initRootContext()) {
            MDC.put("B", "2");
            assertThat(RootLoggingContext.valueCopy()).isEqualTo(Map.of("A", "1"));
            try (var ctx2 = LoggingContext.initRootContext()) {
                MDC.put("C", "3");
                assertThat(RootLoggingContext.valueCopy()).isEqualTo(Map.of("A", "1", "B", "2"));
                try (var ctx3 = LoggingContext.initRootContext()) {
                    assertThat(RootLoggingContext.valueCopy()).isEqualTo(Map.of("A", "1", "B", "2", "C", "3"));
                }
                assertThat(RootLoggingContext.valueCopy()).isEqualTo(Map.of("A", "1", "B", "2"));
            }
            assertThat(RootLoggingContext.valueCopy()).isEqualTo(Map.of("A", "1"));
        }
        assertThat(RootLoggingContext.valueCopy()).isEmpty();
    }

    @Test
    void shouldCreateLoggingContextForConnectorWithEmptyRootAndPreviousContext() {
        // given
        final var connectorType = "type";
        final var connectorName = "connector";
        final var contextName = "test";
        MDC.setContextMap(Collections.emptyMap());

        // when
        try (var rootLoggingContext = LoggingContext.initRootContext()) {
            MDC.clear();
            final var previousContext = LoggingContext.forConnector(connectorType, connectorName, contextName);
            final var context = MDC.getCopyOfContextMap();
            previousContext.restore();

            // then
            assertThat(context).isEqualTo(Map.of(
                    LoggingContext.CONNECTOR_TYPE, connectorType,
                    LoggingContext.CONNECTOR_NAME, connectorName,
                    LoggingContext.CONNECTOR_CONTEXT, contextName));
        }
        assertThat(MDC.getCopyOfContextMap()).isEmpty();
    }

    @AfterEach
    void cleanup() {
        executorService.shutdown();
        MDC.clear();
    }
}
