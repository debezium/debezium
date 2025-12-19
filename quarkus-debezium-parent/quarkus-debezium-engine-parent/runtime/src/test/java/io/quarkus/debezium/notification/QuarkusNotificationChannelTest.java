/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.notification;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.stream.Stream;

import jakarta.enterprise.inject.Instance;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.pipeline.notification.Notification;

class QuarkusNotificationChannelTest {

    private final Instance<NotificationHandler> notificationHandlers = Mockito.mock(Instance.class);
    private final NotificationHandler avaiableNotificationHandler = Mockito.mock(NotificationHandler.class);
    private final NotificationHandler notAvailablenotificationHandler = Mockito.mock(NotificationHandler.class);

    @Test
    @DisplayName("should handle the event only when the aggregateType is available for the handler")
    void shouldHandleOnlyWhenIsAvailableForAggregateType() {
        when(notificationHandlers.stream()).thenReturn(Stream.of(avaiableNotificationHandler, notAvailablenotificationHandler));
        when(avaiableNotificationHandler.isAvailable("anAggregateType")).thenReturn(true);

        QuarkusNotificationChannel underTest = new QuarkusNotificationChannel(notificationHandlers);

        underTest.send(new Notification("id", "anAggregateType", "aType", Map.of("aKey", "aValue"), 1L));
        verify(avaiableNotificationHandler, times(1)).handle(new Notification("id", "anAggregateType", "aType", Map.of("aKey", "aValue"), 1L));
        verify(notAvailablenotificationHandler, times(0)).handle(new Notification("id", "anAggregateType", "aType", Map.of("aKey", "aValue"), 1L));
    }
}
