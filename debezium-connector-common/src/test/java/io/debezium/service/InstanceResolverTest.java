/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

/**
 * Verifies the behavior of the {@link DefaultInstanceResolver}.
 */
class InstanceResolverTest {

    @Test
    void shouldResolveInstanceFromFallbackByDefault() {
        final var instance = new Object();

        assertThat(new DefaultInstanceResolver().resolve(Object.class, "some.key", () -> instance)).isSameAs(instance);
    }

    @Test
    void shouldNotApplyInitializerToFallbackInstanceByDefault() {
        final var initialized = new AtomicBoolean();

        new DefaultInstanceResolver().resolve(Object.class, "some.key", Object::new, instance -> initialized.set(true));

        assertThat(initialized).isFalse();
    }

    @Test
    void shouldResolveAllInstancesFromFallbackByDefault() {
        final var initialized = new AtomicBoolean();
        final List<String> fallback = List.of("one", "two");

        final List<String> instances = new DefaultInstanceResolver().resolveAll(String.class, () -> fallback, instance -> initialized.set(true));

        assertThat(instances).containsExactly("one", "two").isNotSameAs(fallback);
        assertThat(initialized).isFalse();
    }
}
