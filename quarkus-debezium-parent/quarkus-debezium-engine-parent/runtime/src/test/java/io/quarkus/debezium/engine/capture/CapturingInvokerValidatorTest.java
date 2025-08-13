/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture;

import static java.util.List.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class CapturingInvokerValidatorTest {

    private final CapturingInvokerValidator<TestInvoker> underTest = new CapturingInvokerValidator<>();

    @Test
    @DisplayName("the validation should pass if the destinations are different")
    void shouldBeValidIfAreUnique() {
        assertThat(catchThrowable(() -> underTest.validate(of(
                new TestInvoker("Napoli"),
                new TestInvoker("London"),
                new TestInvoker("Berlin"))))).isNull();
    }

    @Test
    @DisplayName("the validation should not pass if there are multiple invokers with the same destination")
    void shouldBeNotValidIfAreNotUnique() {
        assertThatThrownBy(() -> underTest.validate(of(
                new TestInvoker("Napoli"),
                new TestInvoker("Napoli"),
                new TestInvoker("London"),
                new TestInvoker("Berlin")))).isInstanceOf(IllegalArgumentException.class);
    }

    static class TestInvoker implements CapturingInvoker<String> {
        private final String destination;

        TestInvoker(String destination) {
            this.destination = destination;
        }

        @Override
        public String destination() {
            return destination;
        }

        @Override
        public void capture(String event) {
            // ignore
        }
    }
}
