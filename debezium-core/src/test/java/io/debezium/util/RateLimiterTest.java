/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

public class RateLimiterTest {

    private static final long CAPACITY = 10;
    private static final long REFILL_RATE = 1;

    private RateLimiter rateLimiter;

    @Before
    public void setUp() {
        rateLimiter = new RateLimiter(CAPACITY, REFILL_RATE);
    }

    @Test
    public void testConsume() {
        // Try to consume tokens when bucket is full
        assertThat(rateLimiter.tryConsume(5)).isTrue();
        assertThat(rateLimiter.getTokensAvailable()).isEqualTo(5);

        // Try to consume tokens when bucket is partially full
        assertThat(rateLimiter.tryConsume(3)).isTrue();
        assertThat(rateLimiter.getTokensAvailable()).isEqualTo(2);

        // Try to consume tokens when bucket is empty
        assertThat(rateLimiter.tryConsume(5)).isFalse();
        assertThat(rateLimiter.getTokensAvailable()).isEqualTo(2);
    }

}
