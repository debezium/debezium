/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.annotation.VisibleForTesting;

/**
 * A rate limiter that uses the token bucket algorithm to limit the number of requests over time.
 * The token bucket is filled at a fixed rate and requests are allowed if tokens are available.
 */
@NotThreadSafe
public class RateLimiter {

    private long lastRefillTime; // the time of the last token refill, in milliseconds since epoch
    private long tokensAvailable; // the number of tokens currently in the bucket
    private final long capacity; // the maximum number of tokens that can be stored in the bucket
    private final float refillRate; // the rate at which the bucket is refilled, in tokens per second

    /**
     * Constructs a new rate limiter with the given capacity and refill rate.
     *
     * @param capacity   the maximum number of tokens that can be stored in the bucket
     * @param refillRate the rate at which the bucket is refilled, in tokens per second
     */
    public RateLimiter(long capacity, float refillRate) {
        this.capacity = capacity;
        this.refillRate = refillRate;
        this.tokensAvailable = capacity;
        this.lastRefillTime = System.currentTimeMillis();
    }

    /**
     * Attempts to consume the given number of tokens from the bucket.
     *
     * @param tokens the number of tokens to consume
     * @return true if the tokens are available and can be consumed, false otherwise
     */
    public boolean tryConsume(long tokens) {
        refillTokens();

        if (tokensAvailable >= tokens) {
            tokensAvailable -= tokens;
            return true;
        }
        else {
            return false;
        }
    }

    @VisibleForTesting
    long getTokensAvailable() {
        return tokensAvailable;
    }

    private void refillTokens() {
        long now = System.currentTimeMillis();
        long timeElapsed = now - lastRefillTime; // the time elapsed since the last token refill, in milliseconds
        long tokensToAdd = (long) (timeElapsed * refillRate / 1000); // the number of tokens to add based on the elapsed time and refill rate
        tokensAvailable = Math.min(tokensAvailable + tokensToAdd, capacity); // ensure the number of tokens doesn't exceed the capacity
        lastRefillTime = now; // update the last refill time to the current time
    }

}
