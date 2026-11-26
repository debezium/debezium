/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.systemone;

import java.net.URI;
import java.time.Duration;

import org.apache.kafka.common.config.ConfigException;

/**
 * Validated System One options. The API key is never part of {@link #toString()}.
 *
 * @author Jiri Pechanec
 */
record SystemOneSettings(String provider,
        String apiKey,
        URI endpoint,
        String model,
        boolean longContext,
        int maxScoreLevels,
        Duration requestTimeout,
        Duration connectTimeout,
        int maxAttempts,
        Duration initialDelay,
        Duration maxDelay) {

    static final String ENDPOINT_PATH = "/v1/systemone";

    static URI endpoint(final String baseUrl, final String key) {
        if (baseUrl == null || baseUrl.isBlank()) {
            throw new ConfigException("'" + key + "' must be set to non-empty value.");
        }
        final URI uri;
        try {
            uri = URI.create(baseUrl.trim());
        }
        catch (IllegalArgumentException e) {
            throw new ConfigException("'" + key + "' is not a valid URL.");
        }
        final var scheme = uri.getScheme();
        if (scheme == null || (!scheme.equalsIgnoreCase("http") && !scheme.equalsIgnoreCase("https"))) {
            throw new ConfigException("'" + key + "' has scheme '" + scheme + "'. Only 'http' and 'https' schemes are permitted for security reasons.");
        }
        if (uri.getHost() == null) {
            throw new ConfigException("'" + key + "' must contain a host.");
        }
        if (uri.getRawQuery() != null || uri.getRawFragment() != null) {
            throw new ConfigException("'" + key + "' must not contain a query or fragment.");
        }
        final var base = uri.toString();
        return URI.create((base.endsWith("/") ? base.substring(0, base.length() - 1) : base) + ENDPOINT_PATH);
    }

    @Override
    public String toString() {
        return "SystemOneSettings[endpoint=" + endpoint + ", model=" + model + ", longContext=" + longContext + ", requestTimeout=" + requestTimeout + ", connectTimeout="
                + connectTimeout + ", maxAttempts=" + maxAttempts + ", initialDelay=" + initialDelay + ", maxDelay=" + maxDelay + "]";
    }
}
