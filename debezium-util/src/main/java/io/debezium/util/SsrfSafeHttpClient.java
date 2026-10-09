/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.io.IOException;
import java.io.Serial;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/**
 * A framework-free HTTP sender hardened against Server-Side Request Forgery (SSRF), with built-in exponential-backoff
 * retry. It depends only on {@code java.net.http} and the existing {@link RetryingRunnable}/{@link DelayStrategy}
 * utilities, so it is usable identically from Debezium core, connectors, and the platform Conductor.
 * <p>
 * The SSRF guard ({@link #validatePublicHost(String, boolean)}) resolves the target host and rejects loopback,
 * site-local, link-local and any-local addresses unless private networks are explicitly allowed. This is the same
 * check previously embedded in the platform {@code WebhookNotifier}, extracted here for reuse.
 */
public final class SsrfSafeHttpClient {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration DEFAULT_RETRY_INITIAL_DELAY = Duration.ofSeconds(1);
    private static final Duration DEFAULT_RETRY_MAX_DELAY = Duration.ofSeconds(30);

    /**
     * Thrown when a target URL fails the SSRF guard (missing/unresolvable host, or a host that resolves to a
     * non-public address while private networks are disallowed). Non-retriable: it indicates a configuration or
     * security problem, not a transient failure.
     */
    public static class SsrfValidationException extends Exception {

        @Serial
        private static final long serialVersionUID = 1L;

        public SsrfValidationException(String message) {
            super(message);
        }
    }

    /**
     * Thrown when an HTTP delivery attempt fails (I/O error or a non-2xx response). Retriable: the sender retries
     * this exception up to the configured number of attempts before propagating it.
     */
    public static class HttpDeliveryException extends Exception {

        @Serial
        private static final long serialVersionUID = 1L;

        public HttpDeliveryException(String message) {
            super(message);
        }

        public HttpDeliveryException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private final HttpClient httpClient;
    private final Duration requestTimeout;
    private final boolean allowPrivateNetworks;
    private final Duration retryInitialDelay;
    private final Duration retryMaxDelay;

    private SsrfSafeHttpClient(Builder builder) {
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(builder.connectTimeout)
                .build();
        this.requestTimeout = builder.requestTimeout;
        this.allowPrivateNetworks = builder.allowPrivateNetworks;
        this.retryInitialDelay = builder.retryInitialDelay;
        this.retryMaxDelay = builder.retryMaxDelay;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Convenience wrapper for {@link #send(String, String, Map, String, int)} that uses the {@code POST} method.
     *
     * @param url target URL
     * @param headers request headers
     * @param body request body
     * @param maxAttempts total number of attempts (must be at least 1); {@code maxAttempts - 1} retries follow the
     *            first attempt
     * @throws SsrfValidationException if the URL fails the SSRF guard (thrown before any request is sent)
     * @throws HttpDeliveryException if every attempt fails
     * @throws InterruptedException if interrupted while waiting between retries
     */
    public void post(String url, Map<String, String> headers, String body, int maxAttempts)
            throws SsrfValidationException, HttpDeliveryException, InterruptedException {
        send("POST", url, headers, body, maxAttempts);
    }

    /**
     * Validates the URL against the SSRF guard, then sends the request with exponential-backoff retry. The URL is
     * validated once, up front, so an SSRF failure is never retried; only {@link HttpDeliveryException} (I/O errors
     * and non-2xx responses) triggers a retry.
     *
     * @param method HTTP method (e.g. {@code POST}, {@code PUT})
     * @param url target URL
     * @param headers request headers
     * @param body request body
     * @param maxAttempts total number of attempts (must be at least 1); {@code maxAttempts - 1} retries follow the
     *            first attempt
     * @throws SsrfValidationException if the URL fails the SSRF guard (thrown before any request is sent)
     * @throws HttpDeliveryException if every attempt fails
     * @throws InterruptedException if interrupted while waiting between retries
     */
    public void send(String method, String url, Map<String, String> headers, String body, int maxAttempts)
            throws SsrfValidationException, HttpDeliveryException, InterruptedException {

        validatePublicHost(url, allowPrivateNetworks);

        RetryingRunnable.<HttpDeliveryException> builder()
                .retries(Math.max(0, maxAttempts - 1))
                .doRun(() -> deliver(method, url, headers, body))
                .delayStrategy(DelayStrategy.exponential(retryInitialDelay, retryMaxDelay))
                .retriableExceptions(HttpDeliveryException.class)
                .build()
                .run();
    }

    private void deliver(String method, String url, Map<String, String> headers, String body)
            throws HttpDeliveryException, InterruptedException {
        try {
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(requestTimeout)
                    .method(method, HttpRequest.BodyPublishers.ofString(body == null ? "" : body));

            if (headers != null) {
                headers.forEach(requestBuilder::header);
            }

            HttpResponse<String> response = httpClient.send(requestBuilder.build(),
                    HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new HttpDeliveryException("HTTP " + response.statusCode());
            }
        }
        catch (IOException e) {
            // Retriable transport failure; RetryingRunnable retries HttpDeliveryException. InterruptedException is
            // deliberately not caught here so it propagates and stops the retry loop immediately.
            throw new HttpDeliveryException(e.getMessage(), e);
        }
    }

    public static final class Builder {

        private Duration connectTimeout = DEFAULT_TIMEOUT;
        private Duration requestTimeout = DEFAULT_TIMEOUT;
        private boolean allowPrivateNetworks = false;
        private Duration retryInitialDelay = DEFAULT_RETRY_INITIAL_DELAY;
        private Duration retryMaxDelay = DEFAULT_RETRY_MAX_DELAY;

        private Builder() {
        }

        /**
         * Convenience setter that applies the same value to both {@link #connectTimeout(Duration)} and
         * {@link #requestTimeout(Duration)}.
         */
        public Builder timeout(Duration timeout) {
            this.connectTimeout = timeout;
            this.requestTimeout = timeout;
            return this;
        }

        /**
         * Sets the timeout for establishing the TCP connection to the target host.
         */
        public Builder connectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        /**
         * Sets the timeout for awaiting the response once the request has been sent.
         */
        public Builder requestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }

        public Builder allowPrivateNetworks(boolean allowPrivateNetworks) {
            this.allowPrivateNetworks = allowPrivateNetworks;
            return this;
        }

        public Builder retryInitialDelay(Duration retryInitialDelay) {
            this.retryInitialDelay = retryInitialDelay;
            return this;
        }

        public Builder retryMaxDelay(Duration retryMaxDelay) {
            this.retryMaxDelay = retryMaxDelay;
            return this;
        }

        public SsrfSafeHttpClient build() {
            return new SsrfSafeHttpClient(this);
        }
    }

    /**
     * Validates that {@code url} has a resolvable host that is safe to contact. When {@code allowPrivateNetworks} is
     * {@code false}, any address that is loopback, site-local, link-local or any-local causes an
     * {@link SsrfValidationException}.
     *
     * @param url the target URL
     * @param allowPrivateNetworks when {@code true}, private/loopback addresses are permitted (e.g. an in-cluster
     *            Service target); when {@code false}, only public addresses are allowed
     * @throws SsrfValidationException if the URL has no host, cannot be resolved, or resolves to a non-public address
     */
    public static void validatePublicHost(String url, boolean allowPrivateNetworks) throws SsrfValidationException {
        String host;
        try {
            host = URI.create(url).getHost();
        }
        catch (IllegalArgumentException e) {
            throw new SsrfValidationException("Invalid URL: " + e.getMessage());
        }
        if (host == null) {
            throw new SsrfValidationException("Invalid URL: no host in '" + url + "'");
        }
        if (allowPrivateNetworks) {
            return;
        }
        try {
            InetAddress[] addresses = InetAddress.getAllByName(host);
            for (InetAddress addr : addresses) {
                if (addr.isLoopbackAddress() || addr.isSiteLocalAddress()
                        || addr.isLinkLocalAddress() || addr.isAnyLocalAddress()
                        || isIpv6UniqueLocal(addr)) {
                    throw new SsrfValidationException("URL resolves to non-public address: " + addr.getHostAddress());
                }
            }
        }
        catch (UnknownHostException e) {
            throw new SsrfValidationException("Cannot resolve host: " + e.getMessage());
        }
    }

    /**
     * Returns {@code true} for IPv6 unique-local addresses ({@code fc00::/7}, in practice {@code fd00::/8}), which
     * {@link InetAddress#isSiteLocalAddress()} does not recognise (it only covers the deprecated {@code fec0::/10}).
     */
    private static boolean isIpv6UniqueLocal(InetAddress addr) {
        if (!(addr instanceof Inet6Address)) {
            return false;
        }
        return (addr.getAddress()[0] & 0xfe) == 0xfc;
    }
}
