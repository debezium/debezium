/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.notification.channels;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.pipeline.notification.Notification;
import io.debezium.pipeline.spi.SnapshotResult.SnapshotResultStatus;
import io.debezium.util.SsrfSafeHttpClient;
import io.debezium.util.Strings;

/**
 * A {@link NotificationChannel} that delivers notifications over HTTP by POSTing a JSON representation to a
 * pre-configured URL. It reuses the shared, SSRF-hardened {@link SsrfSafeHttpClient} transport (with exponential-backoff
 * retry) so it does not re-implement HTTP delivery or host validation.
 * <p>
 * The channel is inert until enabled via {@code notification.enabled.channels} (add {@code http}) and configured with a
 * URL under the {@code notification.http.*} prefix. Delivery is dispatched to a single-threaded bounded executor so a
 * slow or unreachable endpoint (with its blocking timeouts and backoff retries) never blocks the pipeline thread that
 * emits the notification; if the queue is full the notification is dropped rather than applying back-pressure to the
 * snapshot. Terminal notifications ({@code COMPLETED}/{@code ABORTED}/{@code SKIPPED}) escalate to a higher retry count
 * because, unlike progress events, they are not self-correcting on a subsequent delivery.
 */
public class HttpNotificationChannel implements NotificationChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpNotificationChannel.class);

    public static final String CHANNEL_NAME = "http";

    private static final int DEFAULT_TIMEOUT_MS = 5000;
    private static final int DEFAULT_RETRIES = 2;
    // Terminal notifications trigger history persistence downstream and are not self-correcting, so they get at least
    // this many retries regardless of the configured (progress-oriented) retry count.
    private static final int MIN_TERMINAL_RETRIES = 3;
    // Bounded so a stalled endpoint cannot accumulate unbounded queued deliveries; excess notifications are dropped.
    private static final int DELIVERY_QUEUE_CAPACITY = 1000;
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 5;

    public static final Field NOTIFICATION_URL = Field.create(CommonConnectorConfig.NOTIFICATION_CONFIGURATION_FIELD_PREFIX_STRING + "http.url")
            .withDisplayName("HTTP notification endpoint URL")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The URL to which notifications are POSTed as JSON. Required when 'http' is in the list of enabled channels.")
            .withValidation(HttpNotificationChannel::validateNotificationUrl);

    public static final Field NOTIFICATION_TIMEOUT_MS = Field.create(CommonConnectorConfig.NOTIFICATION_CONFIGURATION_FIELD_PREFIX_STRING + "http.timeout.ms")
            .withDisplayName("HTTP notification timeout (ms)")
            .withType(ConfigDef.Type.INT)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(DEFAULT_TIMEOUT_MS)
            .withValidation(Field::isPositiveInteger)
            .withDescription("Connect and read timeout, in milliseconds, for HTTP notification delivery.");

    public static final Field NOTIFICATION_RETRIES = Field.create(CommonConnectorConfig.NOTIFICATION_CONFIGURATION_FIELD_PREFIX_STRING + "http.retries")
            .withDisplayName("HTTP notification retries")
            .withType(ConfigDef.Type.INT)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(DEFAULT_RETRIES)
            .withValidation(Field::isNonNegativeInteger)
            .withDescription("Number of retries after the first failed HTTP notification delivery attempt. Terminal notifications "
                    + "(COMPLETED/ABORTED/SKIPPED) are always retried at least " + MIN_TERMINAL_RETRIES + " times.");

    public static final Field NOTIFICATION_ALLOW_PRIVATE_NETWORKS = Field
            .create(CommonConnectorConfig.NOTIFICATION_CONFIGURATION_FIELD_PREFIX_STRING + "http.allow.private.networks")
            .withDisplayName("Allow private network notification targets")
            .withType(ConfigDef.Type.BOOLEAN)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(false)
            .withDescription("When true, the SSRF guard permits notification URLs that resolve to loopback, site-local or "
                    + "link-local addresses (e.g. an in-cluster Service). Defaults to false so only public addresses are allowed.");

    // Terminal snapshot statuses, single-sourced from the snapshot-result domain enum rather than string literals.
    private static final Set<String> TERMINAL_TYPES = Set.of(
            SnapshotResultStatus.COMPLETED.name(),
            SnapshotResultStatus.ABORTED.name(),
            SnapshotResultStatus.SKIPPED.name());

    private String url;
    private int retries;
    private SsrfSafeHttpClient http;
    private ExecutorService deliveryExecutor;

    @Override
    public String name() {
        return CHANNEL_NAME;
    }

    @Override
    public void init(CommonConnectorConfig config) {
        this.url = config.getNotificationHttpUrl();
        this.retries = config.getNotificationHttpRetries();
        Duration timeout = Duration.ofMillis(config.getNotificationHttpTimeoutMs());
        // allowPrivateNetworks defaults to false so only public addresses are allowed. It can be enabled when the
        // callback deliberately targets an in-cluster Service DNS name that resolves to a private/site-local address.
        // The SSRF guard still rejects a null/unresolvable host regardless of this flag.
        boolean allowPrivateNetworks = config.isNotificationHttpAllowPrivateNetworks();
        this.http = SsrfSafeHttpClient.builder()
                .timeout(timeout)
                .allowPrivateNetworks(allowPrivateNetworks)
                .build();
        this.deliveryExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(DELIVERY_QUEUE_CAPACITY),
                runnable -> {
                    Thread thread = new Thread(runnable, "debezium-http-notification");
                    thread.setDaemon(true);
                    return thread;
                });
    }

    @Override
    public void send(Notification notification) {
        if (!isConfigured()) {
            return;
        }
        String body;
        try {
            body = serialize(notification);
        }
        catch (JsonProcessingException e) {
            LOGGER.warn("Failed to serialize notification {}: {}", notification.getType(), e.getMessage());
            return;
        }

        int maxAttempts = maxAttemptsFor(notification);
        try {
            // Dispatch off the connector thread: the blocking timeouts and backoff retries must never block the producer.
            deliveryExecutor.execute(() -> deliver(notification, body, maxAttempts));
        }
        catch (RejectedExecutionException e) {
            LOGGER.warn("Dropped snapshot notification {} to {}: delivery queue is full or the channel is closed",
                    notification.getType(), url);
        }
    }

    private void deliver(Notification notification, String body, int maxAttempts) {
        try {
            http.post(url, Map.of("Content-Type", "application/json"), body, maxAttempts);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Interrupted while delivering notification {} to {}", notification.getType(), url);
        }
        catch (Exception e) {
            // Never propagate: a failed notification must not block the snapshot. Terminal events are not
            // self-correcting, so they are logged at a higher level.
            if (isTerminal(notification)) {
                LOGGER.error("Failed to deliver terminal snapshot notification {} to {} after {} attempts",
                        notification.getType(), url, maxAttempts, e);
            }
            else {
                LOGGER.warn("Dropped snapshot notification {} to {} after {} attempts: {}",
                        notification.getType(), url, maxAttempts, e.getMessage());
            }
        }
    }

    @Override
    public void close() {
        if (deliveryExecutor == null) {
            return;
        }
        deliveryExecutor.shutdown();
        try {
            if (!deliveryExecutor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                deliveryExecutor.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            deliveryExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Total number of delivery attempts for the given notification: {@code retries + 1} for progress events, escalated
     * to {@code max(retries, MIN_TERMINAL_RETRIES) + 1} for terminal events.
     */
    int maxAttemptsFor(Notification notification) {
        int effectiveRetries = isTerminal(notification) ? Math.max(retries, MIN_TERMINAL_RETRIES) : retries;
        return effectiveRetries + 1;
    }

    private static boolean isTerminal(Notification notification) {
        return TERMINAL_TYPES.contains(notification.getType());
    }

    /**
     * Whether the channel has a usable target URL. It can be enabled via {@code notification.enabled.channels} yet left
     * without a URL, in which case delivery is a no-op.
     */
    private boolean isConfigured() {
        return !Strings.isNullOrBlank(url);
    }

    private static int validateNotificationUrl(Configuration config, Field field, Field.ValidationOutput problems) {
        boolean httpChannelEnabled = config.getList(CommonConnectorConfig.NOTIFICATION_ENABLED_CHANNELS).contains(CHANNEL_NAME);
        if (!httpChannelEnabled) {
            return 0; // the URL is only relevant when the 'http' channel is enabled
        }
        String url = config.getString(field);
        if (Strings.isNullOrBlank(url)) {
            problems.accept(field, url, "HTTP notification URL must be provided when the 'http' notification channel is enabled");
            return 1;
        }
        try {
            URI uri = URI.create(url);
            String scheme = uri.getScheme();
            if (uri.getHost() == null || scheme == null || !(scheme.equals("http") || scheme.equals("https"))) {
                problems.accept(field, url, "HTTP notification URL must be a valid http(s) URL with a host");
                return 1;
            }
        }
        catch (IllegalArgumentException e) {
            problems.accept(field, url, "HTTP notification URL is not a valid URL: " + e.getMessage());
            return 1;
        }
        return 0;
    }

    private String serialize(Notification notification) throws JsonProcessingException {
        // Reuse Notification's canonical (camelCase) bean serialization so the payload stays in sync with the type as it
        // evolves, then re-add the id it marks @JsonIgnore (and thus omits from toJson) so the receiver can correlate
        // and de-duplicate events.
        ObjectNode payload = Notification.MAPPER.valueToTree(notification);
        payload.put(Notification.ID_KEY, notification.getId());
        return Notification.MAPPER.writeValueAsString(payload);
    }
}
