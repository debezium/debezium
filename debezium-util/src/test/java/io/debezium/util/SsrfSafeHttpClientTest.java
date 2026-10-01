/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.jupiter.api.Test;

/**
 * Tests the framework-free SSRF guard of {@link SsrfSafeHttpClient}. These cases rely only on literal IP
 * addresses (no DNS lookup) or the loopback name, so they run offline and deterministically.
 */
public class SsrfSafeHttpClientTest {

    @Test
    void shouldRejectUrlWithoutHost() {
        assertThatExceptionOfType(SsrfSafeHttpClient.SsrfValidationException.class)
                .isThrownBy(() -> SsrfSafeHttpClient.validatePublicHost("/relative/path", false));
    }

    @Test
    void shouldRejectMalformedUrl() {
        assertThatExceptionOfType(SsrfSafeHttpClient.SsrfValidationException.class)
                .isThrownBy(() -> SsrfSafeHttpClient.validatePublicHost("http://exa mple.com", false));
    }

    @Test
    void shouldRejectLoopbackWhenPrivateNotAllowed() {
        assertThatExceptionOfType(SsrfSafeHttpClient.SsrfValidationException.class)
                .isThrownBy(() -> SsrfSafeHttpClient.validatePublicHost("http://127.0.0.1/x", false));
    }

    @Test
    void shouldRejectLocalhostWhenPrivateNotAllowed() {
        assertThatExceptionOfType(SsrfSafeHttpClient.SsrfValidationException.class)
                .isThrownBy(() -> SsrfSafeHttpClient.validatePublicHost("http://localhost/x", false));
    }

    @Test
    void shouldRejectSiteLocalWhenPrivateNotAllowed() {
        assertThatExceptionOfType(SsrfSafeHttpClient.SsrfValidationException.class)
                .isThrownBy(() -> SsrfSafeHttpClient.validatePublicHost("http://10.0.0.1/x", false));
    }

    @Test
    void shouldRejectIpv6UniqueLocalWhenPrivateNotAllowed() {
        // fd00::/8 (part of fc00::/7) is an IPv6 unique-local address; InetAddress#isSiteLocalAddress does not cover it.
        assertThatExceptionOfType(SsrfSafeHttpClient.SsrfValidationException.class)
                .isThrownBy(() -> SsrfSafeHttpClient.validatePublicHost("http://[fd12:3456:789a::1]/x", false));
    }

    @Test
    void shouldAllowIpv6UniqueLocalWhenPrivateAllowed() {
        assertThatCode(() -> SsrfSafeHttpClient.validatePublicHost("http://[fd12:3456:789a::1]/x", true))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldAllowLoopbackWhenPrivateAllowed() {
        assertThatCode(() -> SsrfSafeHttpClient.validatePublicHost("http://127.0.0.1/x", true))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldAllowSiteLocalWhenPrivateAllowed() {
        assertThatCode(() -> SsrfSafeHttpClient.validatePublicHost("http://10.0.0.1/x", true))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldAllowPublicAddress() {
        // 8.8.8.8 is a literal public IP: getAllByName parses it without a DNS lookup, so this is offline-safe.
        assertThatCode(() -> SsrfSafeHttpClient.validatePublicHost("http://8.8.8.8/x", false))
                .doesNotThrowAnyException();
    }
}
