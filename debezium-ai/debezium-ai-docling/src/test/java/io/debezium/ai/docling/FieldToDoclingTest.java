/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.docling;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;

/**
 * Unit tests for {@link FieldToDocling} transformation.
 * Tests focus on configuration validation, URI security, and enum parsing.
 */
public class FieldToDoclingTest {

    @Test
    public void testConfigureWithMissingSourceField() {
        Map<String, String> config = new HashMap<>();
        config.put("serve.url", "http://localhost:8080");
        config.put("input.source", "text");
        config.put("input.format", "pdf");
        config.put("output.format", "text");
        // Missing field.source

        FieldToDocling<?> transform = new FieldToDocling<>();
        assertThatThrownBy(() -> transform.configure(config))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("field.source");
    }

    @Test
    public void testConfigureWithEmptySourceField() {
        Map<String, String> config = new HashMap<>();
        config.put("field.source", "");
        config.put("serve.url", "http://localhost:8080");
        config.put("input.source", "text");
        config.put("input.format", "pdf");
        config.put("output.format", "text");

        FieldToDocling<?> transform = new FieldToDocling<>();
        assertThatThrownBy(() -> transform.configure(config))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("field.source");
    }

    @Test
    public void testConfigureWithInvalidInputSource() {
        Map<String, String> config = new HashMap<>();
        config.put("field.source", "content");
        config.put("serve.url", "http://localhost:8080");
        config.put("input.source", "invalid");
        config.put("input.format", "pdf");
        config.put("output.format", "text");

        FieldToDocling<?> transform = new FieldToDocling<>();
        assertThatThrownBy(() -> transform.configure(config))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("input.source")
                .hasMessageContaining("invalid");
    }

    @Test
    public void testConfigureWithInvalidInputFormat() {
        Map<String, String> config = new HashMap<>();
        config.put("field.source", "content");
        config.put("serve.url", "http://localhost:8080");
        config.put("input.source", "text");
        config.put("input.format", "invalid_format");
        config.put("output.format", "text");

        FieldToDocling<?> transform = new FieldToDocling<>();
        assertThatThrownBy(() -> transform.configure(config))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("input format")
                .hasMessageContaining("Invalid");
    }

    @Test
    public void testConfigureWithBooleanIncludeImages() {
        Map<String, String> config = new HashMap<>();
        config.put("field.source", "content");
        config.put("serve.url", "http://localhost:8080");
        config.put("input.source", "text");
        config.put("input.format", "pdf");
        config.put("include.images", "false");
        config.put("output.format", "text");

        FieldToDocling<?> transform = new FieldToDocling<>();
        // Should not throw - boolean type is now correct
        transform.configure(config);
        transform.close();
    }

    @Test
    public void testConfigureWithInvalidServeUrlScheme() {
        Map<String, String> config = new HashMap<>();
        config.put("field.source", "content");
        config.put("serve.url", "ftp://example.com");
        config.put("input.source", "text");
        config.put("input.format", "pdf");
        config.put("output.format", "text");

        FieldToDocling<?> transform = new FieldToDocling<>();
        assertThatThrownBy(() -> transform.configure(config))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("scheme 'ftp' is not allowed")
                .hasMessageContaining("Only 'http' and 'https' schemes are permitted");
    }

    @Test
    public void testConfigureWithFileServeUrlScheme() {
        Map<String, String> config = new HashMap<>();
        config.put("field.source", "content");
        config.put("serve.url", "file:///etc/passwd");
        config.put("input.source", "text");
        config.put("input.format", "pdf");
        config.put("output.format", "text");

        FieldToDocling<?> transform = new FieldToDocling<>();
        assertThatThrownBy(() -> transform.configure(config))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("scheme 'file' is not allowed")
                .hasMessageContaining("Only 'http' and 'https' schemes are permitted");
    }

    @Test
    public void testConfigureWithNoSchemeServeUrl() {
        Map<String, String> config = new HashMap<>();
        config.put("field.source", "content");
        config.put("serve.url", "localhost:8080");
        config.put("input.source", "text");
        config.put("input.format", "pdf");
        config.put("output.format", "text");

        FieldToDocling<?> transform = new FieldToDocling<>();
        // Note: "localhost:8080" is parsed as scheme "localhost", which is not http/https
        assertThatThrownBy(() -> transform.configure(config))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("is not allowed");
    }

    @Test
    public void testConfigureWithValidHttpServeUrl() {
        Map<String, String> config = new HashMap<>();
        config.put("field.source", "content");
        config.put("serve.url", "http://localhost:8080");
        config.put("input.source", "text");
        config.put("input.format", "pdf");
        config.put("output.format", "text");

        FieldToDocling<?> transform = new FieldToDocling<>();
        // Should not throw
        transform.configure(config);
        transform.close();
    }

    @Test
    public void testConfigureWithValidHttpsServeUrl() {
        Map<String, String> config = new HashMap<>();
        config.put("field.source", "content");
        config.put("serve.url", "https://docling.example.com");
        config.put("input.source", "text");
        config.put("input.format", "pdf");
        config.put("output.format", "text");

        FieldToDocling<?> transform = new FieldToDocling<>();
        // Should not throw
        transform.configure(config);
        transform.close();
    }

    @Test
    public void testvalidateUrlWithFileScheme() {
        FieldToDocling<?> transform = new FieldToDocling<>();
        assertThatThrownBy(() -> transform.validateUrl("file:///etc/passwd"))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("URI scheme 'file' is not allowed")
                .hasMessageContaining("Only 'http' and 'https' schemes are permitted for security reasons");
    }

    @Test
    public void testvalidateUrlWithFtpScheme() {
        FieldToDocling<?> transform = new FieldToDocling<>();
        assertThatThrownBy(() -> transform.validateUrl("ftp://example.com/file.pdf"))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("URI scheme 'ftp' is not allowed")
                .hasMessageContaining("Only 'http' and 'https' schemes are permitted for security reasons");
    }

    @Test
    public void testvalidateUrlWithNoScheme() {
        FieldToDocling<?> transform = new FieldToDocling<>();
        // URI without :// has null scheme
        assertThatThrownBy(() -> transform.validateUrl("example.com/document.pdf"))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("URI scheme 'null' is not allowed");
    }

    @Test
    public void testvalidateUrlWithNullUri() {
        FieldToDocling<?> transform = new FieldToDocling<>();
        assertThatThrownBy(() -> transform.validateUrl(null))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("URL is empty");
    }

    @Test
    public void testvalidateUrlWithEmptyUri() {
        FieldToDocling<?> transform = new FieldToDocling<>();
        assertThatThrownBy(() -> transform.validateUrl(""))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("URL is empty");
    }

    @Test
    public void testvalidateUrlWithValidHttpUri() {
        FieldToDocling<?> transform = new FieldToDocling<>();
        // Should not throw
        transform.validateUrl("http://example.com/document.pdf");
    }

    @Test
    public void testvalidateUrlWithValidHttpsUri() {
        FieldToDocling<?> transform = new FieldToDocling<>();
        // Should not throw
        transform.validateUrl("https://example.com/document.pdf");
    }

    @Test
    public void testvalidateUrlWithRelativePath() {
        FieldToDocling<?> transform = new FieldToDocling<>();
        // Relative paths have no scheme, will be rejected
        assertThatThrownBy(() -> transform.validateUrl("/path/to/document.pdf"))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("URI scheme 'null' is not allowed");
    }

    @Test
    public void testInputSourceParseCaseInsensitive() {
        Map<String, String> config1 = new HashMap<>();
        config1.put("field.source", "content");
        config1.put("serve.url", "http://localhost:8080");
        config1.put("input.source", "TEXT");
        config1.put("input.format", "pdf");
        config1.put("output.format", "text");

        FieldToDocling<?> transform1 = new FieldToDocling<>();
        transform1.configure(config1);
        transform1.close();

        Map<String, String> config2 = new HashMap<>();
        config2.put("field.source", "content");
        config2.put("serve.url", "http://localhost:8080");
        config2.put("input.source", "LINK");
        config2.put("input.format", "pdf");
        config2.put("output.format", "text");

        FieldToDocling<?> transform2 = new FieldToDocling<>();
        transform2.configure(config2);
        transform2.close();
    }

    @Test
    public void testInputFormatParseCaseInsensitive() {
        Map<String, String> config = new HashMap<>();
        config.put("field.source", "content");
        config.put("serve.url", "http://localhost:8080");
        config.put("input.source", "text");
        config.put("input.format", "PDF");
        config.put("output.format", "text");

        FieldToDocling<?> transform = new FieldToDocling<>();
        // Should not throw - case insensitive parsing
        transform.configure(config);
        transform.close();
    }

    @Test
    public void testOutputFormatParseCaseInsensitive() {
        Map<String, String> config = new HashMap<>();
        config.put("field.source", "content");
        config.put("serve.url", "http://localhost:8080");
        config.put("input.source", "text");
        config.put("input.format", "pdf");
        config.put("output.format", "HTML");

        FieldToDocling<?> transform = new FieldToDocling<>();
        // Should not throw - case insensitive parsing
        transform.configure(config);
        transform.close();
    }

    @Test
    public void testValidConfiguration() {
        Map<String, String> config = new HashMap<>();
        config.put("field.source", "content");
        config.put("field.docling", "docling_output");
        config.put("serve.url", "https://docling.example.com");
        config.put("input.source", "text");
        config.put("input.format", "pdf");
        config.put("include.images", "true");
        config.put("output.format", "markdown");

        FieldToDocling<?> transform = new FieldToDocling<>();
        // Should not throw
        transform.configure(config);
        assertThat(transform.version()).isNotNull();
        transform.close();
    }

    @Test
    public void testValidConfigurationWithNestedSourceField() {
        Map<String, String> config = new HashMap<>();
        config.put("field.source", "after.content");
        config.put("serve.url", "http://localhost:8080");
        config.put("input.source", "text");
        config.put("input.format", "pdf");
        config.put("output.format", "text");

        FieldToDocling<?> transform = new FieldToDocling<>();
        // Should not throw
        transform.configure(config);
        transform.close();
    }
}
