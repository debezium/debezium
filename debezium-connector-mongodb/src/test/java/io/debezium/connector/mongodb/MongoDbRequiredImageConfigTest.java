/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.debezium.config.Configuration;

public class MongoDbRequiredImageConfigTest {

    @Test
    void shouldExposeImagePoliciesWithCompatibleDefaults() {
        final var keys = MongoDbConnectorConfig.configDef().configKeys();
        assertThat(keys).containsKeys("capture.mode.pre.image", "capture.mode.post.image");
        assertThat(keys.get("capture.mode.pre.image").defaultValue).isEqualTo("off");
        assertThat(keys.get("capture.mode.post.image").defaultValue).isEqualTo("lookup");
        assertThat(keys.get("capture.mode.pre.image").importance).isEqualTo(ConfigDef.Importance.MEDIUM);
        assertThat(keys.get("capture.mode.post.image").importance).isEqualTo(ConfigDef.Importance.MEDIUM);
    }

    @ParameterizedTest
    @CsvSource({
            "off, off",
            "off, lookup",
            "off, post_image",
            "off, post_image_required",
            "when_available, off",
            "when_available, lookup",
            "when_available, post_image",
            "when_available, post_image_required",
            "required, off",
            "required, lookup",
            "required, post_image",
            "required, post_image_required"
    })
    void shouldAcceptIndependentImagePolicies(String preImage, String postImage) {
        final var config = TestHelper.getConfiguration().edit()
                .with("capture.mode.pre.image", preImage)
                .with("capture.mode.post.image", postImage)
                .build();
        assertValid(config);
        assertPolicies(config, preImage, postImage);
    }

    @ParameterizedTest
    @CsvSource({
            ", , off, lookup",
            ", post_image, off, post_image",
            "change_streams, , off, off",
            "change_streams, lookup, off, off",
            "change_streams, post_image, off, off",
            "change_streams_update_full, , off, lookup",
            "change_streams_update_full, lookup, off, lookup",
            "change_streams_update_full, post_image, off, post_image",
            "change_streams_update_full, post_image_required, off, post_image_required",
            "change_streams_with_pre_image, , when_available, off",
            "change_streams_with_pre_image, lookup, when_available, off",
            "change_streams_with_pre_image, post_image, when_available, off",
            "change_streams_update_full_with_pre_image, , when_available, lookup",
            "change_streams_update_full_with_pre_image, lookup, when_available, lookup",
            "change_streams_update_full_with_pre_image, post_image, when_available, post_image",
            "change_streams_update_full_with_pre_image, post_image_required, when_available, post_image_required"
    })
    void shouldPreserveLegacyImagePolicies(String captureMode, String fullUpdate, String expectedPreImage, String expectedPostImage) {
        final var config = legacyConfiguration(captureMode, fullUpdate).build();
        assertValid(config);
        assertPolicies(config, expectedPreImage, expectedPostImage);
    }

    @ParameterizedTest
    @CsvSource({
            "change_streams, post_image, required, , required, off",
            "change_streams, post_image, , lookup, off, lookup",
            "change_streams, post_image_required, , off, off, off",
            "change_streams, , required, post_image_required, required, post_image_required",
            "change_streams_with_pre_image, post_image, , post_image_required, when_available, post_image_required",
            "change_streams_update_full_with_pre_image, post_image, off, , off, post_image",
            "change_streams_update_full_with_pre_image, post_image, , off, when_available, off",
            "change_streams_update_full_with_pre_image, post_image, off, off, off, off",
            "change_streams_update_full, post_image, required, lookup, required, lookup",
            ", , required, , required, lookup",
            ", , , off, off, off"
    })
    void shouldOverrideLegacyPoliciesIndependently(String captureMode, String fullUpdate, String preImage, String postImage,
                                                   String expectedPreImage, String expectedPostImage) {
        final var builder = legacyConfiguration(captureMode, fullUpdate);
        if (preImage != null) {
            builder.with("capture.mode.pre.image", preImage);
        }
        if (postImage != null) {
            builder.with("capture.mode.post.image", postImage);
        }
        final var config = builder.build();
        assertValid(config);
        assertPolicies(config, expectedPreImage, expectedPostImage);
    }

    @ParameterizedTest
    @CsvSource({
            "capture.mode.pre.image, invalid",
            "capture.mode.pre.image, ''",
            "capture.mode.post.image, invalid",
            "capture.mode.post.image, ''",
            "capture.mode.post.image, required",
            "capture.mode, invalid",
            "capture.mode.full.update.type, invalid"
    })
    void shouldRejectInvalidImagePolicies(String property, String value) {
        final var config = TestHelper.getConfiguration().edit()
                .with(property, value)
                .build();
        final var values = config.validate(MongoDbConnectorConfig.ALL_FIELDS);
        assertThat(values).containsKey(property);
        assertThat(values.get(property).errorMessages()).isNotEmpty();
    }

    @ParameterizedTest
    @CsvSource({ "change_streams", "change_streams_with_pre_image" })
    void shouldRejectRequiredLegacyPostImagesWhenDisabled(String captureMode) {
        final var config = legacyConfiguration(captureMode, "post_image_required").build();
        assertThat(config.validate(MongoDbConnectorConfig.ALL_FIELDS).get("capture.mode.full.update.type").errorMessages()).isNotEmpty();
    }

    @Test
    void shouldParseMissingPreImageModeAsOff() {
        assertThat(MongoDbConnectorConfig.PreImageMode.parse(null)).isEqualTo(MongoDbConnectorConfig.PreImageMode.OFF);
        assertThat(MongoDbConnectorConfig.PreImageMode.parse("")).isEqualTo(MongoDbConnectorConfig.PreImageMode.OFF);
        assertThat(MongoDbConnectorConfig.PreImageMode.parse("  ")).isEqualTo(MongoDbConnectorConfig.PreImageMode.OFF);
        assertThat(MongoDbConnectorConfig.PreImageMode.parse("invalid")).isNull();
    }

    private static Configuration.Builder legacyConfiguration(String captureMode, String fullUpdate) {
        final var builder = TestHelper.getConfiguration().edit();
        if (captureMode != null) {
            builder.with("capture.mode", captureMode);
        }
        if (fullUpdate != null) {
            builder.with("capture.mode.full.update.type", fullUpdate);
        }
        return builder;
    }

    private static void assertValid(Configuration config) {
        final var values = config.validate(MongoDbConnectorConfig.ALL_FIELDS);
        assertThat(values.values()).allSatisfy(value -> assertThat(value.errorMessages()).isEmpty());
    }

    private static void assertPolicies(Configuration config, String expectedPreImage, String expectedPostImage) {
        final var connectorConfig = new MongoDbConnectorConfig(config);
        assertThat(connectorConfig.getCaptureModePreImage().getValue()).isEqualTo(expectedPreImage);
        assertThat(connectorConfig.getCaptureModePostImage().getValue()).isEqualTo(expectedPostImage);
    }
}
