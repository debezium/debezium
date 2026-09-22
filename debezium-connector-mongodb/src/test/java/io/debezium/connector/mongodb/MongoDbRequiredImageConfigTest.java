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

public class MongoDbRequiredImageConfigTest {

    @Test
    void shouldExposeImagePoliciesWithCompatibleDefaults() {
        final var keys = MongoDbConnectorConfig.configDef().configKeys();
        assertThat(keys).containsKeys("capture.mode.pre.image", "capture.mode.full.update.type");
        assertThat(keys.get("capture.mode.pre.image").defaultValue).isEqualTo("when_available");
        assertThat(keys.get("capture.mode.pre.image").importance).isEqualTo(ConfigDef.Importance.MEDIUM);
        assertThat(keys.get("capture.mode.full.update.type").defaultValue).isEqualTo("lookup");
    }

    @ParameterizedTest
    @CsvSource({
            "required, lookup",
            "when_available, post_image_required",
            "required, post_image_required",
            "when_available, post_image"
    })
    void shouldAcceptIndependentImagePolicies(String preImage, String fullUpdate) {
        final var config = TestHelper.getConfiguration().edit()
                .with(MongoDbConnectorConfig.CAPTURE_MODE, "change_streams_update_full_with_pre_image")
                .with("capture.mode.pre.image", preImage)
                .with("capture.mode.full.update.type", fullUpdate)
                .build();
        final var values = config.validate(MongoDbConnectorConfig.ALL_FIELDS);
        assertThat(values).containsKeys("capture.mode.pre.image", "capture.mode.full.update.type");
        assertThat(values.get("capture.mode.pre.image").errorMessages()).isEmpty();
        assertThat(values.get("capture.mode.full.update.type").errorMessages()).isEmpty();
        assertThat(new MongoDbConnectorConfig(config).getCaptureModeFullUpdateType().getValue()).isEqualTo(fullUpdate);
    }

    @ParameterizedTest
    @CsvSource({
            "change_streams_update_full, capture.mode.pre.image, required",
            "change_streams_with_pre_image, capture.mode.full.update.type, post_image_required",
            "change_streams_update_full_with_pre_image, capture.mode.pre.image, invalid",
            "change_streams_update_full_with_pre_image, capture.mode.full.update.type, invalid"
    })
    void shouldRejectInvalidImagePolicies(String captureMode, String property, String value) {
        final var config = TestHelper.getConfiguration().edit()
                .with(MongoDbConnectorConfig.CAPTURE_MODE, captureMode)
                .with(property, value)
                .build();
        final var values = config.validate(MongoDbConnectorConfig.ALL_FIELDS);
        assertThat(values).containsKey(property);
        assertThat(values.get(property).errorMessages()).isNotEmpty();
    }

    @Test
    void shouldKeepAcceptingUnusedLegacyPostImageSetting() {
        final var config = TestHelper.getConfiguration().edit()
                .with(MongoDbConnectorConfig.CAPTURE_MODE, "change_streams")
                .with("capture.mode.full.update.type", "post_image")
                .build();
        final var values = config.validate(MongoDbConnectorConfig.ALL_FIELDS);
        assertThat(values).containsKey("capture.mode.full.update.type");
        assertThat(values.get("capture.mode.full.update.type").errorMessages()).isEmpty();
    }
}
