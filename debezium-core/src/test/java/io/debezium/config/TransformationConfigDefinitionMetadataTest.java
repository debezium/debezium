/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.Test;

/**
 * Applies basic checks of completeness for the SMT-specific configuration metadata.
 */
public abstract class TransformationConfigDefinitionMetadataTest {

    private final Set<Transformation<?>> transforms;

    public TransformationConfigDefinitionMetadataTest(Transformation<?>... transforms) {
        Set<Transformation<?>> tmp = new HashSet<>();
        Collections.addAll(tmp, transforms);
        this.transforms = Collections.unmodifiableSet(tmp);
    }

    @Test
    public void allFieldsShouldHaveDescription() {
        for (Transformation<?> transformation : transforms) {
            for (Entry<String, ConfigKey> configKey : transformation.config().configKeys().entrySet()) {
                assertThat(configKey.getValue().documentation)
                        .describedAs("Description of config key \"" + configKey.getKey() + "\" of transform class " + transformation.getClass().getName())
                        .isNotNull()
                        .isNotEmpty();
            }
        }
    }
}
