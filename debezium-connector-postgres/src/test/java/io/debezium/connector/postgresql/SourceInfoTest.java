/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.AbstractSourceInfo;

/**
 * @author Jiri Pechanec
 *
 */
public class SourceInfoTest {

    private SourceInfo source;

    @Before
    public void beforeEach() {
        source = new SourceInfo("serverX");
    }

    @Test
    public void versionIsPresent() {
        assertThat(source.source().getString(AbstractSourceInfo.DEBEZIUM_VERSION_KEY)).isEqualTo(Module.version());
    }
}
