/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

/**
 * @author Randall Hauch
 *
 */
public class ModuleTest {

    @Test
    public void shouldReturnVersion() {
        assertThat(Module.version()).isNotNull();
        assertThat(Module.version()).isNotEmpty();
    }

}
