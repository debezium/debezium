/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

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
