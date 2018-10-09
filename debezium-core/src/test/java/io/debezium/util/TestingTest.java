/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class TestingTest implements Testing {
    
    @Test
    public void shouldKnowDirectoriesInsideTestData() {
        assertThat(Testing.Files.inTestDataDir(new File("target/data/somefile").toPath())).isTrue();
        assertThat(Testing.Files.inTestDataDir(new File("../debezium").toPath())).isFalse();
    }
    
    @Test
    public void shouldRemoveDirectory() throws Exception {
        Path path = Paths.get("target/data/test-dir");
        assertThat(path.toFile().mkdirs()).isTrue();

        Path file = Paths.get("target/data/test-dir/file.txt");
        assertThat(file.toFile().createNewFile()).isTrue();

        Testing.Files.delete(path);
        // todo: assert that 'target/data/test-dir' is removed
    }
}
