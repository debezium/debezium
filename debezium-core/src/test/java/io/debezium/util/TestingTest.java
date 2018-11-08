/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.fest.assertions.Assertions.assertThat;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

public class TestingTest implements Testing {

    @Test
    public void shouldKnowDirectoriesInsideTestData() {
        assertThat(Testing.Files.inTestDataDir(Paths.get(Testing.Files.dataDir(), "somefile"))).isTrue();
        assertThat(Testing.Files.inTestDataDir(new File("../debezium").toPath())).isFalse();
    }

    @Test
    public void shouldRemoveDirectory() throws Exception {
        Path path = Paths.get(Testing.Files.dataDir(), "test-dir");
        assertThat(path.toFile().mkdirs()).isTrue();

        Path file = path.resolve("file.txt");
        assertThat(file.toFile().createNewFile()).isTrue();

        Testing.Files.delete(path);
        assertThat(java.nio.file.Files.exists(path)).isFalse();
    }
}
