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
    public void shouldKnowDirectoriesInsideTarget() {
        assertThat(Testing.Files.inTargetDir(new File("target/classes").toPath())).isTrue();
        assertThat(Testing.Files.inTargetDir(new File("../debezium").toPath())).isFalse();
    }
    
    @Test
    public void shouldRemoveDirectory() throws Exception {
        Path path = Paths.get("target/test-dir");
        path.toFile().mkdir();
        Path file = Paths.get("target/test-dir/file.txt");
        file.toFile().createNewFile();
        Testing.Files.delete(path);
    }
}
