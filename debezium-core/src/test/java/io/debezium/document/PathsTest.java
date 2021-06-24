/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.document;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 *
 */
public class PathsTest implements Testing {

    private Path path;

    @Before
    public void beforeEach() {
        this.path = null;
    }

    @Test
    public void shouldParseRootPath() {
        path = parse("/");
        assertThat(path.isRoot()).isTrue();
        assertThat(path.isSingle()).isFalse();
        assertThat(path.size()).isEqualTo(0);
    }

    @Test
    public void shouldParseSingleRelativePath() {
        path = parse("a");
        assertThat(path.isRoot()).isFalse();
        assertThat(path.isSingle()).isTrue();
        assertThat(path.size()).isEqualTo(1);
        assertThat(path.segment(0)).isEqualTo("a");
    }

    @Test
    public void shouldParseSingleAbsolutePath() {
        path = parse("/a");
        assertThat(path.isRoot()).isFalse();
        assertThat(path.isSingle()).isTrue();
        assertThat(path.size()).isEqualTo(1);
        assertThat(path.segment(0)).isEqualTo("a");
    }

    @Test
    public void shouldParseDoubleRelativePath() {
        path = parse("a/b");
        assertThat(path.isRoot()).isFalse();
        assertThat(path.isSingle()).isFalse();
        assertThat(path.size()).isEqualTo(2);
        assertThat(path.segment(0)).isEqualTo("a");
        assertThat(path.segment(1)).isEqualTo("b");
    }

    @Test
    public void shouldParseDoubleAbsolutePath() {
        path = parse("/a/b");
        assertThat(path.isRoot()).isFalse();
        assertThat(path.isSingle()).isFalse();
        assertThat(path.size()).isEqualTo(2);
        assertThat(path.segment(0)).isEqualTo("a");
        assertThat(path.segment(1)).isEqualTo("b");
    }

    @Test
    public void shouldParseMultiRelativePath() {
        path = parse("a/b");
        assertThat(path.isRoot()).isFalse();
        assertThat(path.isSingle()).isFalse();
        assertThat(path.size()).isEqualTo(2);
        assertThat(path.segment(0)).isEqualTo("a");
        assertThat(path.segment(1)).isEqualTo("b");
    }

    @Test
    public void shouldParseMultiAbsolutePath() {
        path = parse("/a/b/c/d/e");
        assertThat(path.isRoot()).isFalse();
        assertThat(path.isSingle()).isFalse();
        assertThat(path.size()).isEqualTo(5);
        assertThat(path.segment(0)).isEqualTo("a");
        assertThat(path.segment(1)).isEqualTo("b");
        assertThat(path.segment(2)).isEqualTo("c");
        assertThat(path.segment(3)).isEqualTo("d");
        assertThat(path.segment(4)).isEqualTo("e");
    }

    protected Path parse(String path) {
        return Paths.parse(path, false);
    }

}
