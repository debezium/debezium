/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

// Adapted for Debezium from https://github.com/antlr/grammars-v4/tree/master/sql/mysql/Oracle/Java
package io.debezium.antlr.mysql;

/**
 * SQL modes that control MySQL parsing behavior.
 */
public enum SqlMode {
    NoMode,
    AnsiQuotes,
    HighNotPrecedence,
    PipesAsConcat,
    IgnoreSpace,
    NoBackslashEscapes
}
