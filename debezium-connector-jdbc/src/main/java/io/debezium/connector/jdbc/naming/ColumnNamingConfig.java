/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.naming;

import io.debezium.connector.jdbc.util.NamingStyle;

public record ColumnNamingConfig(String prefix, String suffix, NamingStyle namingStyle) { }
