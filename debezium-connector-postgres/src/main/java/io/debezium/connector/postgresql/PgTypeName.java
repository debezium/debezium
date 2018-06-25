/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

/**
 * Contains Postgres specific datatype names, especially ones without a reliable oid.
 *
 * @author Olavi Mustanoja (tilastokeskus@gmail.com)
 */
public final class PgTypeName {

    public static final String CITEXT = "citext";
}
