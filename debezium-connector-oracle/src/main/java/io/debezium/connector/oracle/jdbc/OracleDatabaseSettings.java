/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.jdbc;

import oracle.sql.CharacterSet;

/**
 * Database settings that affect how string data is interpreted, read once from the database that the connector
 * captures, which is the pluggable database when one is configured, and shared by every component that needs them.
 *
 * @param databaseCharacterSet the character set for {@code VARCHAR2}, {@code CHAR} and {@code CLOB} data
 * @param nationalCharacterSet the character set for {@code NVARCHAR2}, {@code NCHAR} and {@code NCLOB} data
 * @param extendedStringsSupported whether {@code MAX_STRING_SIZE} is {@code EXTENDED}
 * @param charLengthSemantics whether {@code NLS_LENGTH_SEMANTICS} is {@code CHAR}
 *
 * @author Chris Cranford
 */
public record OracleDatabaseSettings(CharacterSet databaseCharacterSet, CharacterSet nationalCharacterSet,
        boolean extendedStringsSupported, boolean charLengthSemantics) {
}
