/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.charset;

/**
 * Contract for handling binlog-based character set features
 *
 * @author Chris Cranford
 */
public interface BinlogCharsetRegistry {
    /**
     * Get the size of the character set registry map.
     * @return the map size
     */
    int getCharsetMapSize();

    /**
     * Get the collation name for a collation index.
     *
     * @param collationIndex the collation index, should not be {@code null}
     * @return the collation name or {@code null} if not found or index is invalid
     */
    String getCollationNameForCollationIndex(Integer collationIndex);

    /**
     * Get the collation name for the collation index
     *
     * @param collationIndex the collation index, should not be {@code null}
     * @return the collation name or {@code null} if not found or index is invalid
     */
    String getCharsetNameForCollationIndex(Integer collationIndex);

    /**
     * Get the Java encoding for a database character set name.
     *
     * @param charSetName the database character set name
     * @return the java encoding for the character set name
     */
    String getJavaEncodingForCharSet(String charSetName);
}
