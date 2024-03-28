/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.charset;

import com.mysql.cj.CharsetMapping;

/**
 * A simple bridge between the MySQL driver and the MariaDB code to resolve charset mappings.
 *
 * @author Chris Cranford
 */
public class CharsetMappingResolver {

    // todo: replace with our own implementation
    public static int getMapSize() {
        return CharsetMapping.MAP_SIZE;
    }

    // todo: replace with our own implementation
    public static String getStaticCollationNameForCollationIndex(Integer collationIndex) {
        return CharsetMapping.getStaticCollationNameForCollationIndex(collationIndex);
    }

    // todo: replace with our own implementation
    public static String getStaticMariaDbCharsetNameForCollationIndex(Integer collationIndex) {
        return CharsetMapping.getStaticMysqlCharsetNameForCollationIndex(collationIndex);
    }

    // todo: replace with our own implementation
    public static String getJavaEncodingForMariaDbCharSet(String charSetName) {
        return CharsetMappingWrapper.getJavaEncodingForMariaDbCharSet(charSetName);
    }

    private CharsetMappingResolver() {
    }

    /**
     * Helper to gain access to the private method.
     */
    private final static class CharsetMappingWrapper extends CharsetMapping {
        static String getJavaEncodingForMariaDbCharSet(String charSetName) {
            return CharsetMapping.getStaticJavaEncodingForMysqlCharset(charSetName);
        }
    }
}
