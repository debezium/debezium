/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.charset;

import com.mysql.cj.CharsetMapping;

import io.debezium.connector.binlog.charset.BinlogCharsetRegistry;

/**
 * Character-set registry implementation for MySQL that delegates to the MySQL JDBC driver.
 *
 * @author Chris Cranford
 */
public class MySqlCharsetRegistry implements BinlogCharsetRegistry {
    @Override
    public int getCharsetMapSize() {
        return CharsetMapping.MAP_SIZE;
    }

    @Override
    public String getCollationNameForCollationIndex(Integer collationIndex) {
        return CharsetMapping.getStaticCollationNameForCollationIndex(collationIndex);
    }

    @Override
    public String getCharsetNameForCollationIndex(Integer collationIndex) {
        return CharsetMapping.getStaticMysqlCharsetNameForCollationIndex(collationIndex);
    }

    @Override
    public String getJavaEncodingForCharSet(String charSetName) {
        return CharsetMappingMapper.getJavaEncodingForCharSet(charSetName);
    }

    /**
     * Helper to gain access to protected method
     */
    private final static class CharsetMappingMapper extends CharsetMapping {
        static String getJavaEncodingForCharSet(String charSetName) {
            return CharsetMapping.getStaticJavaEncodingForMysqlCharset(charSetName);
        }
    }
}
