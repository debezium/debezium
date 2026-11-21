/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;
import io.debezium.relational.Column;

class RowImageUtilsTest {

    @Test
    @FixFor("debezium/dbz#962")
    void shouldTreatJsonAsBlobOrTextColumn() {
        // MariaDB stores JSON as LONGTEXT and omits it from NOBLOB row images like TEXT
        assertThat(RowImageUtils.isBlobOrTextColumn(column("JSON", Types.OTHER))).isTrue();
        assertThat(RowImageUtils.isBlobOrTextColumn(column("LONGTEXT", Types.VARCHAR))).isTrue();
        assertThat(RowImageUtils.isBlobOrTextColumn(column("MEDIUMBLOB", Types.BLOB))).isTrue();
        // Both MySQL and MariaDB keep these columns in NOBLOB row images
        assertThat(RowImageUtils.isBlobOrTextColumn(column("POINT", Types.OTHER))).isFalse();
        assertThat(RowImageUtils.isBlobOrTextColumn(column("VARCHAR", Types.VARCHAR))).isFalse();
    }

    private static Column column(String typeName, int jdbcType) {
        return Column.editor().name("c").type(typeName).jdbcType(jdbcType).create();
    }
}
