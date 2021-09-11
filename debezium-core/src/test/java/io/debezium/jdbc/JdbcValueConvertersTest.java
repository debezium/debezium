/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

import java.sql.Types;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.debezium.doc.FixFor;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;

public class JdbcValueConvertersTest {
    private ColumnEditor columnEditor;

    @Before
    public void beforeEach() {
        columnEditor = Column.editor();
    }

    @Test
    @FixFor("DBZ-3989")
    public void testConvertIntegerWithWhiteSpace() {
        Column column = columnEditor.name("col1").type("INTEGER").jdbcType(Types.INTEGER).defaultValue("1 ").create();
        Field field = new Field("test", -1, Schema.INT32_SCHEMA);
        JdbcValueConverters converters = new JdbcValueConverters();

        Object convertedData = converters.convertInteger(column, field, "1 ");

        Assert.assertEquals(1, convertedData);
    }

    @Test
    @FixFor("DBZ-3989")
    public void testConvertIntegerWithoutWhiteSpace() {
        Column column = columnEditor.name("col1").type("INTEGER").jdbcType(Types.INTEGER).defaultValue("1 ").create();
        Field field = new Field("test", -1, Schema.INT32_SCHEMA);
        JdbcValueConverters converters = new JdbcValueConverters();

        Object convertedData = converters.convertInteger(column, field, 1);

        Assert.assertEquals(1, convertedData);
    }

    @Test
    @FixFor("DBZ-3989")
    public void testConvertStringWithWhiteSpace() {
        Column column = columnEditor.name("col1").type("STRING").jdbcType(Types.VARCHAR).defaultValue("1 ").create();
        Field field = new Field("test", -1, Schema.STRING_SCHEMA);
        JdbcValueConverters converters = new JdbcValueConverters();

        Object convertedData = converters.convertString(column, field, "abca ");

        Assert.assertEquals("abca ", convertedData);
    }
}
