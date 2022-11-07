/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.mapping;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;

import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.junit.relational.TestRelationalDatabaseConfig;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import io.debezium.relational.ValueConverter;
import io.debezium.util.Strings;

/**
 * @author Randall Hauch
 */
public class ColumnMappersTest {

    private final TableId tableId = new TableId("db", null, "A");
    private Column column;
    private Column column2;
    private Column column3;
    private ColumnMappers mappers;
    private ValueConverter converter;
    private String fullyQualifiedNames;

    @Before
    public void beforeEach() {
        column = Column.editor().name("firstName").jdbcType(Types.VARCHAR).type("VARCHAR").position(1).create();
        column2 = Column.editor().name("lastName").jdbcType(Types.VARCHAR).type("VARCHAR").position(2).create();
        column3 = Column.editor().name("otherColumn").jdbcType(Types.VARCHAR).type("VARCHAR").position(3).create();
        fullyQualifiedNames = tableId + "." + column.name() + ","
                + tableId + "." + column3.name() + ",";
    }

    @Test
    public void shouldNotFindMapperForUnmatchedColumn() {
        Configuration config = Configuration.create()
                .with("column.truncate.to.10.chars", fullyQualifiedNames)
                .build();

        mappers = ColumnMappers.create(new TestRelationalDatabaseConfig(config, null, null, 0));
        converter = mappers.mappingConverterFor(tableId, column2);
        assertThat(converter).isNull();
    }

    @Test
    public void shouldTruncateStrings() {
        Configuration config = Configuration.create()
                .with("column.truncate.to.10.chars", fullyQualifiedNames.toUpperCase())
                .build();

        mappers = ColumnMappers.create(new TestRelationalDatabaseConfig(config, null, null, 0));

        converter = mappers.mappingConverterFor(tableId, column);
        assertThat(converter).isNotNull();
        assertThat(converter.convert("12345678901234567890").toString()).isEqualTo("1234567890");
        assertThat(converter.convert("12345678901234567890").toString().length()).isEqualTo(10);
        assertThat(converter.convert("12345678901").toString()).isEqualTo("1234567890");
        assertThat(converter.convert("12345678901").toString().length()).isEqualTo(10);
        assertThat(converter.convert("1234567890").toString()).isEqualTo("1234567890");
        assertThat(converter.convert("1234567890").toString().length()).isEqualTo(10);
        assertThat(converter.convert("123456789").toString()).isEqualTo("123456789");
        assertThat(converter.convert("123456789").toString().length()).isEqualTo(9);
        assertThat(converter.convert(null)).isNull(); // null values are unaltered
    }

    @Test
    public void shouldMaskStringsToFixedLength() {
        String maskValue = "**********";

        Configuration config = Configuration.create()
                .with("column.mask.with.10.chars", fullyQualifiedNames)
                .build();

        mappers = ColumnMappers.create(new TestRelationalDatabaseConfig(config, null, null, 0)); // exact case
        converter = mappers.mappingConverterFor(tableId, column);
        assertThat(converter).isNotNull();
        assertThat(converter.convert("12345678901234567890")).isEqualTo(maskValue);
        assertThat(converter.convert("12345678901")).isEqualTo(maskValue);
        assertThat(converter.convert("1234567890")).isEqualTo(maskValue);
        assertThat(converter.convert("123456789")).isEqualTo(maskValue);
        assertThat(converter.convert(null)).isEqualTo(maskValue); // null values are masked, too
    }

    @Test
    public void shouldMaskStringsToFixedNumberOfSpecifiedCharacters() {
        char maskChar = '=';
        String maskValue = Strings.createString(maskChar, 10);
        mappers = ColumnMappers.build().maskStrings(fullyQualifiedNames, maskValue.length(), maskChar).build();
        converter = mappers.mappingConverterFor(tableId, column);
        assertThat(converter).isNotNull();
        assertThat(converter.convert("12345678901234567890")).isEqualTo(maskValue);
        assertThat(converter.convert("12345678901")).isEqualTo(maskValue);
        assertThat(converter.convert("1234567890")).isEqualTo(maskValue);
        assertThat(converter.convert("123456789")).isEqualTo(maskValue);
        assertThat(converter.convert(null)).isEqualTo(maskValue); // null values are masked, too
    }

    @Test
    public void shouldMaskStringsWithSpecificValue() {
        String maskValue = "*-*-*-*-*";
        mappers = ColumnMappers.build().maskStrings(fullyQualifiedNames, maskValue).build(); // exact case
        converter = mappers.mappingConverterFor(tableId, column);
        assertThat(converter).isNotNull();
        assertThat(converter.convert("12345678901234567890")).isEqualTo(maskValue);
        assertThat(converter.convert("12345678901")).isEqualTo(maskValue);
        assertThat(converter.convert("1234567890")).isEqualTo(maskValue);
        assertThat(converter.convert("123456789")).isEqualTo(maskValue);
        assertThat(converter.convert(null)).isEqualTo(maskValue); // null values are masked, too
    }

    @Test
    public void shouldMapValuesUsingColumnMapperInstance() {
        RepeatingColumnMapper mapper = new RepeatingColumnMapper();
        mappers = ColumnMappers.build().map(fullyQualifiedNames, mapper).build();
        converter = mappers.mappingConverterFor(tableId, column);
        assertThat(converter).isNotNull();
        assertThat(converter.convert("1234")).isEqualTo("12341234");
        assertThat(converter.convert("a")).isEqualTo("aa");
        assertThat(converter.convert(null)).isNull();
    }

    @Test
    public void shouldMapValuesUsingFunctionByClassName() {
        mappers = ColumnMappers.build().map(fullyQualifiedNames, RepeatingColumnMapper.class.getName()).build();
        converter = mappers.mappingConverterFor(tableId, column);
        assertThat(converter).isNotNull();
        assertThat(converter.convert("1234")).isEqualTo("12341234");
        assertThat(converter.convert("a")).isEqualTo("aa");
        assertThat(converter.convert(null)).isNull();
    }

    @Test
    @FixFor("DBZ-5366")
    public void shouldMaskStringsToMaskHashV1Column() {
        String maskValue = "5944c66655670e4ce234df8529d452ba1cae10a641b9cd1583abf62585b8515a";

        Configuration config = Configuration.create()
                .with("column.mask.hash.SHA-256.with.salt.salt123", fullyQualifiedNames)
                .build();

        mappers = ColumnMappers.create(new TestRelationalDatabaseConfig(config, null, null, 0)); // exact case
        converter = mappers.mappingConverterFor(tableId, column);
        assertThat(converter).isNotNull();
        assertThat(converter.convert("12345678901234567890")).isEqualTo(maskValue);
        assertThat(converter.convert(null)).isNull();
    }

    @Test
    @FixFor("DBZ-5366")
    public void shouldMaskStringsToMaskHashV2Column() {
        String maskValue = "b65875d34a3dedf070f3a012970bf3b5da424560d7be3d2c23b986b525d2d7f3";

        Configuration config = Configuration.create()
                .with("column.mask.hash.v2.SHA-256.with.salt.salt123", fullyQualifiedNames)
                .build();

        mappers = ColumnMappers.create(new TestRelationalDatabaseConfig(config, null, null, 0)); // exact case
        converter = mappers.mappingConverterFor(tableId, column);
        assertThat(converter).isNotNull();
        assertThat(converter.convert("12345678901234567890")).isEqualTo(maskValue);
        assertThat(converter.convert(null)).isNull();
    }

    public static class RepeatingColumnMapper implements ColumnMapper {
        @Override
        public ValueConverter create(Column column) {
            return (value) -> value == null ? null : value.toString() + value.toString();
        }
    }

}
