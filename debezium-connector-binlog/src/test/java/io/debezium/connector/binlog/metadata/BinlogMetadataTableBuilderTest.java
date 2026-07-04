/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Types;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventMetadata;
import com.github.shyiko.mysql.binlog.event.TableMapEventMetadata.DefaultCharset;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * Unit test for {@link BinlogMetadataTableBuilder}, seeded with the exact FULL {@code TABLE_MAP}
 * metadata values captured from a real MySQL/Percona 8.0.35 server (see
 * {@code .experiment/dbz978/tablemap-groundtruth.md}). Asserts the reconstructed {@link Table}
 * matches the source {@code types_demo} DDL.
 */
class BinlogMetadataTableBuilderTest {

    private final BinlogMetadataTableBuilder builder = new BinlogMetadataTableBuilder();

    /**
     * Builds a TABLE_MAP event equivalent to:
     *
     * <pre>
     * CREATE TABLE types_demo (
     *   id INT NOT NULL, big_u BIGINT UNSIGNED, small_s SMALLINT, is_active TINYINT(1),
     *   price DECIMAL(12,3), ratio DOUBLE, name VARCHAR(64) utf8mb4, code CHAR(4) NOT NULL,
     *   raw_b BINARY(8), descr TEXT, blob_c BLOB, status ENUM('NEW','OK','ERR'),
     *   tags SET('a','b','c'), flags BIT(5), created_at DATETIME(3), ts TIMESTAMP(6),
     *   d DATE, doc JSON, PRIMARY KEY (id, code));
     * </pre>
     */
    private TableMapEventData typesDemoTableMap() {
        final TableMapEventMetadata meta = new TableMapEventMetadata();
        meta.setColumnNames(List.of(
                "id", "big_u", "small_s", "is_active", "price", "ratio", "name", "code",
                "raw_b", "descr", "blob_c", "status", "tags", "flags", "created_at", "ts", "d", "doc"));
        // signedness is indexed over numeric columns only: id,big_u,small_s,is_active,price,ratio -> bit 1 (big_u) unsigned
        final BitSet signedness = new BitSet();
        signedness.set(1);
        meta.setSignedness(signedness);
        // default charset utf8mb4 (255), with binary (63) overrides for the character-column indices of raw_b (2) and blob_c (4)
        final DefaultCharset defaultCharset = new DefaultCharset();
        defaultCharset.setDefaultCharsetCollation(255);
        defaultCharset.setCharsetCollations(Map.of(2, 63, 4, 63));
        meta.setDefaultCharset(defaultCharset);
        meta.setEnumStrValues(List.<String[]> of(new String[]{ "NEW", "OK", "ERR" }));
        meta.setSetStrValues(List.<String[]> of(new String[]{ "a", "b", "c" }));
        meta.setSimplePrimaryKeys(List.of(0, 7));

        final TableMapEventData data = new TableMapEventData();
        data.setDatabase("test");
        data.setTable("types_demo");
        data.setColumnTypes(new byte[]{
                3, 8, 2, 1, (byte) 246, 5, 15, (byte) 254, (byte) 254, (byte) 252,
                (byte) 252, (byte) 254, (byte) 254, 16, 18, 17, 10, (byte) 245 });
        data.setColumnMetadata(new int[]{
                0, 0, 0, 0, 780, 8, 256, 65040, 65032, 2, 2, 63233, 63489, 5, 3, 6, 0, 4 });
        // nullable = all except id(0) and code(7)
        final BitSet nullability = new BitSet();
        for (int i = 0; i < 18; i++) {
            if (i != 0 && i != 7) {
                nullability.set(i);
            }
        }
        data.setColumnNullability(nullability);
        data.setEventMetadata(meta);
        return data;
    }

    @Test
    void shouldReconstructTableFromFullMetadata() {
        final Table table = builder.build(TableId.parse("test.types_demo"), typesDemoTableMap());

        assertThat(table.columns()).hasSize(18);
        assertThat(table.primaryKeyColumnNames()).containsExactly("id", "code");

        assertColumn(table, "id", "INT", Types.INTEGER, false);
        assertColumn(table, "big_u", "BIGINT UNSIGNED", Types.BIGINT, true);
        assertColumn(table, "small_s", "SMALLINT", Types.SMALLINT, true);
        assertColumn(table, "is_active", "TINYINT", Types.SMALLINT, true);

        final Column price = table.columnWithName("price");
        assertThat(price.typeName()).isEqualTo("DECIMAL");
        assertThat(price.jdbcType()).isEqualTo(Types.DECIMAL);
        assertThat(price.length()).isEqualTo(12);
        assertThat(price.scale()).contains(3);

        assertColumn(table, "ratio", "DOUBLE", Types.DOUBLE, true);

        final Column name = table.columnWithName("name");
        assertThat(name.typeName()).isEqualTo("VARCHAR");
        assertThat(name.jdbcType()).isEqualTo(Types.VARCHAR);
        assertThat(name.length()).isEqualTo(64); // 256 bytes / 4 (utf8mb4)
        assertThat(name.charsetName()).isEqualTo("utf8mb4");

        final Column code = table.columnWithName("code");
        assertThat(code.typeName()).isEqualTo("CHAR");
        assertThat(code.jdbcType()).isEqualTo(Types.CHAR);
        assertThat(code.length()).isEqualTo(4);
        assertThat(code.isOptional()).isFalse();

        final Column rawB = table.columnWithName("raw_b");
        assertThat(rawB.typeName()).isEqualTo("BINARY");
        assertThat(rawB.jdbcType()).isEqualTo(Types.BINARY);
        assertThat(rawB.length()).isEqualTo(8);

        assertColumn(table, "descr", "TEXT", Types.VARCHAR, true);
        assertColumn(table, "blob_c", "BLOB", Types.BLOB, true);

        final Column status = table.columnWithName("status");
        assertThat(status.typeName()).isEqualTo("ENUM");
        assertThat(status.jdbcType()).isEqualTo(Types.CHAR);
        assertThat(status.enumValues()).containsExactly("NEW", "OK", "ERR");

        final Column tags = table.columnWithName("tags");
        assertThat(tags.typeName()).isEqualTo("SET");
        assertThat(tags.enumValues()).containsExactly("a", "b", "c");

        final Column flags = table.columnWithName("flags");
        assertThat(flags.typeName()).isEqualTo("BIT");
        assertThat(flags.jdbcType()).isEqualTo(Types.BIT);
        assertThat(flags.length()).isEqualTo(5);

        final Column createdAt = table.columnWithName("created_at");
        assertThat(createdAt.typeName()).isEqualTo("DATETIME");
        assertThat(createdAt.jdbcType()).isEqualTo(Types.TIMESTAMP);
        assertThat(createdAt.length()).isEqualTo(3);

        final Column ts = table.columnWithName("ts");
        assertThat(ts.typeName()).isEqualTo("TIMESTAMP");
        assertThat(ts.jdbcType()).isEqualTo(Types.TIMESTAMP_WITH_TIMEZONE);
        assertThat(ts.length()).isEqualTo(6);

        assertColumn(table, "d", "DATE", Types.DATE, true);
        assertColumn(table, "doc", "JSON", Types.OTHER, true);
    }

    @Test
    void shouldDecodeBitLengthFromPackedMetadata() {
        // BIT metadata is (bytes << 8) | remaining_bits: BIT(2)=2, BIT(8)=0x0100, BIT(64)=0x0800.
        final TableMapEventMetadata meta = new TableMapEventMetadata();
        meta.setColumnNames(List.of("b2", "b8", "b64"));

        final TableMapEventData data = new TableMapEventData();
        data.setDatabase("test");
        data.setTable("bits");
        data.setColumnTypes(new byte[]{ 16, 16, 16 });
        data.setColumnMetadata(new int[]{ 2, 256, 2048 });
        data.setColumnNullability(new BitSet());
        data.setEventMetadata(meta);

        final Table table = builder.build(TableId.parse("test.bits"), data);
        assertThat(table.columnWithName("b2").length()).isEqualTo(2);
        assertThat(table.columnWithName("b8").length()).isEqualTo(8);
        assertThat(table.columnWithName("b64").length()).isEqualTo(64);
    }

    @Test
    void shouldMapVectorColumn() {
        final TableMapEventMetadata meta = new TableMapEventMetadata();
        meta.setColumnNames(List.of("v"));

        final TableMapEventData data = new TableMapEventData();
        data.setDatabase("test");
        data.setTable("vec");
        data.setColumnTypes(new byte[]{ (byte) 242 });
        data.setColumnMetadata(new int[]{ 4 });
        data.setColumnNullability(new BitSet());
        data.setEventMetadata(meta);

        final Table table = builder.build(TableId.parse("test.vec"), data);
        assertThat(table.columnWithName("v").typeName()).isEqualTo("VECTOR");
        assertThat(table.columnWithName("v").jdbcType()).isEqualTo(Types.OTHER);
    }

    @Test
    void shouldFailWhenMetadataMissing() {
        final TableMapEventData data = new TableMapEventData();
        data.setDatabase("test");
        data.setTable("no_meta");
        data.setColumnTypes(new byte[]{ 3 });
        data.setColumnMetadata(new int[]{ 0 });
        data.setColumnNullability(new BitSet());
        // no event metadata (MINIMAL) -> must fail fast
        assertThatThrownBy(() -> builder.build(TableId.parse("test.no_meta"), data))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("binlog_row_metadata=FULL");
    }

    private void assertColumn(Table table, String name, String typeName, int jdbcType, boolean optional) {
        final Column c = table.columnWithName(name);
        assertThat(c).as("column %s exists", name).isNotNull();
        assertThat(c.typeName()).as("%s typeName", name).isEqualTo(typeName);
        assertThat(c.jdbcType()).as("%s jdbcType", name).isEqualTo(jdbcType);
        assertThat(c.isOptional()).as("%s optional", name).isEqualTo(optional);
    }
}
