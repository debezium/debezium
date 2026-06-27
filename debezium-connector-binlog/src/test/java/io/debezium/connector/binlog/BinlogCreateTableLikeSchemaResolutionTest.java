/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

public class BinlogCreateTableLikeSchemaResolutionTest {

    private TableId extractViaPattern(String sql, String currentDatabase) {
        TableId[] tables = BinlogStreamingChangeEventSource.extractLikeTables(sql, currentDatabase);
        return tables == null ? null : tables[1];
    }

    private TableId extractNewTable(String sql, String currentDatabase) {
        TableId[] tables = BinlogStreamingChangeEventSource.extractLikeTables(sql, currentDatabase);
        return tables == null ? null : tables[0];
    }

    @Test
    public void shouldExtractSimpleLikeReference() {
        TableId ref = extractViaPattern("CREATE TABLE new_shard LIKE template_table", "test_db");
        assertThat(ref).isNotNull();
        assertThat(ref.catalog()).isEqualTo("test_db");
        assertThat(ref.table()).isEqualTo("template_table");
    }

    @Test
    public void shouldExtractBacktickQuotedReference() {
        TableId ref = extractViaPattern("CREATE TABLE `new_shard` LIKE `template_table`", "test_db");
        assertThat(ref).isNotNull();
        assertThat(ref.catalog()).isEqualTo("test_db");
        assertThat(ref.table()).isEqualTo("template_table");
    }

    @Test
    public void shouldExtractCrossDatabaseReference() {
        TableId ref = extractViaPattern("CREATE TABLE `db1`.`new_shard` LIKE `db2`.`template_table`", "db1");
        assertThat(ref).isNotNull();
        assertThat(ref.catalog()).isEqualTo("db2");
        assertThat(ref.table()).isEqualTo("template_table");
    }

    @Test
    public void shouldExtractCrossDatabaseReferenceWithoutBackticks() {
        TableId ref = extractViaPattern("CREATE TABLE db1.new_shard LIKE db2.template_table", "db1");
        assertThat(ref).isNotNull();
        assertThat(ref.catalog()).isEqualTo("db2");
        assertThat(ref.table()).isEqualTo("template_table");
    }

    @Test
    public void shouldExtractIfNotExistsLikeReference() {
        TableId ref = extractViaPattern("CREATE TABLE IF NOT EXISTS new_shard LIKE template_table", "test_db");
        assertThat(ref).isNotNull();
        assertThat(ref.catalog()).isEqualTo("test_db");
        assertThat(ref.table()).isEqualTo("template_table");
    }

    @Test
    public void shouldBeCaseInsensitive() {
        TableId ref = extractViaPattern("create table new_shard like template_table", "test_db");
        assertThat(ref).isNotNull();
        assertThat(ref.catalog()).isEqualTo("test_db");
        assertThat(ref.table()).isEqualTo("template_table");
    }

    @Test
    public void shouldHandleLeadingSqlComment() {
        TableId ref = extractViaPattern("/* Test Mock */ CREATE TABLE new_shard LIKE template_table", "test_db");
        assertThat(ref).isNotNull();
        assertThat(ref.catalog()).isEqualTo("test_db");
        assertThat(ref.table()).isEqualTo("template_table");
    }

    @Test
    public void shouldHandleTrailingSqlComment() {
        TableId ref = extractViaPattern("CREATE TABLE new_shard LIKE template_table;", "test_db");
        assertThat(ref).isNotNull();
        assertThat(ref.catalog()).isEqualTo("test_db");
        assertThat(ref.table()).isEqualTo("template_table");
    }

    @Test
    public void shouldReturnNullForNonLikeDdl() {
        assertThat(extractViaPattern("CREATE TABLE t(id INT PRIMARY KEY)", "test_db")).isNull();
        assertThat(extractViaPattern("ALTER TABLE t ADD COLUMN c INT", "test_db")).isNull();
        assertThat(extractViaPattern("DROP TABLE t", "test_db")).isNull();
        assertThat(extractViaPattern("INSERT INTO t VALUES (1)", "test_db")).isNull();
    }

    @Test
    public void shouldReturnNullForCreateTableAsSelect() {
        assertThat(extractViaPattern(
                "CREATE TABLE new_table AS SELECT * FROM old_table", "test_db")).isNull();
    }

    @Test
    public void shouldExtraWithMixedQuoting() {
        TableId ref = extractViaPattern("CREATE TABLE `db1`.new_shard LIKE `db2`.template_table", "db1");
        assertThat(ref).isNotNull();
        assertThat(ref.catalog()).isEqualTo("db2");
        assertThat(ref.table()).isEqualTo("template_table");
    }

    @Test
    public void shouldExtraNewTableAndDatabase() {
        TableId newTable = extractNewTable("CREATE TABLE `db1`.new_shard LIKE `db2`.template_table", "db1");
        assertThat(newTable).isNotNull();
        assertThat(newTable.catalog()).isEqualTo("db1");
        assertThat(newTable.table()).isEqualTo("new_shard");
    }

    @Test
    public void shouldFallBackToCurrentDatabaseForUnqualifiedNewTable() {
        TableId newTable = extractNewTable("CREATE TABLE new_shard LIKE template_table", "db1");
        assertThat(newTable).isNotNull();
        assertThat(newTable.catalog()).isEqualTo("db1");
        assertThat(newTable.table()).isEqualTo("new_shard");
    }

    @Test
    public void shouldExtraIdentifiersContainingDollarSign() {
        TableId[] tables = BinlogStreamingChangeEventSource.extractLikeTables(
                "CREATE TABLE new_shard$2 LIKE template$table", "test_db");
        assertThat(tables).isNotNull();
        assertThat(tables[0].table()).isEqualTo("new_shard$2");
        assertThat(tables[1].table()).isEqualTo("template$table");
    }

}
