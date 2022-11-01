/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;

import io.debezium.junit.SkipTestRule;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.Tables;
import io.debezium.util.Testing;

@SkipWhenDatabaseVersion(check = LESS_THAN, major = 5, minor = 6, patch = 5, reason = "MySQL 5.5 does not support CURRENT_TIMESTAMP on DATETIME and only a single column can specify default CURRENT_TIMESTAMP, lifted in MySQL 5.6.5")
public class MetadataIT implements Testing {

    @Rule
    public SkipTestRule skipTest = new SkipTestRule();

    /**
     * Loads the {@link Tables} definition by reading JDBC metadata. Note that some characteristics, such as whether columns
     * are generated, are not exposed through JDBC (unlike when reading DDL).
     * @throws SQLException if there's an error
     */
    @Test
    public void shouldLoadMetadataViaJdbc() throws SQLException {
        final UniqueDatabase DATABASE = new UniqueDatabase("readbinlog_it", "readbinlog_test");
        DATABASE.createAndInitialize();

        try (MySqlTestConnection conn = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());) {
            conn.connect();
            // Set up the table as one transaction and wait to see the events ...
            conn.execute("DROP TABLE IF EXISTS person",
                    "DROP TABLE IF EXISTS product",
                    "DROP TABLE IF EXISTS purchased");

            conn.execute("CREATE TABLE person ("
                    + "  name VARCHAR(255) primary key,"
                    + "  birthdate DATE NULL,"
                    + "  age INTEGER NULL DEFAULT 10,"
                    + "  salary DECIMAL(5,2),"
                    + "  bitStr BIT(18)"
                    + ")");
            conn.execute("SELECT * FROM person");
            Tables tables = new Tables();
            conn.readSchema(tables, DATABASE.getDatabaseName(), null, null, null, true);
            // System.out.println(tables);
            assertThat(tables.size()).isEqualTo(1);
            Table person = tables.forTable(DATABASE.getDatabaseName(), null, "person");
            assertThat(person).isNotNull();
            assertThat(person.filterColumns(col -> col.isAutoIncremented())).isEmpty();
            assertThat(person.primaryKeyColumnNames()).containsOnly("name");
            assertThat(person.retrieveColumnNames()).containsExactly("name", "birthdate", "age", "salary", "bitStr");
            assertThat(person.columnWithName("name").name()).isEqualTo("name");
            assertThat(person.columnWithName("name").typeName()).isEqualTo("VARCHAR");
            assertThat(person.columnWithName("name").jdbcType()).isEqualTo(Types.VARCHAR);
            assertThat(person.columnWithName("name").length()).isEqualTo(255);
            assertFalse(person.columnWithName("name").scale().isPresent());
            assertThat(person.columnWithName("name").position()).isEqualTo(1);
            assertThat(person.columnWithName("name").isAutoIncremented()).isFalse();
            assertThat(person.columnWithName("name").isGenerated()).isFalse();
            assertThat(person.columnWithName("name").isOptional()).isFalse();
            assertThat(person.columnWithName("birthdate").name()).isEqualTo("birthdate");
            assertThat(person.columnWithName("birthdate").typeName()).isEqualTo("DATE");
            assertThat(person.columnWithName("birthdate").jdbcType()).isEqualTo(Types.DATE);
            assertThat(person.columnWithName("birthdate").length()).isEqualTo(10);
            assertFalse(person.columnWithName("birthdate").scale().isPresent());
            assertThat(person.columnWithName("birthdate").position()).isEqualTo(2);
            assertThat(person.columnWithName("birthdate").isAutoIncremented()).isFalse();
            assertThat(person.columnWithName("birthdate").isGenerated()).isFalse();
            assertThat(person.columnWithName("birthdate").isOptional()).isTrue();
            assertThat(person.columnWithName("age").name()).isEqualTo("age");
            assertThat(person.columnWithName("age").typeName()).isEqualTo("INT");
            assertThat(person.columnWithName("age").jdbcType()).isEqualTo(Types.INTEGER);
            assertThat(person.columnWithName("age").length()).isEqualTo(10);

            // DBZ-763: this used to be 0 as of MySQL Connector/J 5.x
            assertThat(!person.columnWithName("age").scale().isPresent());
            assertThat(person.columnWithName("age").position()).isEqualTo(3);
            assertThat(person.columnWithName("age").isAutoIncremented()).isFalse();
            assertThat(person.columnWithName("age").isGenerated()).isFalse();
            assertThat(person.columnWithName("age").isOptional()).isTrue();
            assertThat(person.columnWithName("salary").name()).isEqualTo("salary");
            assertThat(person.columnWithName("salary").typeName()).isEqualTo("DECIMAL");
            assertThat(person.columnWithName("salary").jdbcType()).isEqualTo(Types.DECIMAL);
            assertThat(person.columnWithName("salary").length()).isEqualTo(5);
            assertThat(person.columnWithName("salary").scale().get()).isEqualTo(2);
            assertThat(person.columnWithName("salary").position()).isEqualTo(4);
            assertThat(person.columnWithName("salary").isAutoIncremented()).isFalse();
            assertThat(person.columnWithName("salary").isGenerated()).isFalse();
            assertThat(person.columnWithName("salary").isOptional()).isTrue();
            assertThat(person.columnWithName("bitStr").name()).isEqualTo("bitStr");
            assertThat(person.columnWithName("bitStr").typeName()).isEqualTo("BIT");
            assertThat(person.columnWithName("bitStr").jdbcType()).isEqualTo(Types.BIT);
            assertThat(person.columnWithName("bitStr").length()).isEqualTo(18);
            assertFalse(person.columnWithName("bitStr").scale().isPresent());
            assertThat(person.columnWithName("bitStr").position()).isEqualTo(5);
            assertThat(person.columnWithName("bitStr").isAutoIncremented()).isFalse();
            assertThat(person.columnWithName("bitStr").isGenerated()).isFalse();
            assertThat(person.columnWithName("bitStr").isOptional()).isTrue();

            conn.execute("CREATE TABLE product ("
                    + "  id INT NOT NULL AUTO_INCREMENT,"
                    + "  createdByDate DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                    + "  modifiedDate DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
                    + "  PRIMARY KEY(id)"
                    + ")");
            conn.execute("SELECT * FROM product");
            tables = new Tables();
            conn.readSchema(tables, DATABASE.getDatabaseName(), null, null, null, true);
            // System.out.println(tables);
            assertThat(tables.size()).isEqualTo(2);
            Table product = tables.forTable(DATABASE.getDatabaseName(), null, "product");
            assertThat(product).isNotNull();
            List<Column> autoIncColumns = product.filterColumns(Column::isAutoIncremented);
            assertThat(autoIncColumns).hasSize(1);
            assertThat(autoIncColumns.get(0).name()).isEqualTo("id");
            assertThat(product.primaryKeyColumnNames()).containsOnly("id");
            assertThat(product.retrieveColumnNames()).containsExactly("id", "createdByDate", "modifiedDate");
            assertThat(product.columnWithName("id").name()).isEqualTo("id");
            assertThat(product.columnWithName("id").typeName()).isEqualTo("INT");
            assertThat(product.columnWithName("id").jdbcType()).isEqualTo(Types.INTEGER);
            assertThat(product.columnWithName("id").length()).isEqualTo(10);
            assertThat(!product.columnWithName("id").scale().isPresent()
                    || product.columnWithName("id").scale().get() == 0);
            assertThat(product.columnWithName("id").position()).isEqualTo(1);
            assertThat(product.columnWithName("id").isAutoIncremented()).isTrue();
            assertThat(product.columnWithName("id").isGenerated()).isFalse();
            assertThat(product.columnWithName("id").isOptional()).isFalse();
            assertThat(product.columnWithName("createdByDate").name()).isEqualTo("createdByDate");
            assertThat(product.columnWithName("createdByDate").typeName()).isEqualTo("DATETIME");
            assertThat(product.columnWithName("createdByDate").jdbcType()).isEqualTo(Types.TIMESTAMP);
            assertThat(product.columnWithName("createdByDate").length()).isEqualTo(19);
            assertFalse(product.columnWithName("createdByDate").scale().isPresent());
            assertThat(product.columnWithName("createdByDate").position()).isEqualTo(2);
            assertThat(product.columnWithName("createdByDate").isAutoIncremented()).isFalse();
            assertThat(product.columnWithName("createdByDate").isGenerated()).isEqualTo(conn.databaseAsserts().isCurrentDateTimeDefaultGenerated());
            assertThat(product.columnWithName("createdByDate").isOptional()).isFalse();
            assertThat(product.columnWithName("modifiedDate").name()).isEqualTo("modifiedDate");
            assertThat(product.columnWithName("modifiedDate").typeName()).isEqualTo("DATETIME");
            assertThat(product.columnWithName("modifiedDate").jdbcType()).isEqualTo(Types.TIMESTAMP);
            assertThat(product.columnWithName("modifiedDate").length()).isEqualTo(19);
            assertFalse(product.columnWithName("modifiedDate").scale().isPresent());
            assertThat(product.columnWithName("modifiedDate").position()).isEqualTo(3);
            assertThat(product.columnWithName("modifiedDate").isAutoIncremented()).isFalse();
            assertThat(product.columnWithName("modifiedDate").isGenerated()).isEqualTo(conn.databaseAsserts().isCurrentDateTimeDefaultGenerated());
            assertThat(product.columnWithName("modifiedDate").isOptional()).isFalse();

            conn.execute("CREATE TABLE purchased ("
                    + "  purchaser VARCHAR(255) NOT NULL,"
                    + "  productId INT NOT NULL,"
                    + "  purchaseDate DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                    + "  PRIMARY KEY(productId,purchaser)"
                    + ")");
            conn.execute("SELECT * FROM purchased");
            tables = new Tables();
            conn.readSchema(tables, DATABASE.getDatabaseName(), null, null, null, true);
            // System.out.println(tables);
            assertThat(tables.size()).isEqualTo(3);
            Table purchased = tables.forTable(DATABASE.getDatabaseName(), null, "purchased");
            assertThat(purchased).isNotNull();
            assertThat(person.filterColumns(col -> col.isAutoIncremented())).isEmpty();
            assertThat(purchased.primaryKeyColumnNames()).containsOnly("productId", "purchaser");
            assertThat(purchased.retrieveColumnNames()).containsExactly("purchaser", "productId", "purchaseDate");
            assertThat(purchased.columnWithName("purchaser").name()).isEqualTo("purchaser");
            assertThat(purchased.columnWithName("purchaser").typeName()).isEqualTo("VARCHAR");
            assertThat(purchased.columnWithName("purchaser").jdbcType()).isEqualTo(Types.VARCHAR);
            assertThat(purchased.columnWithName("purchaser").length()).isEqualTo(255);
            assertFalse(purchased.columnWithName("purchaser").scale().isPresent());
            assertThat(purchased.columnWithName("purchaser").position()).isEqualTo(1);
            assertThat(purchased.columnWithName("purchaser").isAutoIncremented()).isFalse();
            assertThat(purchased.columnWithName("purchaser").isGenerated()).isFalse();
            assertThat(purchased.columnWithName("purchaser").isOptional()).isFalse();
            assertThat(purchased.columnWithName("productId").name()).isEqualTo("productId");
            assertThat(purchased.columnWithName("productId").typeName()).isEqualTo("INT");
            assertThat(purchased.columnWithName("productId").jdbcType()).isEqualTo(Types.INTEGER);
            assertThat(purchased.columnWithName("productId").length()).isEqualTo(10);

            // DBZ-763: this used to be 0 as of MySQL Connector/J 5.x
            assertThat(!purchased.columnWithName("productId").scale().isPresent());
            assertThat(purchased.columnWithName("productId").position()).isEqualTo(2);
            assertThat(purchased.columnWithName("productId").isAutoIncremented()).isFalse();
            assertThat(purchased.columnWithName("productId").isGenerated()).isFalse();
            assertThat(purchased.columnWithName("productId").isOptional()).isFalse();
            assertThat(purchased.columnWithName("purchaseDate").name()).isEqualTo("purchaseDate");
            assertThat(purchased.columnWithName("purchaseDate").typeName()).isEqualTo("DATETIME");
            assertThat(purchased.columnWithName("purchaseDate").jdbcType()).isEqualTo(Types.TIMESTAMP);
            assertThat(purchased.columnWithName("purchaseDate").length()).isEqualTo(19);
            assertFalse(purchased.columnWithName("purchaseDate").scale().isPresent());
            assertThat(purchased.columnWithName("purchaseDate").position()).isEqualTo(3);
            assertThat(purchased.columnWithName("purchaseDate").isAutoIncremented()).isFalse();
            assertThat(purchased.columnWithName("purchaseDate").isGenerated()).isEqualTo(conn.databaseAsserts().isCurrentDateTimeDefaultGenerated());
            assertThat(purchased.columnWithName("purchaseDate").isOptional()).isFalse();
        }
    }
}
