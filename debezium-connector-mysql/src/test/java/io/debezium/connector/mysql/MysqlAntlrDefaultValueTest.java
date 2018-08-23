/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.fest.assertions.Assertions.assertThat;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;

import org.junit.Test;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.doc.FixFor;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * @author Jiri Pechanec <jpechane@redhat.com>
 */
public class MysqlAntlrDefaultValueTest extends AbstractMysqlDefaultValueTest {

    {
        parserProducer = MySqlAntlrDdlParser::new;
    }

    @Test
    @FixFor("DBZ-870")
    public void shouldAcceptZeroAsDefaultValueForDateColumn() {
        String ddl = "CREATE TABLE data(id INT, nullable_date date default 0, not_nullable_date date not null default 0, PRIMARY KEY (id))";
        parser.parse(ddl, tables);

        Table table = tables.forTable(new TableId(null, null, "data"));

        assertThat(table.columnWithName("nullable_date").hasDefaultValue()).isTrue();

        // zero date should be mapped to null for nullable column
        assertThat(table.columnWithName("nullable_date").defaultValue()).isNull();

        assertThat(table.columnWithName("not_nullable_date").hasDefaultValue()).isTrue();

        // zero date should be mapped to epoch for non-nullable column (expecting Date, as this test is using "connect"
        // mode)
        assertThat(table.columnWithName("not_nullable_date").defaultValue()).isEqualTo(getEpochDate());
    }

    private Date getEpochDate() {
        return Date.from(LocalDate.of(1970, 1, 1).atStartOfDay(ZoneId.of("UTC")).toInstant());
    }
}
