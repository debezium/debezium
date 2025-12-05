/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import io.debezium.doc.FixFor;
import io.debezium.relational.TableId;

/**
 * @author Chris Cranford
 */
public class OracleTableIdParserTest {

    @Test
    @FixFor("DBZ-4744")
    public void shouldParseFullyQualifiedTableNameWithDomainNameAsCatalog() throws Exception {
        final TableId tableId = OracleTableIdParser.parse("RB02.DOMAIN.COM.TBSLV.DEBEZIUM_SIGNAL");
        assertThat(tableId.table()).isEqualTo("DEBEZIUM_SIGNAL");
        assertThat(tableId.schema()).isEqualTo("TBSLV");
        assertThat(tableId.catalog()).isEqualTo("RB02.DOMAIN.COM");
    }

    @Test
    @FixFor("DBZ-4744")
    public void shouldParseFullyQualifiedTableWithoutDomainNameAsCatalogName() throws Exception {
        final TableId tableId = OracleTableIdParser.parse("SERVER1.TBSLV.DEBEZIUM_SIGNAL");
        assertThat(tableId.table()).isEqualTo("DEBEZIUM_SIGNAL");
        assertThat(tableId.schema()).isEqualTo("TBSLV");
        assertThat(tableId.catalog()).isEqualTo("SERVER1");
    }

    @Test
    @FixFor("DBZ-7942")
    public void shouldQuoteTableNameThatStartsWithUnderscoreWithDatabaseAndSchemaNames() throws Exception {
        final List<String> keywords = Collections.singletonList("FROM");
        final TableId tableId = OracleTableIdParser.parse("DB.SCHEMA.__DEBEZIUM_SIGNAL");
        final String quotedValue = OracleTableIdParser.quoteIfNeeded(tableId, false, true, keywords);
        assertThat(quotedValue).isEqualTo("SCHEMA.\"__DEBEZIUM_SIGNAL\"");
    }

    @Test
    @FixFor("DBZ-7942")
    public void shouldQuoteTableNameThatStartsWithUnderscoreWithSchemaName() throws Exception {
        final List<String> keywords = Collections.singletonList("FROM");
        final TableId tableId = OracleTableIdParser.parse("SCHEMA.__DEBEZIUM_SIGNAL");
        final String quotedValue = OracleTableIdParser.quoteIfNeeded(tableId, false, true, keywords);
        assertThat(quotedValue).isEqualTo("SCHEMA.\"__DEBEZIUM_SIGNAL\"");
    }

    @Test
    @FixFor("DBZ-7942")
    public void shouldQuoteTableNameThatContainsDatabaseKeyword() throws Exception {
        final List<String> keywords = Collections.singletonList("FROM");
        final TableId tableId = OracleTableIdParser.parse("SCHEMA.FROM");
        final String quotedValue = OracleTableIdParser.quoteIfNeeded(tableId, false, true, keywords);
        assertThat(quotedValue).isEqualTo("SCHEMA.\"FROM\"");
    }

}
