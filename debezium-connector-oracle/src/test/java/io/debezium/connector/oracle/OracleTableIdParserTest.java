/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

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

}
