/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.relational.TableId;

/**
 * Specialized parser implementation for Oracle {@link TableId} instances.
 *
 * @author Chris Cranford
 */
public class OracleTableIdParser {

    public static TableId parse(String identifier) {
        final String[] parts = TableId.parseParts(identifier);
        if (parts.length > 3) {
            // Oracle identifiers may be in the format of "domain"."schema"."table" where the domain is also
            // a multi-dotted value. In this case, we should effectively treat the last two parts as the
            // schema and table respectively while concatenating the former parts as the catalog.
            final String tableName = parts[parts.length - 1];
            final String schemaName = parts[parts.length - 2];
            final String catalogName = resolveCatalogFromDomainName(parts);
            return new TableId(catalogName, schemaName, tableName);
        }
        return TableId.parse(identifier);
    }

    private static String resolveCatalogFromDomainName(String[] parts) {
        StringBuilder catalogName = new StringBuilder(parts[0]);
        for (int index = 1; index < parts.length - 2; ++index) {
            catalogName.append('.').append(parts[index]);
        }
        return catalogName.toString();
    }

}
