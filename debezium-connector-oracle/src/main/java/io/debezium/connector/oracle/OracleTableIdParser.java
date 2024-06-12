/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.util.List;

import io.debezium.relational.TableId;
import io.debezium.util.Strings;

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
        return TableId.parse(identifier, false);
    }

    public static String quoteIfNeeded(TableId tableId, boolean useCatalog, boolean useSchema, List<String> keywords) {
        final StringBuilder sb = new StringBuilder();
        if (useCatalog) {
            sb.append(quotePartIfNeeded(tableId.catalog(), keywords)).append(".");
        }
        else if (useSchema) {
            sb.append(quotePartIfNeeded(tableId.schema(), keywords)).append(".");
        }
        return sb.append(quotePartIfNeeded(tableId.table(), keywords)).toString();
    }

    private static String quotePartIfNeeded(String part, List<String> keywords) {
        if (!Strings.isNullOrEmpty(part)) {
            if (part.startsWith("_") || keywords.stream().anyMatch(keyword -> keyword.equalsIgnoreCase(part))) {
                return "\"" + part + "\"";
            }
        }
        return part;
    }

    private static String resolveCatalogFromDomainName(String[] parts) {
        StringBuilder catalogName = new StringBuilder(parts[0]);
        for (int index = 1; index < parts.length - 2; ++index) {
            catalogName.append('.').append(parts[index]);
        }
        return catalogName.toString();
    }

}
