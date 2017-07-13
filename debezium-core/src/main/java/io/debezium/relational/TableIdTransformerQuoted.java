/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

/**
 * Generic implementation of a TrableIdTransformer that accepts either
 * a quote character to use for the start and end of every identifier
 * part or two separate identifiers to use for start and end of each
 * part.
 *
 */
public class TableIdTransformerQuoted implements TableIdTransformer {

    private final String identifierStart;
    private final String identifierEnd;

    public TableIdTransformerQuoted(String quoteChar) {
        this(quoteChar, quoteChar);
    }

    public TableIdTransformerQuoted(String identifierStart, String identifierEnd) {
        this.identifierStart = identifierStart;
        this.identifierEnd = identifierEnd;
    }

    @Override
    public String fromSqlQuoted(String part) {
        if ( part.startsWith(identifierStart) && part.endsWith(identifierEnd)) {
            return part.substring(identifierStart.length(), part.length()-identifierEnd.length());
        }
        return part;
    }

    @Override
    public String toSqlQuoted(String part) {
        if ( !part.startsWith(identifierStart) && !part.endsWith(identifierEnd)) {
            return identifierStart + part + identifierEnd;
        }
        return part;
    }
}
