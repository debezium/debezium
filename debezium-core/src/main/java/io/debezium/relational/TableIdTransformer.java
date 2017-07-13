/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

/**
 * Interface that allows transformation of table identifier parts
 * to support escaping and unescaping quoted identifiers.
 *
 */
public interface TableIdTransformer {
    String fromSqlQuoted(String part);

    String toSqlQuoted(String part);
}
