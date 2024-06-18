/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.echache;

public enum LogMinerObjectType {
    XML_WRITE,
    LOB_WRITE,
    DML,
    DML_REDO,
    DML_XML_BEGIN,
    DML_TRUNCATE,
    DML_LOB_LOCATOR_EVENT,
    XML_END,
    LOB_ERASE_EVENT
}
