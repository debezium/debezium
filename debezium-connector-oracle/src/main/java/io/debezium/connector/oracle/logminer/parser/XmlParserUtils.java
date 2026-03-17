/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.parser;

import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;

/**
 * Utility helper methods for the Oracle LogMiner XML parsing classes.
 *
 * @author Chris Cranford
 */
public class XmlParserUtils {
    /**
     * Returns whether the XML event is serialized using binary format.
     *
     * @param event the event, should not be {@code null}
     * @return {@code true} if the XML document is serialized as binary, {@code false} otherwise.
     */
    public static boolean isXmlSerializedAsBinary(LogMinerEventRow event) {
        return event.getInfo().endsWith("not re-executable");
    }
}
