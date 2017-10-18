package org.telmate.SMT;

import org.apache.kafka.connect.data.Schema;

import java.util.Date;

/**
 * Created by svegesna on 9/29/17.
 */
public interface TimestampTranslator {
    /**
     * Convert from the type-specific format to the universal java.util.Date format
     */
    Date toRaw(DebeziumTimestampConverter.Config config, Object orig);

    /**
     * Get the schema for this format.
     */
    Schema typeSchema();

    /**
     * Convert from the universal java.util.Date format to the type-specific format
     */
    Object toType(DebeziumTimestampConverter.Config config, Date orig,String format);

    Schema optionalSchema();
}


