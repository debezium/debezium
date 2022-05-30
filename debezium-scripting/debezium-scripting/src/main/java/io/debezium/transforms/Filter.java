/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;

import io.debezium.common.annotation.Incubating;
import io.debezium.config.Field;

/**
 * This SMT should allow user to filter out records depending on an expression and language configured.
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Jiri Pechanec
 */
@Incubating
public class Filter<R extends ConnectRecord<R>> extends ScriptingTransformation<R> {

    public static final Field EXPRESSION = Field.create("condition")
            .withDisplayName("Filtering condition")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withDescription("An expression determining whether the record should be filtered out. When evaluated to true the record is removed.");

    @Override
    protected R doApply(R record) {
        return engine.eval(record, Boolean.class) ? record : null;
    }

    @Override
    protected Field expressionField() {
        return EXPRESSION;
    }
}
