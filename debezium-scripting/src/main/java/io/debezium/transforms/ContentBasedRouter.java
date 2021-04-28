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
 * This SMT allows to route records to specific topics depending on their
 * content, an expression and language configured.
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation
 *            will operate
 * @author Jiri Pechanec
 */
@Incubating
public class ContentBasedRouter<R extends ConnectRecord<R>> extends ScriptingTransformation<R> {

    public static final Field EXPRESSION = Field.create("topic.expression")
            .withDisplayName("Topic name expression")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withDescription("An expression determining the new name of the topic the record should use. When null the record is delivered to the original topic.");

    @Override
    protected R doApply(R record) {
        final String topicName = engine.eval(record, String.class);
        return topicName == null ? record
                : record.newRecord(
                        topicName,
                        record.kafkaPartition(),
                        record.keySchema(),
                        record.key(),
                        record.valueSchema(),
                        record.value(),
                        record.timestamp(),
                        record.headers());
    }

    @Override
    protected Field expressionField() {
        return EXPRESSION;
    }
}
