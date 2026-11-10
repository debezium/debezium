/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;

import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.config.Field;

import groovy.lang.Closure;

/**
 * This SMT should allow user transform records with a script expression.
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation
 *            will operate
 * @author Felix Dörre
 */
@Incubating
public class ColumnExpression<R extends ConnectRecord<R>> extends ScriptingTransformation<R> {
    private static final String GROOVY = "jsr223.groovy";

    public static final Field EXPRESSION = Field.create("expression").withDisplayName("Transformation expression")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .required().withDescription(
                    "An expression determining how a record should be transformed.");
    public static final Field HEADER = Field.create("header").withDisplayName("Script header, e.g. for includes.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .optional()
            .withDefault("")
            .withDescription(
                    "A script snippet for initializing the script context for the transformation expression.");

    private String header;
    private String language;

    private SmtManager<R> smtManager;

    @Override
    public void configure(Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        smtManager = new SmtManager<R>(config);
        language = config.getString(LANGUAGE);
        header = config.getString(HEADER);
        super.configure(configs);
    }

    @Override
    protected String getExpression(Configuration config, String language) {
        if (GROOVY.equals(language)) {
            return header
                    + "\ndef transform(org.apache.kafka.connect.connector.ConnectRecord record, org.apache.kafka.connect.data.Struct row) {\n"
                    + super.getExpression(config, language) + "\n}\n" + "\n"
                    + "return {org.apache.kafka.connect.connector.ConnectRecord record, "
                    + "org.apache.kafka.connect.data.Struct row -> transform(record, row)}";
        }
        if ("jsr223.graal.js".equals(language)) {
            return header + "\nfunction(record, value){\n" + super.getExpression(config, language) + "\n}";
        }
        return super.getExpression(config, language);
    }

    private Closure<?> groovyExpression = null;
    private Function<Object[], Object> graalExpression = null;

    @Override
    protected R doApply(R record) {
        if (!smtManager.isValidEnvelope(record)) {
            return record;
        }
        if (groovyExpression == null && graalExpression == null) {
            if (GROOVY.equals(language)) {
                groovyExpression = engine.eval(record, Closure.class);
            }
            else {
                graalExpression = (Function<Object[], Object>) engine.eval(record, Function.class);
            }
        }
        Struct value = (Struct) record.value();
        if (value == null) {
            Object result = evalExpression(record, null);
            return (R) (ConnectRecord<R>) result;
        }
        Struct before = value.getStruct("before");
        if (before != null) {
            Object result = evalExpression(record, engine.asProxyObject(before));
            if (result instanceof ConnectRecord) {
                return (R) result;
            }
            for (Entry<?, ?> e : ((Map<?, ?>) result).entrySet()) {
                before.put((String) e.getKey(), e.getValue());
            }
        }
        Struct after = value.getStruct("after");
        if (after != null) {
            Object result = evalExpression(record, engine.asProxyObject(after));
            if (result instanceof ConnectRecord) {
                return (R) result;
            }
            for (Entry<?, ?> e : ((Map<?, ?>) result).entrySet()) {
                after.put((String) e.getKey(), e.getValue());
            }
        }
        return record;
    }

    private Object evalExpression(R record, Object row) {
        if (graalExpression != null) {
            return graalExpression.apply(new Object[]{ record, row });
        }
        return groovyExpression.call(record, row);
    }

    @Override
    public ConfigDef config() {
        final ConfigDef config = super.config();
        Field.group(config, null, HEADER);
        return config;
    }

    @Override
    protected Field expressionField() {
        return EXPRESSION;
    }

}
