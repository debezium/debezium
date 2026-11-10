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
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Felix Dörre
 */
@Incubating
public class ColumnExpression<R extends ConnectRecord<R>> extends ScriptingTransformation<R> {
    private static final String GROOVY = "jsr223.groovy";

    public static final Field EXPRESSION = Field.create("expression").withDisplayName("Filtering condition")
            .withType(ConfigDef.Type.STRING).withWidth(ConfigDef.Width.MEDIUM).withImportance(ConfigDef.Importance.HIGH)
            .required().withDescription(
                    "An expression determining whether the record should be filtered out. When evaluated to true the record is removed.");

    public static final Field COMPILE_STATIC = Field.create("compile.static").withDisplayName("Compile static")
            .withType(ConfigDef.Type.BOOLEAN).withImportance(ConfigDef.Importance.MEDIUM).withDefault(false)
            .withDescription(
                    "A boolean flag, whether the groovy template should include CompileStatic, to increase performance.");

    private boolean compileStatic = false;
    private String language;

    @Override
    public void configure(Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        language = config.getString(LANGUAGE);
        compileStatic = config.getBoolean(COMPILE_STATIC);
        super.configure(configs);
    }

    @Override
    protected String getExpression(Configuration config, String language) {
        if (GROOVY.equals(language)) {
            return (compileStatic ? "@groovy.transform.CompileStatic\n" : "")
                    + "def transform(org.apache.kafka.connect.connector.ConnectRecord record, org.apache.kafka.connect.data.Struct row) {\n"
                    + super.getExpression(config, language) + "\n}\n" + "\n"
                    + "return {org.apache.kafka.connect.connector.ConnectRecord record, "
                    + "org.apache.kafka.connect.data.Struct row -> transform(record, row)}";
        }
        if ("jsr223.graal.js".equals(language)) {
            return "function(record, value){\n" + super.getExpression(config, language) + "\n}";
        }
        return super.getExpression(config, language);
    }

    private Closure<?> groovyExpression = null;
    private Function<Object[], Object> graalExpression = null;

    @Override
    protected R doApply(R record) {
        Struct value = (Struct) record.value();
        Struct before = value.getStruct("before");
        if (groovyExpression == null && graalExpression == null) {
            if (GROOVY.equals(language)) {
                groovyExpression = engine.eval(record, Closure.class);
            }
            else {
                graalExpression = (Function<Object[], Object>) engine.eval(record, Function.class);
            }
        }
        if (before != null) {
            for (Entry<?, ?> e : evalExpression(record, before).entrySet()) {
                before.put((String) e.getKey(), e.getValue());
            }
        }
        Struct after = value.getStruct("after");
        if (after != null) {
            for (Entry<?, ?> e : evalExpression(record, after).entrySet()) {
                after.put((String) e.getKey(), e.getValue());
            }
        }
        return record;
    }

    private Map<?, ?> evalExpression(R record, Struct row) {
        if (graalExpression != null) {
            return (Map<?, ?>) graalExpression.apply(new Object[]{ record, row });
        }
        return (Map<?, ?>) groovyExpression.call(record, row);
    }

    @Override
    protected Field expressionField() {
        return EXPRESSION;
    }

}
