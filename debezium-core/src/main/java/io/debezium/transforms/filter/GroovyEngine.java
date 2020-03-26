/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.filter;

import org.apache.kafka.connect.connector.ConnectRecord;

import io.debezium.DebeziumException;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.Script;

/**
 * An implementation of the expression language evaluator based on Groovy scripting languages.
 * The expression receives variables to work with
 * <ul>
 * <li>key - key of the record</li>
 * <li>value - value of the record</li>
 * <li>keySchema - schema for key</li>
 * <li>valueSchema - schema for value</li>
 * </ul>
 *
 * @author Jiri Pechanec
 */
public class GroovyEngine implements Engine {

    private String expression;
    private Script script;

    @Override
    public void parseExpression(String expression) {
        this.expression = expression;
        final GroovyShell shell = new GroovyShell();
        script = shell.parse(expression);
    }

    @Override
    public boolean eval(ConnectRecord<?> record) {
        final Binding binding = new Binding();
        binding.setVariable("key", record.key());
        binding.setVariable("value", record.value());
        binding.setVariable("keySchema", record.keySchema());
        binding.setVariable("valueSchema", record.valueSchema());
        script.setBinding(binding);
        try {
            final Object result = script.run();
            if (result instanceof Boolean) {
                return (Boolean) result;
            }
            else {
                throw new DebeziumException("Value '" + result + "' returned by the condition is not a boolean");
            }
        }
        catch (Exception e) {
            throw new DebeziumException("Error while evaluating expression '" + expression + "' for record '" + record + "'", e);
        }
    }

}
