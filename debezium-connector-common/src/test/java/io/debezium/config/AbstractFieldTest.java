/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

/**
 * @author Randall Hauch
 *
 */
public abstract class AbstractFieldTest {

    private Field.Set allConnectorFields;

    public void setAllConnectorFields(Field.Set allConnectorFields) {
        this.allConnectorFields = allConnectorFields;
    }

}
