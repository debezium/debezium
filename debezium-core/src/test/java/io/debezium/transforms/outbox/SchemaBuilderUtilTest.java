/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.outbox;

import static org.fest.assertions.Assertions.assertThat;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.doc.FixFor;

/**
 * @author vjuranek
 */
public class SchemaBuilderUtilTest {

    @Test
    @FixFor("DBZ-5475")
    public void failSchemaCheckForArrayWithDifferentNumberTypes() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode testNode = mapper.readTree("{\"test\": [1, 2.0, 3.0]}");

        RuntimeException expectedException = null;
        try {
            SchemaBuilderUtil.jsonNodeToSchema(testNode);
        }
        catch (ConnectException e) {
            expectedException = e;
        }
        assertThat(expectedException).isNotNull();
        assertThat(expectedException).isInstanceOf(ConnectException.class);
        assertThat(expectedException).hasMessage("Field is not a homogenous array (1 x 2.0), different number types (Schema{INT32} x Schema{FLOAT64})");
    }

}
