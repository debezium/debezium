/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * @author Randall Hauch
 *
 */
public abstract class AbstractFieldTest {

    private Field.Set allConnectorFields;

    public void setAllConnectorFields(Field.Set allConnectorFields) {
        this.allConnectorFields = allConnectorFields;
    }

    @Test
    public void shouldNotHaveDuplicateGroupOrders() {
        Map<Field.Group, Map<Integer, String>> groups = new HashMap<>();
        List<String> errors = new ArrayList<>();
        Map<Field.Group, Integer> counters = new HashMap<>();

        allConnectorFields.forEach(field -> {
            Field.GroupEntry groupEntry = field.group();
            Field.Group group = groupEntry.getGroup();
            int order = counters.merge(group, 1, Integer::sum);

            groups.computeIfAbsent(group, k -> new HashMap<>());
            if (groups.get(group).containsKey(order)) {
                errors.add("\"" + field.name() + "\" has a duplicate order in group \"" + group
                        + "\". Position no. " + order + " is already taken by field: \""
                        + groups.get(group).get(order) + "\"");
            }
            else {
                groups.get(group).put(order, field.name());
            }
        });

        if (!errors.isEmpty()) {
            fail("Duplicate field group orders found:\n" + String.join("\n", errors));
        }
    }
}
