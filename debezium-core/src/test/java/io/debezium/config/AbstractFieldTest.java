/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

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
    public void shouldNotAllowDuplicateGroupAssignment() {
        Map<Field.Group, Map<Integer, String>> groups = new HashMap<>();

        List<String> errors = new ArrayList<>();

        allConnectorFields.forEach(field -> {
            Field.GroupEntry currentGroupEntry = field.group();
            if (!groups.containsKey(currentGroupEntry.getGroup())) {
                groups.put(currentGroupEntry.getGroup(), new HashMap<>());
            }
            if (groups.get(currentGroupEntry.getGroup()).containsKey(currentGroupEntry.getPositionInGroup())) {
                if (currentGroupEntry.getPositionInGroup() < 9999) {
                    errors.add("\"" + field.name() + "\" uses an occupied position in group \"" + currentGroupEntry.getGroup() + "\". Position no. "
                            + currentGroupEntry.getPositionInGroup() + " is already taken by field: \""
                            + groups.get(currentGroupEntry.getGroup()).get(currentGroupEntry.getPositionInGroup()));
                }
            }
            else {
                groups.get(currentGroupEntry.getGroup()).put(currentGroupEntry.getPositionInGroup(), field.name());
            }
        });
        if (!errors.isEmpty()) {
            fail("Duplicate field group assignments found: " + String.join("\n", errors));
        }
    }
}
