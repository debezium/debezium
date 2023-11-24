/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.rest.jolokia;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JolokiaAttributes {
    private List<String> attributeNames = new ArrayList<>();

    public JolokiaAttributes(String attributesFilePath) {
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream(attributesFilePath)) {
            if (stream != null) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                    String attribute;
                    while ((attribute = reader.readLine()) != null) {
                        attributeNames.add(attribute);
                    }
                }
            }
            else {
                attributeNames = Collections.emptyList();
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> getAttributeNames() {
        return attributeNames;
    }
}
