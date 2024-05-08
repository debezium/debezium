/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * @author Jakub Cechace
 */
public final class YAML {
    /**
     *  Deserialize object fromResource YAML file
     *
     * @param path file path
     * @param c type of object
     * @return deserialized object
     */
    public static <T> T from(String path, Class<T> c) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            return mapper.readValue(new File(path), c);
        }
        catch (InvalidFormatException e) {
            throw new IllegalArgumentException(e);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T fromResource(String path, Class<T> c) {
        URL resource = YAML.class.getResource(path);
        return from(resource.getFile(), c);
    }
}
