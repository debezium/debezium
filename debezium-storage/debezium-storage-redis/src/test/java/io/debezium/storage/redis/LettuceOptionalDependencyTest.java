/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

/**
 * {@code lettuce-core} is an optional dependency, so classes that are loaded on the default (Jedis) path
 * must not carry any reference to a Lettuce type. A stray import would only surface at runtime, as a
 * {@link NoClassDefFoundError} in a deployment that never asked for Lettuce, so it is checked here instead.
 */
public class LettuceOptionalDependencyTest {

    private static final String LETTUCE_PACKAGE = "io/lettuce/";

    @Test
    public void classesOnTheDefaultPathMustNotReferenceLettuce() throws IOException {
        for (Class<?> clazz : new Class<?>[]{ RedisConnection.class, RedisCommonConfig.class, RedisClientLibrary.class, JedisClient.class }) {
            assertFalse(referencesLettuce(clazz), clazz.getSimpleName() + " must not reference any io.lettuce type; "
                    + "keep Lettuce usage inside LettuceClient so the optional dependency stays optional");
        }
    }

    @Test
    public void lettuceClientItselfDoesReferenceLettuce() throws IOException {
        // Guards the check above against silently passing because the scan itself is broken.
        assertTrue(referencesLettuce(LettuceClient.class), "LettuceClient is expected to reference io.lettuce types");
    }

    private boolean referencesLettuce(Class<?> clazz) throws IOException {
        String resource = clazz.getName().replace('.', '/') + ".class";
        try (InputStream in = clazz.getClassLoader().getResourceAsStream(resource)) {
            assertNotNull(in, "Could not read the compiled class file for " + clazz.getName());
            // Class names appear verbatim in the constant pool, which is what the JVM resolves against.
            return new String(in.readAllBytes(), StandardCharsets.ISO_8859_1).contains(LETTUCE_PACKAGE);
        }
    }
}
