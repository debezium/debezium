/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.transforms;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ismail Simsek
 */
public class AESEncryptionTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(AESEncryptionTest.class);

    @Test
    public void shouldMatchDecryptedValue() throws Exception {
        String value = "My-data-321";
        String secretKey = "My-secret-123";

        AESEncryption util = new AESEncryption(secretKey);
        String encrypted = util.encrypt(value);
        System.out.println(encrypted);
        assertThat(util.decrypt(encrypted)).isEqualTo(value);

        encrypted = util.encrypt("TEST-123");
        assertThat(util.decrypt(encrypted)).isEqualTo(util.decrypt(encrypted));
        encrypted = util.encrypt(null);
        assertThat(util.decrypt(encrypted)).isEqualTo(util.decrypt(null));
    }
}
