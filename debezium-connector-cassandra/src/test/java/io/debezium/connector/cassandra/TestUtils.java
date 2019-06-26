/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import com.google.cloud.storage.StorageException;

/**
 * Miscellaneous utility methods for unit testing.
 */
public final class TestUtils {

    private TestUtils() { }

    /**
     * Generate a {@link StorageException} with a specified error code
     * @param code error code
     * @return a storage exception
     */
    public static StorageException generateGoogleStorageException(int code) {
        GoogleJsonError jsonError = new GoogleJsonError();
        jsonError.setCode(code);
        jsonError.setMessage("n/a");
        HttpResponseException.Builder builder = new HttpResponseException.Builder(code, "n/a", new HttpHeaders());
        GoogleJsonResponseException jsonResponseException = new GoogleJsonResponseException(builder, jsonError);
        return new StorageException(jsonResponseException);
    }
}
