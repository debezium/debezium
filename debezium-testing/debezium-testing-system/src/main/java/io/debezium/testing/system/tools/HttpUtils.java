/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools;

import static io.debezium.testing.system.tools.WaitConditions.scaled;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

/**
 * Utility methods for HTTP requests
 * @author Jakub Cechacek
 */
public class HttpUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpUtils.class);

    private final OkHttpClient http;

    public HttpUtils(OkHttpClient http) {
        this.http = http;
    }

    /**
     * Waits until URL starts responding with success response code
     * @param url tested url
     */
    public void awaitApi(HttpUrl url) {
        LOGGER.info("Waiting for API at " + url);
        await()
                .atMost(scaled(1), TimeUnit.MINUTES)
                .ignoreException(IOException.class)
                .until(() -> pingApi(url));
    }

    private boolean pingApi(HttpUrl address) throws IOException {
        Request r = new Request.Builder().url(address).build();
        try (Response res = http.newCall(r).execute()) {
            return res.isSuccessful();
        }
    }
}
