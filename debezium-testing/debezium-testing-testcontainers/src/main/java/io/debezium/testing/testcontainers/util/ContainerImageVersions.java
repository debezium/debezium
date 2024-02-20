/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.HttpsURLConnection;

public class ContainerImageVersions {

    private static final String QUAY_IO_REGISTRY = "quay.io/";
    private static final String QUAY_URL = "https://quay.io/api/v1/repository/%s/tag/?onlyActiveTags=true";
    private static final String DOCKER_HUB_URL = "https://hub.docker.com/v2/repositories/%s/tags/";

    private static final String VERSION_PROPERTY_PREFIX = "debezium.testcontainers.version";

    public static String getStableImage(String image) {
        return image + ":" + getStableVersion(image);
    }

    public static String getStableVersion(String image) {
        if (image.startsWith(QUAY_IO_REGISTRY)) {
            image = image.substring(QUAY_IO_REGISTRY.length());
        }

        return getStableVersionFromProperty(image).orElse(getStableVersionFromAnyRegistry(image));
    }

    public static String getStableVersionFromQuay(String name) {
        return getStableVersionFromRegistry(QUAY_URL, name);
    }

    public static String getStableVersionFromDockerHub(String name) {
        return getStableVersionFromRegistry(DOCKER_HUB_URL, name);
    }

    public static String getStableVersionFromRegistry(String baseUrl, String image) {
        try {
            URL url = new URL(String.format(baseUrl, image));
            HttpsURLConnection httpsURLConnection = (HttpsURLConnection) url.openConnection();
            httpsURLConnection.setRequestMethod("GET");

            int responseCode = httpsURLConnection.getResponseCode();

            if (responseCode == HttpsURLConnection.HTTP_OK) {
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(httpsURLConnection.getInputStream()));
                String content;
                StringBuilder response = new StringBuilder();

                while ((content = bufferedReader.readLine()) != null) {
                    response.append(content);
                }

                Pattern pattern = Pattern.compile("\\d.\\d.\\d.Final");
                Matcher matcher = pattern.matcher(response);

                List<String> stableVersionList = new ArrayList<>();

                while (matcher.find()) {
                    stableVersionList.add(matcher.group());
                }

                Collections.sort(stableVersionList);
                return stableVersionList.get(stableVersionList.size() - 1);
            }
            else {
                throw new RuntimeException("Couldn't obtain stable version for image " + image);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Optional<String> getStableVersionFromProperty(String image) {
        var propImageName = image.replace("/", ".");
        var version = System.getProperty(VERSION_PROPERTY_PREFIX + "." + propImageName);

        return Optional.ofNullable(version);
    }

    private static String getStableVersionFromAnyRegistry(String name) {
        try {
            return getStableVersionFromQuay(name);
        }
        catch (Exception e) {
            return getStableVersionFromDockerHub(name);
        }
    }
}
