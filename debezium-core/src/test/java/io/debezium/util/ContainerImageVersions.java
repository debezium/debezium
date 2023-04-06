/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.HttpsURLConnection;

public class ContainerImageVersions {

    private static final String BASE_URL = "https://quay.io/api/v1/repository/%s/tag/?onlyActiveTags=true";

    public static String getStableImage(String image) {
        return "quay.io/" + image + ":" + getStableVersion(image);
    }

    public static String getStableVersion(String image) {
        try {
            URL url = new URL(String.format(BASE_URL, image));
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
}
