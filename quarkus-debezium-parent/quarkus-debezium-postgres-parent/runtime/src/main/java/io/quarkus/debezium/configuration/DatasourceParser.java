/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.configuration;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConfiguration;

public class DatasourceParser {

    private static final Logger logger = LoggerFactory.getLogger(DatasourceParser.class);
    public static final String REGEX = "jdbc:[a-z]+://(?<hostname>[^:/;?]+)(:(?<port>\\d+))?([/;](?<dbname>[^?;]+))?";
    private static final Pattern pattern = Pattern.compile(REGEX);
    private final String value;

    public DatasourceParser(String value) {
        this.value = value;
    }

    public Optional<JdbcDatasource> asString() {
        Matcher matcher = pattern.matcher(value);

        if (matcher.find()) {
            logger.trace("Found datasource definition for {}", value);

            String host = matcher.group(JdbcConfiguration.HOSTNAME.name());
            String port = matcher.group(JdbcConfiguration.PORT.name());
            String database = matcher.group(JdbcConfiguration.DATABASE.name());

            return Optional.of(new JdbcDatasource(host, port, database));
        }

        logger.warn("Unable to parse datasource: {}", value);
        return Optional.empty();
    }

    public record JdbcDatasource(String host, String port, String database) {

    }
}
