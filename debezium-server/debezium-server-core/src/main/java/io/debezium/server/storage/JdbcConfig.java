/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.debezium.server.storage;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.runtime.WorkerConfig;

import java.util.Map;

import static io.debezium.server.storage.JdbcOffsetBackingStore.*;

public class JdbcConfig extends WorkerConfig {
    private static final ConfigDef CONFIG;

    static {
        // @TODO add replication database! store per replication_id
        // todo read table name from config
        CONFIG = baseConfigDef()
                .define(JDBC_URI.name(),
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "Jdbc database uri")
                .define(JDBC_USER.name(),
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "Jdbc database username")
                .define(JDBC_PASSWORD.name(),
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "Jdbc database password")
        ;
    }

    public JdbcConfig(Map<String, String> props) {
        super(CONFIG, props);
    }
}
