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
package io.debezium.server.redis;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

/** 
 * Implementation of OffsetBackingStore that saves to Redis
 * @author Oren Elias
 */

public class RedisOffsetBackingStore extends MemoryOffsetBackingStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisOffsetBackingStore.class);

    private static final String PROP_PREFIX = "offset.storage.redis.";
    private static final String PROP_ADDRESS = PROP_PREFIX + "address";
    private static final String PROP_USER = PROP_PREFIX + "user";
    private static final String PROP_PASSWORD = PROP_PREFIX + "password";
    private static final String PROP_KEY_NAME = PROP_PREFIX + "key_name";

    private HostAndPort address;
    private Optional<String> user;
    private Optional<String> password;
    private String keyName;

    private Jedis client = null;

    public RedisOffsetBackingStore() {

    }

    void connect() {
        if (this.client != null) {
            try {
                client.ping();
                return;
            }
            catch (Exception e) {
                LOGGER.warn("Invalid connection to redis. Reconnecting.");
            }
        }

        client = new Jedis(address);
        // connect using only user or user/pass combination
        if (user.isPresent()) {
            client.auth(user.get(), password.get());
        }
        else if (password.isPresent()) {
            client.auth(password.get());
        }
        // make sure that client is connected
        client.ping();

        LOGGER.info("Using default Jedis '{}'", client);
    }

    private Map<String, String> config;

    @Override
    public void configure(WorkerConfig config) {
        super.configure(config);
        this.config = config.originalsStrings();
        address = HostAndPort.from(this.config.getOrDefault(PROP_ADDRESS, "localhost:6379"));
        user = Optional.ofNullable(this.config.get(PROP_USER));
        password = Optional.ofNullable(this.config.get(PROP_PASSWORD));
        keyName = this.config.getOrDefault(PROP_KEY_NAME, "offsets");
    }

    @Override
    public synchronized void start() {
        super.start();
        LOGGER.info("Starting RedisOffsetBackingStore");
        this.connect();
        this.load();
    }

    @Override
    public synchronized void stop() {
        super.stop();
        // Nothing to do since this doesn't maintain any outstanding connections/data
        LOGGER.info("Stopped RedisOffsetBackingStore");
    }

    /**
    * Load offsets from redis keys
    */
    private void load() {
        Map<String, String> offsets = client.hgetAll(keyName);
        this.data = new HashMap<>();
        for (Map.Entry<String, String> mapEntry : offsets.entrySet()) {
            ByteBuffer key = (mapEntry.getKey() != null) ? ByteBuffer.wrap(mapEntry.getKey().getBytes()) : null;
            ByteBuffer value = (mapEntry.getValue() != null) ? ByteBuffer.wrap(mapEntry.getValue().getBytes()) : null;
            data.put(key, value);
        }
    }

    /**
    * Save offsets to redis keys
    */
    @Override
    protected void save() {
        for (Map.Entry<ByteBuffer, ByteBuffer> mapEntry : data.entrySet()) {
            try {
                byte[] key = (mapEntry.getKey() != null) ? mapEntry.getKey().array() : null;
                byte[] value = (mapEntry.getValue() != null) ? mapEntry.getValue().array() : null;
                client.hset(keyName.getBytes(), key, value);
            }
            catch (JedisException e) {
                throw new ConnectException(e);
            }
        }
    }
}
