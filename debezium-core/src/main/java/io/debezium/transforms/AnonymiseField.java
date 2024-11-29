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
package io.debezium.transforms;

import static java.lang.String.format;
import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.Module;

/**
 * This SMT hashes given field values, with given hashing algorithm, useful when sensitive values needs to be pseudo anonymised.
 * Hashing and Encrypting is a technique that further straightens data privacy
 *
 * @author Ismail Simsek
 */
public abstract class AnonymiseField<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final Logger LOGGER = LoggerFactory.getLogger(AnonymiseField.class);
    public static final String OVERVIEW_DOC = "Hash specified fields. hashing algorithm can be set using `hashing` setting." +
            "<p/>Use the concrete transformation type designed for the record key (<code>" + AnonymiseField.Key.class.getName()
            + "</code>) or value (<code>" + AnonymiseField.Value.class.getName() + "</code>).";
    private static final String FIELDS_CONFIG = "fields";
    private static final String HASHING_CONFIG = "hashing";
    private static final String STRIP_CONFIG = "strip";
    private static final String AES_SECRET_CONFIG = "aes.secret";

    private enum hashingOption {
        MD5,
        SHA256,
        SHA256_AES,
        MD5_AES
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyListValidator(),
                    ConfigDef.Importance.HIGH, "Names of fields to anonymise.")
            .define(HASHING_CONFIG, ConfigDef.Type.STRING, hashingOption.MD5.name(), new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.LOW, "The hashing algorithm, default is" + hashingOption.MD5.name())
            .define(STRIP_CONFIG, ConfigDef.Type.BOOLEAN, true,
                    ConfigDef.Importance.LOW, "Flag whether to run strip on input value!")
            .define(AES_SECRET_CONFIG, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.LOW, "Secret value for AES encryption");

    private static final String PURPOSE = "anonymise fields";

    private Set<String> anonymisedFields;
    private hashingOption hashingOptionAlgo;
    private boolean doStripValue;
    private boolean doLowercaseValue = true;
    private Cache<Schema, Schema> schemaUpdateCache;
    private AESEncryption encrypter;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        anonymisedFields = new HashSet<>(config.getList(FIELDS_CONFIG));
        hashingOptionAlgo = hashingOption.valueOf(config.getString(HASHING_CONFIG));
        doStripValue = config.getBoolean(STRIP_CONFIG);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
        String aesSecret = config.getString(AES_SECRET_CONFIG);

        if (Set.of(hashingOption.MD5_AES, hashingOption.SHA256_AES).contains(hashingOptionAlgo) && aesSecret == null) {
            throw new ConfigException(format("'%s' config must be set and should not be empty", AES_SECRET_CONFIG));
        }
        encrypter = new AESEncryption(aesSecret);
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        }
        else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        final HashMap<String, Object> updatedValue = new HashMap<>(value);
        for (String field : anonymisedFields) {
            updatedValue.put(field, anonymise(value.get(field)));
        }
        return newRecord(record, null, updatedValue);
    }

    private Struct updatedValue(Schema schema) {
        Schema updatedSchema = schemaUpdateCache.get(schema);
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(schema);
            schemaUpdateCache.put(schema, updatedSchema);
        }
        return new Struct(updatedSchema);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
        final Struct updatedValue = updatedValue(value.schema());

        for (Field field : updatedValue.schema().fields()) {
            final Object origFieldValue = value.get(field);
            if (anonymisedFields.contains(field.name())) {
                updatedValue.put(field, anonymise(origFieldValue));
            }
            else {
                updatedValue.put(field, origFieldValue);
            }
        }
        return newRecord(record, updatedValue.schema(), updatedValue);
    }

    public String cleanValue(Object value) {
        String valString = doStripValue ? String.valueOf(value).strip() : String.valueOf(value);
        return doLowercaseValue ? valString.toLowerCase() : valString;
    }

    private Object anonymise(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof List || value instanceof Map)
            throw new DataException("Cannot hash value of type: " + value.getClass());

        String valString = cleanValue(value);
        try {
            switch (hashingOptionAlgo) {
                case MD5:
                    return md5(valString);
                case SHA256:
                    return sha256(valString);
                case SHA256_AES:
                    return encrypter.encrypt(sha256(valString));
                case MD5_AES:
                    return encrypter.encrypt(md5(valString));
                default:
                    throw new IllegalArgumentException("`hashing` value should be one of [" + hashingOption.MD5.name() + ", " + hashingOption.SHA256.name() + "]");
            }
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }
    }

    public Schema makeUpdatedSchema(Schema schema) {
        SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        // copy over fields from original schema
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            if (anonymisedFields.contains(field.name()) && field.schema().type() != Schema.Type.STRING) {
                // Change field schema to Schema.Type.STRING for anonymised fields!
                builder.field(field.name(), makeStringFieldSchema(field.schema()));
                continue;
            }

            builder.field(field.name(), field.schema());
        }

        return builder.build();
    }

    /**
     * Creates copy of the schema in type of {@link Schema.Type#STRING}. Because hashing will result string value output.
     *
     * @param source
     * @return
     */
    protected Schema makeStringFieldSchema(Schema source) {
        // copy over schema properties
        SchemaBuilder fieldNewSchema = SchemaUtil.copySchemaBasics(source, SchemaBuilder.string());

        // copy over isOptional property
        if (source.isOptional()) {
            fieldNewSchema.optional();
        }

        // copy over defaultValue property
        if (source.defaultValue() != null) {
            fieldNewSchema.defaultValue(anonymise(source.defaultValue()));
        }

        // return new schema for the anonymised field with Schema.Type.STRING type!
        return fieldNewSchema.build().schema();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void close() {
    }

    public static String md5(String input) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] messageDigest = md.digest(input.getBytes());

        // Convert byte array into signum representation
        BigInteger no = new BigInteger(1, messageDigest);

        // Convert message digest into hex value
        String hashtext = no.toString(16);
        while (hashtext.length() < 32) {
            hashtext = "0" + hashtext;
        }
        return hashtext.toLowerCase();
    }

    public static String sha256(String input) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] messageDigest = md.digest(input.getBytes());

        // Convert byte array into signum representation
        BigInteger no = new BigInteger(1, messageDigest);

        // Convert message digest into hex value
        String hashtext = no.toString(16);
        while (hashtext.length() < 64) {
            hashtext = "0" + hashtext;
        }
        return hashtext.toLowerCase();
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static final class Key<R extends ConnectRecord<R>> extends AnonymiseField<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static final class Value<R extends ConnectRecord<R>> extends AnonymiseField<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }

}
