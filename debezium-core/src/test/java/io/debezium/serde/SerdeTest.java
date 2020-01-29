/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.serde;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.serialization.Serde;
import org.fest.assertions.Assertions;
import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.debezium.data.Envelope;
import io.debezium.util.Testing;

public class SerdeTest implements Testing {

    private static final int FIELDS_IN_ENVELOPE = Envelope.ALL_FIELD_NAMES.size();

    private static final class CompositeKey {
        public int a;
        public int b;

        public CompositeKey() {
        }

        public CompositeKey(int a, int b) {
            super();
            this.a = a;
            this.b = b;
        }

        @Override
        public int hashCode() {
            return Objects.hash(a, b);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            CompositeKey other = (CompositeKey) obj;
            return a == other.a && b == other.b;
        }
    }

    private static final class Customer {
        public int id;

        @JsonProperty("first_name")
        public String firstName;

        @JsonProperty("last_name")
        public String lastName;

        public String email;

        public Customer() {
        }

        public Customer(int id, String firstName, String lastName, String email) {
            super();
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
            this.email = email;
        }

        @Override
        public int hashCode() {
            return Objects.hash(email, firstName, id, lastName);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Customer other = (Customer) obj;
            return Objects.equals(email, other.email) && Objects.equals(firstName, other.firstName) && id == other.id
                    && Objects.equals(lastName, other.lastName);
        }
    }

    @Test
    public void simpleKey() {
        final Serde<Integer> keySerde = DebeziumSerdes.payloadJson(Integer.class);
        keySerde.configure(Collections.emptyMap(), true);

        Assertions.assertThat(keySerde.deserializer().deserialize("xx", "{\"payload\": {\"a\": 1}}".getBytes())).isEqualTo(1);
        Assertions.assertThat(keySerde.deserializer().deserialize("xx", "{\"payload\": 1}".getBytes())).isEqualTo(1);
        Assertions.assertThat(keySerde.deserializer().deserialize("xx", "{\"payload\": {\"a\": null}}".getBytes())).isNull();
        Assertions.assertThat(keySerde.deserializer().deserialize("xx", "{\"payload\": null}".getBytes())).isNull();

        Assertions.assertThat(keySerde.deserializer().deserialize("xx", "{\"a\": 1}".getBytes())).isEqualTo(1);
        Assertions.assertThat(keySerde.deserializer().deserialize("xx", "1".getBytes())).isEqualTo(1);
        Assertions.assertThat(keySerde.deserializer().deserialize("xx", "{\"a\": null}".getBytes())).isNull();
        Assertions.assertThat(keySerde.deserializer().deserialize("xx", "null".getBytes())).isNull();
    }

    @Test
    public void compositeKey() {
        final Serde<CompositeKey> keySerde = DebeziumSerdes.payloadJson(CompositeKey.class);
        keySerde.configure(Collections.emptyMap(), true);
        Assertions.assertThat(keySerde.deserializer().deserialize("xx", "{\"a\": 1, \"b\": 2}".getBytes())).isEqualTo(new CompositeKey(1, 2));
    }

    @Test
    public void valuePayloadWithSchema() {
        final Serde<Customer> valueSerde = DebeziumSerdes.payloadJson(Customer.class);
        valueSerde.configure(Collections.singletonMap("from.field", "after"), false);
        final String content = Testing.Files.readResourceAsString("json/serde-with-schema.json");
        Assertions.assertThat(valueSerde.deserializer().deserialize("xx", content.getBytes())).isEqualTo(new Customer(1004, "Anne", "Kretchmar", "annek@noanswer.org"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void valueEnvelopeWithSchema() {
        final Serde<HashMap> valueSerde = DebeziumSerdes.payloadJson(HashMap.class);
        valueSerde.configure(Collections.emptyMap(), false);
        final String content = Testing.Files.readResourceAsString("json/serde-with-schema.json");
        Map<String, String> envelope = valueSerde.deserializer().deserialize("xx", content.getBytes());
        Assertions.assertThat(envelope).hasSize(FIELDS_IN_ENVELOPE - 1); // tx block not present
        Assertions.assertThat(envelope.get("op")).isEqualTo("c");
    }

    @Test
    public void valuePayloadWithoutSchema() {
        final Serde<Customer> valueSerde = DebeziumSerdes.payloadJson(Customer.class);
        valueSerde.configure(Collections.singletonMap("from.field", "after"), false);
        final String content = Testing.Files.readResourceAsString("json/serde-without-schema.json");
        Assertions.assertThat(valueSerde.deserializer().deserialize("xx", content.getBytes())).isEqualTo(new Customer(1004, "Anne", "Kretchmar", "annek@noanswer.org"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void valueEnvelopeWithoutSchema() {
        final Serde<HashMap> valueSerde = DebeziumSerdes.payloadJson(HashMap.class);
        valueSerde.configure(Collections.emptyMap(), false);
        final String content = Testing.Files.readResourceAsString("json/serde-without-schema.json");
        Map<String, String> envelope = valueSerde.deserializer().deserialize("xx", content.getBytes());
        Assertions.assertThat(envelope).hasSize(5);
        Assertions.assertThat(envelope.get("op")).isEqualTo("c");
    }

    @Test
    public void valueBeforeField() {
        final Serde<Customer> valueSerde = DebeziumSerdes.payloadJson(Customer.class);
        valueSerde.configure(Collections.singletonMap("from.field", "before"), false);

        String content = Testing.Files.readResourceAsString("json/serde-update.json");
        Assertions.assertThat(valueSerde.deserializer().deserialize("xx", content.getBytes()))
                .isEqualTo(new Customer(1004, "Anne-Marie", "Kretchmar", "annek@noanswer.org"));

        content = Testing.Files.readResourceAsString("json/serde-without-schema.json");
        Assertions.assertThat(valueSerde.deserializer().deserialize("xx", content.getBytes())).isNull();
    }

    @Test
    public void valueNull() {
        final Serde<Customer> valueSerde = DebeziumSerdes.payloadJson(Customer.class);
        valueSerde.configure(Collections.emptyMap(), false);

        Assertions.assertThat(valueSerde.deserializer().deserialize("xx", "null".getBytes())).isNull();
        Assertions.assertThat(valueSerde.deserializer().deserialize("xx", null)).isNull();
    }

    @Test
    public void valuePayloadUnwrapped() {
        final Serde<Customer> valueSerde = DebeziumSerdes.payloadJson(Customer.class);
        valueSerde.configure(Collections.emptyMap(), false);
        final String content = Testing.Files.readResourceAsString("json/serde-unwrapped.json");
        Assertions.assertThat(valueSerde.deserializer().deserialize("xx", content.getBytes())).isEqualTo(new Customer(1004, "Anne", "Kretchmar", "annek@noanswer.org"));
    }

    @Test(expected = RuntimeException.class)
    public void valueWithUnknownPropertyThrowRuntimeException() {
        final Serde<Customer> valueSerde = DebeziumSerdes.payloadJson(Customer.class);
        valueSerde.configure(Collections.singletonMap("from.field", "before"), false);

        String content = Testing.Files.readResourceAsString("json/serde-unknown-property.json");
        Assertions.assertThat(valueSerde.deserializer().deserialize("xx", content.getBytes()))
                .isEqualTo(new Customer(1004, "Anne-Marie", "Kretchmar", "annek@noanswer.org"));
    }

    @Test
    public void valueWithUnknownPropertyIgnored() {
        Map<String, Object> options = new HashMap<>();
        options.put("from.field", "before");
        options.put("unknown.properties.ignored", true);

        final Serde<Customer> valueSerde = DebeziumSerdes.payloadJson(Customer.class);
        valueSerde.configure(options, false);

        String content = Testing.Files.readResourceAsString("json/serde-unknown-property.json");
        Assertions.assertThat(valueSerde.deserializer().deserialize("xx", content.getBytes()))
                .isEqualTo(new Customer(1004, "Anne-Marie", "Kretchmar", "annek@noanswer.org"));
    }
}
