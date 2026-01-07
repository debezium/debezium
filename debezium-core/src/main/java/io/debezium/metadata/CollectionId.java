/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metadata;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import io.debezium.util.Strings;

/**
 * An abstract data model that represents a table or collection of a datastore in its hierarchy.
 * (e.g. server/catalog/database, schema, table).
 * <br>
 * It can either have two levels (namespace and name) or three levels (realm, namespace and name),
 * depending on the requirements of underlying datastore:
 * <br>
 * realm: The optional highest layer, used for datastores that have a concept of three layers.
 *        For example MySQL and Oracle have database, schema and table, where database would be the highest layer
 *        and will be represented by realm.
 * <br>
 * namespace: The second-highest layer, used for datastores that have a concept of three or two layers.
 *            For example MySQL and Oracle have database, schema and table, where schema would be the second layer
 *            and will be represented by namespace.
 *            PostgreSQL only has schema and table, then schema will be represented by namespace.
 * <br>
 * name: The lowest layer, used for datastores that have a concept of three, two or one single layer hierarchy.
 *       This is usually the table layer of a relational database or the collection in MongoDB or similar datastores
 *       (e.g. document stores, key-value stores), this layer will then be represented by name.
 * <br>
 * Also acts as model class representing the structure of the response for the REST Extension call that returns the
 * matching tables/collections on the `validate filters` endpoint.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CollectionId {

    /**
     * catalog, schema or replica set name
     */
    private final String namespace;

    /**
     * table or collection
     */
    private final String name;

    /**
     * optional database or schema name
     */
    private final String realm;

    public CollectionId(String realm, String namespace, String name) {
        if ("null".equalsIgnoreCase(realm)) {
            this.realm = null;
        }
        else {
            this.realm = realm;
        }
        if ("null".equalsIgnoreCase(namespace)) {
            this.namespace = null;
        }
        else {
            this.namespace = namespace;
        }
        if ("null".equalsIgnoreCase(name)) {
            this.name = null;
        }
        else {
            this.name = name;
        }
    }

    public CollectionId(String namespace, String name) {
        this(null, namespace, name);
    }

    public CollectionId(String name) {
        this(null, null, name);
    }

    @JsonGetter("identifier")
    public String identifier() {
        return toFullIdentiferString();
    }

    @JsonGetter("realm")
    public String realm() {
        return this.realm;
    }

    @JsonGetter("namespace")
    public String namespace() {
        return this.namespace;
    }

    @JsonGetter("name")
    public String name() {
        return this.name;
    }

    @Override
    @JsonIgnore
    public String toString() {
        return "CollectionId{" +
                "realm='" + realm + '\'' +
                ", namespace='" + namespace + '\'' +
                ", name='" + name + '\'' +
                '}';
    }

    @JsonIgnore
    public String toFullIdentiferString() {
        var sb = new StringBuilder();
        if (null != this.realm && !"null".equals(this.realm)) {
            sb.append(this.realm).append(".");
        }
        if (null != this.namespace && !"null".equals(this.namespace)) {
            sb.append(this.namespace).append(".");
        }
        sb.append(this.name);
        return sb.toString();
    }

    public CollectionId toUpperCase() {
        return new CollectionId(
                Strings.isNullOrBlank(realm) ? realm : realm.toUpperCase(),
                Strings.isNullOrBlank(namespace) ? namespace : namespace.toUpperCase(),
                Strings.isNullOrBlank(name) ? name : name.toUpperCase());
    }

    public CollectionId toLowerCase() {
        return new CollectionId(
                Strings.isNullOrBlank(realm) ? realm : realm.toLowerCase(),
                Strings.isNullOrBlank(namespace) ? namespace : namespace.toLowerCase(),
                Strings.isNullOrBlank(name) ? name : name.toLowerCase());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CollectionId tableId = (CollectionId) o;
        return Objects.equals(realm, tableId.realm)
                && Objects.equals(namespace, tableId.namespace)
                && Objects.equals(name, tableId.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(realm, namespace, name);
    }

}
