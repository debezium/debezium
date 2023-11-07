/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.rest;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class DataCollection {

    // catalog, schema or replica set name
    private final String namespace;
    // table or document
    private final String name;
    // optional database or schema name
    private final String realm;

    public DataCollection(String namespace, String name) {
        this.namespace = namespace;
        this.name = name;
        this.realm = null;
    }

    public DataCollection(String realm, String namespace, String name) {
        this.realm = realm;
        this.namespace = namespace;
        this.name = name;
    }

    @JsonGetter("identifier")
    public String identifier() {
        if (null == this.realm || "null".equals(this.realm)) {
            return this.namespace + "." + this.name;
        }
        else {
            return this.realm + "." + this.namespace + "." + this.name;
        }
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
    public String getName() {
        return this.name;
    }

    @Override
    public String toString() {
        return "DataCollection{" +
                "realm='" + realm + '\'' +
                ", namespace='" + namespace + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}