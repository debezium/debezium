/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.rest.model;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Base class for JSON payloads describing connector metrics
 *
 * @author Anisha Mohanty
 */
public class MetricsDescriptor {

    @JsonProperty("name")
    private String name;

    @JsonProperty("tasks.max")
    private String tasksMax;

    @JsonProperty("connector")
    private Connector connector;

    @JsonProperty("tasks")
    private List<Task> tasks;

    public MetricsDescriptor(String name, String tasksMax, Connector connector, List<Task> tasks) {
        this.name = name;
        this.tasksMax = tasksMax;
        this.connector = connector;
        this.tasks = tasks;
    }

    public static class Connector {
        @JsonProperty()
        private Map<String, String> metrics;

        public Connector(Map<String, String> metrics) {
            this.metrics = metrics;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class Task {

        @JsonProperty("id")
        private int id;

        @JsonProperty("namespaces")
        private List<Namespace> namespaces;

        public Task(int id, List<Namespace> namespaces) {
            this.id = id;
            this.namespaces = namespaces;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class Namespace {
        @JsonProperty("name")
        private String name;

        @JsonProperty()
        private Map<String, String> metrics;

        public Namespace(String name, Map<String, String> metrics) {
            this.name = name;
            this.metrics = metrics;
        }
    }
}
