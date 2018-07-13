/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Encapsulates a set of a database's system variables.
 *
 * @author Randall Hauch
 */
public class SystemVariables {

    /**
     * Interface that is used for enums defining the customized scope values for specific DBMSs.
     */
    public interface Scope {
        int priority();
    }

    public enum DefaultScope implements Scope {
        DEFAULT_SCOPE(100);

        private int priority;

        DefaultScope(int priority) {
            this.priority = priority;
        }

        @Override
        public int priority() {
            return priority;
        }
    }

    private final Map<Scope, ConcurrentMap<String, String>> systemVariables = new ConcurrentHashMap<>();

    /**
     * Create an instance.
     */
    public SystemVariables() {
        systemVariables.put(DefaultScope.DEFAULT_SCOPE, new ConcurrentHashMap<>());
    }

    public SystemVariables(Scope[] scopes) {
        for (Scope scope : scopes) {
            systemVariables.put(scope, new ConcurrentHashMap<>());
        }
    }

    public SystemVariables(List<Scope> scopes) {
        for (Scope scope : scopes) {
            systemVariables.put(scope, new ConcurrentHashMap<>());
        }
    }

    /**
     * Set the variable with the specified scope.
     *
     * @param scope the variable scope; may be null if the session scope is to be used
     * @param name  the name of the variable; may not be null
     * @param value the variable value; may be null if the value for the named variable is to be removed
     * @return this object for method chaining purposes; never null
     */
    public SystemVariables setVariable(Scope scope, String name, String value) {
        name = variableName(name);
        if (value != null) {
            forScope(scope).put(name, value);
        }
        else {
            forScope(scope).remove(name);
        }
        return this;
    }

    /**
     * Get the variable with the specified name and scope.
     *
     * @param name  the name of the variable; may not be null
     * @param scope the variable scope; may not be null
     * @return the variable value; may be null if the variable is not currently set
     */
    public String getVariable(String name, Scope scope) {
        name = variableName(name);
        return forScope(scope).get(name);
    }

    /**
     * Get the variable with the specified name, from the highest priority scope that contain it.
     *
     * @param name the name of the variable; may not be null
     * @return the variable value; may be null if the variable is not currently set
     */
    public String getVariable(String name) {
        List<ConcurrentMap<String, String>> orderedSystemVariablesByPriority = getOrderedSystemVariablesByScopePriority();

        name = variableName(name);

        for (ConcurrentMap<String, String> variablesByScope : orderedSystemVariablesByPriority) {
            String variableName = variablesByScope.get(name);
            if (variableName != null) {
                return variableName;
            }
        }
        return null;
    }

    private List<ConcurrentMap<String, String>> getOrderedSystemVariablesByScopePriority() {
        return systemVariables.entrySet().stream()
                .sorted(Comparator.comparingInt(entry -> entry.getKey().priority()))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }

    private String variableName(String name) {
        return name.toLowerCase();
    }

    protected ConcurrentMap<String, String> forScope(Scope scope) {
        if (scope != null) {
            return systemVariables.computeIfAbsent(scope, entities -> new ConcurrentHashMap<>());
        }
        // return most prior scope variables if scope is not defined
        List<ConcurrentMap<String, String>> orderedSystemVariablesByScopePriority = getOrderedSystemVariablesByScopePriority();
        return orderedSystemVariablesByScopePriority.isEmpty() ? null : orderedSystemVariablesByScopePriority.get(0);
    }

}
