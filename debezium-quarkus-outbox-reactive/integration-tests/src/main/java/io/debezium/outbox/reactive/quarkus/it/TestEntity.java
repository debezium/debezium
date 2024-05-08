/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.reactive.quarkus.it;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;

/**
 * This entity class is merely a placeholder to address two concerns:
 * <ul>
 *     <li>If no target/classes directory is created, the quarkus maven plugin fails.</li>
 *     <li>If no annotated entity mapping is found, the EntityManager dependency is excluded leading to an unsatisfied dependency exception.</li>
 * </ul>
 *
 * @author Chris Cranford
 */
@Entity
public class TestEntity {
    @Id
    @GeneratedValue
    private Integer id;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }
}
