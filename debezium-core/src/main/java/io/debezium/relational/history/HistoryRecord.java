/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import java.util.Map;

import io.debezium.document.Document;

public class HistoryRecord {
    private final Document doc;
    
    public HistoryRecord(Document document) {
        this.doc = document;
    }

    public HistoryRecord(Map<String, ?> source, Map<String, ?> position, String databaseName, String ddl) {
        this.doc = Document.create();
        Document src = doc.setDocument("source");
        if (source != null) source.forEach(src::set);
        Document pos = doc.setDocument("position");
        if (position != null) position.forEach(pos::set);
        if (databaseName != null) doc.setString("databaseName", databaseName);
        if (ddl != null) doc.setString("ddl", ddl);
    }

    public Document document() {
        return this.doc;
    }

    public boolean isAtOrBefore(HistoryRecord other) {
        if (other == this) return true;
        return this.position().compareToWithoutFieldOrder(other.position()) <= 0
                && source().equals(other.source());
    }

    protected Document source() {
        return doc.getDocument("source");
    }

    protected Document position() {
        return doc.getDocument("position");
    }

    protected String databaseName() {
        return doc.getString("databaseName");
    }

    protected String ddl() {
        return doc.getString("ddl");
    }

    protected boolean hasSameSource(HistoryRecord other) {
        if (this == other) return true;
        return other != null && source().equals(other.source());
    }

    protected boolean hasSameDatabase(HistoryRecord other) {
        if (this == other) return true;
        return other != null && databaseName().equals(other.databaseName());
    }
}