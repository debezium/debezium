/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.certificateutil;

import java.security.KeyPair;
import java.util.List;

import org.bouncycastle.cert.X509CertificateHolder;

public final class CertificateWrapperBuilder {
    private KeyPair keyPair;
    private String subject;
    private List<CertificateExtensionWrapper> extensions;
    private X509CertificateHolder holder;
    private String name;

    CertificateWrapperBuilder() {
    }

    public CertificateWrapperBuilder withKeyPair(KeyPair keyPair) {
        this.keyPair = keyPair;
        return this;
    }

    public CertificateWrapperBuilder withSubject(String subject) {
        this.subject = subject;
        return this;
    }

    public CertificateWrapperBuilder withName(String name) {
        this.name = name;
        return this;
    }

    public CertificateWrapperBuilder withExtensions(List<CertificateExtensionWrapper> extensions) {
        this.extensions = extensions;
        return this;
    }

    public CertificateWrapperBuilder withHolder(X509CertificateHolder holder) {
        this.holder = holder;
        return this;
    }

    public String getSubject() {
        return subject;
    }

    public List<CertificateExtensionWrapper> getExtensions() {
        return extensions;
    }

    public CertificateWrapper build() {
        return new CertificateWrapper(name, keyPair, subject, extensions, holder);
    }
}
