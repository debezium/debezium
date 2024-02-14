/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.certificateutil;

import java.security.KeyPair;
import java.util.List;

import org.bouncycastle.cert.X509CertificateHolder;

public class CertificateWrapper {
    private final String name;
    private final KeyPair keyPair;
    private final String subject;
    private final List<CertificateExtensionWrapper> extensions;
    private final X509CertificateHolder holder;

    public static CertificateWrapperBuilder builder() {
        return new CertificateWrapperBuilder();
    }

    public CertificateWrapper(String name, KeyPair keyPair, String subject, List<CertificateExtensionWrapper> extensions, X509CertificateHolder holder) {
        this.name = name;
        this.keyPair = keyPair;
        this.subject = subject;
        this.extensions = extensions;
        this.holder = holder;
    }

    public String getName() {
        return name;
    }

    public KeyPair getKeyPair() {
        return keyPair;
    }

    public String getSubject() {
        return subject;
    }

    public List<CertificateExtensionWrapper> getExtensions() {
        return extensions;
    }

    public X509CertificateHolder getHolder() {
        return holder;
    }

}
