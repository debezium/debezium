/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.certificateutil;

import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;

/**
 * Simple abstraction for certificate extension for use in certificate builders
 */
public class CertificateExtensionWrapper {
    private final ASN1ObjectIdentifier identifier;
    private final boolean isCritical;
    private final ASN1Encodable value;

    public CertificateExtensionWrapper(ASN1ObjectIdentifier identifier, boolean isCritical, ASN1Encodable value) {
        this.identifier = identifier;
        this.isCritical = isCritical;
        this.value = value;
    }

    public ASN1ObjectIdentifier getIdentifier() {
        return identifier;
    }

    public ASN1Encodable getValue() {
        return value;
    }

    public boolean isCritical() {
        return isCritical;
    }
}
