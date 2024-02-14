/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.certificateutil;

import java.io.IOException;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for generating certs. Can generate a CA cert and list of leaf certificates signed by this CA (no intermediates)
 */
public class CertificateGenerator {
    private static final Logger LOGGER = LoggerFactory.getLogger(CertificateGenerator.class);

    private static final String SIGNATURE_ALGORITHM = "SHA384WITHRSA";
    private final X500Name caSubject = new X500Name("cn=RootCA");
    private final List<CertificateWrapperBuilder> certSpecs;
    private CertificateWrapper ca;
    private final List<CertificateWrapper> generatedCerts = new LinkedList<>();
    private final int PRIVATE_KEY_SIZE = 3072;
    private final String PRIVATE_KEY_ALGORITHM = "RSA";

    public CertificateGenerator(List<CertificateWrapperBuilder> leafSpec) {
        this.certSpecs = leafSpec;
    }

    /**
     * generate CA certificate and all leaf certificates specified in certSpecs attribute
     * @throws Exception
     */
    public void generate() throws Exception {
        // generate keys and certificates
        ca = generateCa();

        certSpecs.forEach(l -> {
            try {
                var cert = genLeafCert(l);
                generatedCerts.add(cert);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Generates keystore containing a leaf private key and its certificate chain. Usable for keystore/truststore generation
     * keystore password set by constant in CertUtil class
     * @param leafName
     * @return
     * @throws Exception
     */
    public KeyStore generateKeyStore(String leafName) throws Exception {
        var certWrapper = getLeafCertificateWrapper(leafName);
        var keystore = KeyStore.getInstance("JKS");
        keystore.load(null);
        // Import private key
        keystore.setKeyEntry(leafName,
                certWrapper.getKeyPair().getPrivate(),
                CertUtil.KEYSTORE_PASSWORD.toCharArray(),
                new X509Certificate[]{ holderToCert(certWrapper.getHolder()), holderToCert(ca.getHolder()) });
        return keystore;
    }

    public CertificateWrapper getLeafCertificateWrapper(String name) {
        var spec = generatedCerts.stream().filter(l -> l.getName().equals(name)).collect(Collectors.toList());
        if (spec.size() != 1) {
            throw new IllegalArgumentException("Certificate not found in generated certs list");
        }
        return spec.get(0);
    }

    public CertificateWrapper getCa() {
        return ca;
    }

    private CertificateWrapper generateCa() throws IOException {
        Security.addProvider(new BouncyCastleProvider());
        KeyPair keyPair = generateKeyPair();

        long notBefore = System.currentTimeMillis();
        long notAfter = notBefore + (1000L * 3600L * 24 * 365); // one year from now
        X509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(
                caSubject,
                BigInteger.ONE,
                new Date(notBefore),
                new Date(notAfter),
                caSubject,
                keyPair.getPublic());

        X509CertificateHolder certHolder;
        List<CertificateExtensionWrapper> extensions = List.of(
                new CertificateExtensionWrapper(Extension.basicConstraints, true, new BasicConstraints(true)),
                new CertificateExtensionWrapper(Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign))// ,
        );
        try {
            extensions.forEach(e -> {
                try {
                    certBuilder.addExtension(e.getIdentifier(), e.isCritical(), e.getValue());
                }
                catch (CertIOException ex) {
                    throw new RuntimeException(ex);
                }
            });

            final ContentSigner signer = new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).build(keyPair.getPrivate());
            certHolder = certBuilder.build(signer);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        return CertificateWrapper.builder()
                .withKeyPair(keyPair)
                .withExtensions(extensions)
                .withSubject(new String(caSubject.getEncoded()))
                .withHolder(certHolder)
                .build();
    }

    private CertificateWrapper genLeafCert(CertificateWrapperBuilder builder) throws OperatorCreationException {
        KeyPair keyPair = generateKeyPair();

        long notBefore = System.currentTimeMillis();
        long notAfter = notBefore + (1000L * 3600L * 24 * 365);

        X509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(
                caSubject,
                new BigInteger(String.valueOf(System.currentTimeMillis())),
                new Date(notBefore),
                new Date(notAfter),
                new X500Name(builder.getSubject()),
                keyPair.getPublic());

        builder.getExtensions().forEach(e -> {
            try {
                certBuilder.addExtension(e.getIdentifier(), e.isCritical(), e.getValue());
            }
            catch (CertIOException ex) {
                throw new RuntimeException(ex);
            }
        });

        ContentSigner signer = new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).build(ca.getKeyPair().getPrivate());
        var holder = certBuilder.build(signer);

        return builder
                .withKeyPair(keyPair)
                .withHolder(holder)
                .build();
    }

    private X509Certificate holderToCert(X509CertificateHolder holder) throws CertificateException {
        JcaX509CertificateConverter converter = new JcaX509CertificateConverter();
        converter.setProvider(new BouncyCastleProvider());
        return converter.getCertificate(holder);
    }

    private KeyPair generateKeyPair() {
        try {
            KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(PRIVATE_KEY_ALGORITHM);
            keyPairGenerator.initialize(PRIVATE_KEY_SIZE, new SecureRandom());
            return keyPairGenerator.generateKeyPair();
        }
        catch (GeneralSecurityException var2) {
            throw new AssertionError(var2);
        }
    }

}
