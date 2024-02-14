/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.certificateutil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Map;

import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.openshift.client.OpenShiftClient;

public class CertUtil {
    public static final String KEYSTORE_PASSWORD = "password";

    /**
     * Uploads a string data as configMap to ocp cluster
     * @param project namespace, where to create the configmap
     * @param data content of a file in configMap
     * @param configMapName config map name
     * @param fileNameInConfigMap filename in configmap
     * @param ocp ocp client
     */
    public static void stringToConfigMap(String project, String data, String configMapName, String fileNameInConfigMap, OpenShiftClient ocp) {
        var configMap = new ConfigMapBuilder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(configMapName)
                        .build())
                .withData(Map.of(fileNameInConfigMap, data))
                .build();
        ocp.configMaps().inNamespace(project).createOrReplace(configMap);
    }

    /**
     * Converts keystore to base64 string and uploads as configMap to ocp cluster
     * @param project namespace, where to create the configmap
     * @param keyStore keystore object to be saved in configmap
     * @param configMapName config map name
     * @param fileNameInConfigMap filename in configmap
     * @param ocp ocp client
     */
    public static void keystoreToConfigMap(String project, KeyStore keyStore, String configMapName, String fileNameInConfigMap, OpenShiftClient ocp)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        char[] pwdArray = KEYSTORE_PASSWORD.toCharArray();
        try (ByteArrayOutputStream fos = new ByteArrayOutputStream()) {
            keyStore.store(fos, pwdArray);
            var configMap = new ConfigMapBuilder()
                    .withMetadata(new ObjectMetaBuilder()
                            .withName(configMapName)
                            .build())
                    .withBinaryData(Map.of(fileNameInConfigMap, Base64.getEncoder().encodeToString(fos.toByteArray())))
                    .build();
            ocp.configMaps().inNamespace(project).createOrReplace(configMap);

        }
    }

    /**
     * exports PK to base64 string
     * @param privateKey
     * @return
     * @throws IOException
     */
    public static String exportToBase64PEMString(PrivateKey privateKey) throws IOException {
        StringWriter sw = new StringWriter();
        try (JcaPEMWriter pw = new JcaPEMWriter(sw)) {
            pw.writeObject(privateKey);
        }
        return sw.toString();
    }

    public static String exportToBase64PEMString(X509CertificateHolder holder) throws CertificateException, IOException {
        return exportToBase64PEMString(convertHolderToCert(holder));
    }

    private static X509Certificate convertHolderToCert(X509CertificateHolder holder) throws CertificateException {
        JcaX509CertificateConverter converter = new JcaX509CertificateConverter();
        converter.setProvider(new BouncyCastleProvider());
        return converter.getCertificate(holder);
    }

    private static String exportToBase64PEMString(X509Certificate x509Cert) throws IOException {
        StringWriter sw = new StringWriter();
        try (JcaPEMWriter pw = new JcaPEMWriter(sw)) {
            pw.writeObject(x509Cert);
        }
        return sw.toString();
    }

    /**
     * Creates a String to be passed to mongodb as server certificateKeyFile
     * @param cert
     * @param ca
     * @return
     * @throws IOException
     * @throws CertificateException
     */
    public static String exportCertificateToMongoCompatiblePem(CertificateWrapper cert, CertificateWrapper ca) throws IOException, CertificateException {
        return exportToBase64PEMString(cert.getKeyPair().getPrivate()) +
                exportToBase64PEMString(cert.getHolder()) +
                exportToBase64PEMString(ca.getHolder());
    }
}
