/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb.sharded;

import java.util.List;

import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.DERSequence;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.certificateutil.CertUtil;
import io.debezium.testing.system.tools.certificateutil.CertificateExtensionWrapper;
import io.debezium.testing.system.tools.certificateutil.CertificateGenerator;
import io.debezium.testing.system.tools.certificateutil.CertificateWrapper;
import io.debezium.testing.system.tools.certificateutil.CertificateWrapperBuilder;
import io.fabric8.openshift.client.OpenShiftClient;

public class OcpMongoCertGenerator {
    private static final Logger LOGGER = LoggerFactory.getLogger(OcpMongoCertGenerator.class);

    public static final String KEYSTORE_CONFIGMAP = "keystore";
    public static final String KEYSTORE_SUBPATH = "keystore.jks";
    public static final String TRUSTSTORE_CONFIGMAP = "truststore";
    public static final String TRUSTSTORE_SUBPATH = "truststore.jks";

    public static final String SERVER_CERT_CONFIGMAP = "server-cert";
    public static final String SERVER_CERT_SUBPATH = "server-combined.pem";
    public static final String CA_CERT_CONFIGMAP = "ca-cert";
    public static final String CA_CERT_SUBPATH = "ca-cert.pem";

    public static final String CLIENT_CERT_SUBJECT = "CN=client";
    private static final String SERVER_CERT_SUBJECT = "O=Debezium, CN=mongo-server";
    private static final String CLIENT_CERT_NAME = "client";
    private static final String SERVER_CERT_NAME = "server";

    public static void generateMongoTestCerts(OpenShiftClient ocp) throws Exception {
        List<CertificateWrapperBuilder> specs = getLeafCertSpecs();
        var certificateCreator = new CertificateGenerator(specs);
        certificateCreator.generate();

        LOGGER.info("Creating truststore/keystore configmaps for mongo connector");
        CertUtil.keystoreToConfigMap(ConfigProperties.OCP_PROJECT_DBZ, certificateCreator.generateKeyStore(CLIENT_CERT_NAME), KEYSTORE_CONFIGMAP, KEYSTORE_SUBPATH, ocp);
        CertUtil.keystoreToConfigMap(ConfigProperties.OCP_PROJECT_DBZ, certificateCreator.generateKeyStore(SERVER_CERT_NAME), TRUSTSTORE_CONFIGMAP, TRUSTSTORE_SUBPATH,
                ocp);

        LOGGER.info("Creating certificate configmaps for mongo database");
        CertUtil.stringToConfigMap(ConfigProperties.OCP_PROJECT_MONGO,
                CertUtil.exportCertificateToMongoCompatiblePem(certificateCreator.getLeafCertificateWrapper(SERVER_CERT_NAME), certificateCreator.getCa()),
                SERVER_CERT_CONFIGMAP, SERVER_CERT_SUBPATH, ocp);
        CertUtil.stringToConfigMap(ConfigProperties.OCP_PROJECT_MONGO, CertUtil.exportToBase64PEMString(certificateCreator.getCa().getHolder()), CA_CERT_CONFIGMAP,
                CA_CERT_SUBPATH, ocp);
    }

    private static List<CertificateWrapperBuilder> getLeafCertSpecs() {
        ASN1Encodable[] subjectAltNames = new ASN1Encodable[]{
                new GeneralName(GeneralName.dNSName, "*." + ConfigProperties.OCP_PROJECT_MONGO + ".svc.cluster.local"),
                new GeneralName(GeneralName.dNSName, "localhost"),
                new GeneralName(GeneralName.iPAddress, "127.0.0.1")
        };
        return List.of(
                CertificateWrapper.builder()
                        .withName(CLIENT_CERT_NAME)
                        .withSubject(CLIENT_CERT_SUBJECT)
                        .withExtensions(List.of(
                                new CertificateExtensionWrapper(Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature)),
                                new CertificateExtensionWrapper(Extension.extendedKeyUsage, true,
                                        new ExtendedKeyUsage(new KeyPurposeId[]{ KeyPurposeId.id_kp_clientAuth })),
                                new CertificateExtensionWrapper(Extension.subjectAlternativeName, true, new DERSequence(subjectAltNames)))),
                CertificateWrapper.builder()
                        .withName(SERVER_CERT_NAME)
                        .withSubject(SERVER_CERT_SUBJECT)
                        .withExtensions(List.of(
                                new CertificateExtensionWrapper(Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature)),
                                new CertificateExtensionWrapper(Extension.extendedKeyUsage, true,
                                        new ExtendedKeyUsage(new KeyPurposeId[]{ KeyPurposeId.id_kp_clientAuth, KeyPurposeId.id_kp_serverAuth })),
                                new CertificateExtensionWrapper(Extension.subjectAlternativeName, true, new DERSequence(subjectAltNames)))));
    }

}
