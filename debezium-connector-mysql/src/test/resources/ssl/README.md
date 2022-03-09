This directory contains the truststore (used for validating DB server certificate) and keystore (contains the client
certificate) for running the test suite with SSL enabled and two-way authentication.

The files are generated based on the certificates in src/test/resources/ssl-certs, which in turn were taken from the
MySQL container image (which generates them by default with a validity of 10 years, see /var/lib/mysql; the currently
used certificates were created on March 8 2022, i.e. expect SSL-enabled tests to fail after March 8 2032 due to the
expired certificates). The server used for SSL authentication testing uses those pre-generated certificates (see configuration in
src/test/docker/server-ssl/my.cnf) instead of generating new ones.

To regenerate the truststore/keystore files, run the following commands:

```
keytool -importcert -alias MySQLCACert -file debezium-connector-mysql/src/test/resources/ssl-certs/ca.pem -keystore debezium-connector-mysql/src/test/resources/ssl/truststore -storepass debezium -noprompt
```

```
openssl pkcs12 -export -in debezium-connector-mysql/src/test/resources/ssl-certs/client-cert.pem -inkey debezium-connector-mysql/src/test/resources/ssl-certs/client-key.pem -name "mysqlclient" -passout pass:debezium -out client-keystore.p12
keytool -importkeystore -srckeystore client-keystore.p12 -srcstoretype pkcs12 -srcstorepass debezium -destkeystore debezium-connector-mysql/src/test/resources/ssl/keystore -deststoretype pkcs12 -deststorepass debezium
```
