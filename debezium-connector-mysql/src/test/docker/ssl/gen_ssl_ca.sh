#!/bin/bash
rm -rf /ssl/truststore
keytool -importcert -alias MySQLCACert -file /var/lib/mysql/ca.pem -keystore /ssl/truststore -storepass debezium -noprompt
