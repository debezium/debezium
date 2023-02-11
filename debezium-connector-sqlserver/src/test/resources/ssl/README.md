This directory contains the truststore (used for validating DB server certificate).

The files are generated based on the certificates in src/test/docker, which are generated following the
guidelines outlined in https://docs.microsoft.com/en-us/sql/linux/sql-server-linux-docker-container-security?view=sql-server-ver16.

To generate the server certificate and key:

```
openssl req -x509 -nodes -newkey rsa:2048 -subj '/CN=localhost' -keyout mssql.key -out mssql.pem -days 3650
```

This will output two files, `mssql.key` and `mssql.pem`.

These files are mounted to the SQL Server docker container and therefore the correct permissions must be set
on these files so that they can be read properly by SQL Server's database process. To set the right permissions,
you need to run the following:

```
chmod 444 mssql.key
chmod 444 mssql.pem
```

Note, we explicitly use `444` rather than `440` since the volume is mounted as root but the SQL Server database
is started by the `mssql` user, this allows the key and certificate to be read.

In addition, a special `mssql.conf` file will also be mounted in order for Microsoft SQL Server to know where
to read the database certificates from.  This configuration file will be mounted automatically to
`/var/opt/mssql/mssql.conf`.  The contents of this file is as follows:

```
[network]
tlscert = /etc/ssl/debezium/certs/mssql.pem
tlskey = /etc/ssl/debezium/private/mssql.key
tlsprotocols = 1.2
forceencryption = 1
```

This configuration enforces TLS 1.2 and encryption in order to operate with the SQL Server instance.
If the client does not support encryption or cannot negotiate TLS 1.2, the client connection will be rejected.

Finally, the truststore is generated as follows:

```
keytool -import -v -trustcacerts -alias localhost -file ../../docker/mssql.pem -keystore truststore.ks -storepass debezium -noprompt
```

This imports the `mssql.pem` public certificate into the truststore that we mount into the container and
this truststore will be configured as part of the SQL Server connector's configuration using:

```
database.ssl.truststore=${project.basedir}/src/test/resources/ssl
database.ssl.truststore.password=debezium
database.trustServerCertificate=true
```

We specifically set `trustServerCertificate` because this certificate is self-signed.
This certificate is also valid for 10 years from August 9th, 2022, so expires August 9th, 2032.
