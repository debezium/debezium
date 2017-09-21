# Change log

All notable changes are documented in this file. Release numbers follow [Semantic Versioning](http://semver.org)

## 0.6.0
September 21st, 2017 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?version=12331386&projectId=12317320)

### New features since 0.5.2

* Use new Kafka 0.10 properties for listeners and advertised listeners [DBZ-39](https://issues.jboss.org/projects/DBZ/issues/DBZ-39)
* Add docker-compose handling for Debezium tutorial [DBZ-127](https://issues.jboss.org/projects/DBZ/issues/DBZ-127)
* Topic configuration requirements are not clearly documented [DBZ-241](https://issues.jboss.org/projects/DBZ/issues/DBZ-241)
* Upgrade Docker images to Kafka 0.11.0.0 [DBZ-305](https://issues.jboss.org/projects/DBZ/issues/DBZ-305)
* Add SMT implementation to convert CDC event structure to more traditional row state structure [DBZ-226](https://issues.jboss.org/projects/DBZ/issues/DBZ-226)
* Support SSL connection to Mongodb [DBZ-343](https://issues.jboss.org/projects/DBZ/issues/DBZ-343)
* Support DEC and FIXED type for mysql ddl parser [DBZ-359](https://issues.jboss.org/projects/DBZ/issues/DBZ-359)


### Breaking changes since 0.5.2

This release includes the following change that affects existing installations that captures MongoDB

* Add support for different mongodb _id types in key struct [DBZ-306](https://issues.jboss.org/projects/DBZ/issues/DBZ-306)


### Fixes and changes since 0.5.2

* MySQL snapshotter is not guaranteed to give a consistent snapshot [DBZ-210](https://issues.jboss.org/projects/DBZ/issues/DBZ-210)
* MySQL connector stops consuming data from binlog after server restart [DBZ-219](https://issues.jboss.org/projects/DBZ/issues/DBZ-219)
* Warnings and notifications from PostgreSQL are ignored by the connector [DBZ-279](https://issues.jboss.org/projects/DBZ/issues/DBZ-279)
* BigDecimal has mismatching scale value for given Decimal schema error. [DBZ-318](https://issues.jboss.org/projects/DBZ/issues/DBZ-318)
* Views in database stop PostgreSQL connector [DBZ-319](https://issues.jboss.org/projects/DBZ/issues/DBZ-319)
* Don't pass database history properties to the JDBC connection [DBZ-333](https://issues.jboss.org/projects/DBZ/issues/DBZ-333)
* Sanitize readings from database history topic [DBZ-341](https://issues.jboss.org/projects/DBZ/issues/DBZ-341)
* Support UNION for ALTER TABLE [DBZ-346](https://issues.jboss.org/projects/DBZ/issues/DBZ-346)
* Debezium fails to start when schema history topic contains unparseable SQL [DBZ-347](https://issues.jboss.org/projects/DBZ/issues/DBZ-347)
* JDBC Connection is not closed after schema refresh [DBZ-356](https://issues.jboss.org/projects/DBZ/issues/DBZ-356)
* MySQL integration tests should have completely isolated environment and configuration/setup files [DBZ-304](https://issues.jboss.org/projects/DBZ/issues/DBZ-304)

## 0.5.2

August 17, 2017 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?version=12334601&projectId=12317320)

### New features since 0.5.1

* Mongo Connector: Add "database.whitelist" and "database.blacklist" configuration options [DBZ-302](https://issues.jboss.org/projects/DBZ/issues/DBZ-302)
* Provide a Dockerfile to build images from latest released snapshot [DBZ-320](https://issues.jboss.org/projects/DBZ/issues/DBZ-320)
* Support decimal handling mode for Postgres [DBZ-337](https://issues.jboss.org/projects/DBZ/issues/DBZ-337)
* Enable and show usage of Avro converters [DBZ-271](https://issues.jboss.org/projects/DBZ/issues/DBZ-271)
* Keep TCP connection alive for Postgres [DBZ-286](https://issues.jboss.org/projects/DBZ/issues/DBZ-286)
* Support "PAGE_CHECKSUM=1" option for MySQL tables [DBZ-324](https://issues.jboss.org/projects/DBZ/issues/DBZ-324)

### Breaking changes since 0.5.1

There should be no breaking changes in this relese.

### Fixes and changes since 0.5.1

* Images cannot run on OpenShift online [DBZ-267](https://issues.jboss.org/projects/DBZ/issues/DBZ-267)
* NPE when processing null value in POINT column [DBZ-284](https://issues.jboss.org/projects/DBZ/issues/DBZ-284)
* Postgres Connector: error of mismatching scale value for Decimal and Numeric data types [DBZ-287](https://issues.jboss.org/projects/DBZ/issues/DBZ-287)
* Postgres connector fails with array columns [DBZ-297](https://issues.jboss.org/projects/DBZ/issues/DBZ-297)
* Postgres connector fails with quoted type names [DBZ-298](https://issues.jboss.org/projects/DBZ/issues/DBZ-298)
* LogicalTableRouter SMT uses wrong comparison for validation [DBZ-326](https://issues.jboss.org/projects/DBZ/issues/DBZ-326)
* LogicalTableRouter SMT has a broken key replacement validation [DBZ-327](https://issues.jboss.org/projects/DBZ/issues/DBZ-327)
* Pre-compile and simplify some regular expressions [DBZ-311](https://issues.jboss.org/projects/DBZ/issues/DBZ-311)
* JMX metrics for MySQL connector should be documented [DBZ-293](https://issues.jboss.org/projects/DBZ/issues/DBZ-293)
* PostgreSQL integration tests should have completely isolated environment and configuration/setup files [DBZ-301](https://issues.jboss.org/projects/DBZ/issues/DBZ-301)
* Move snapshot Dockerfile into separated directory [DBZ-321](https://issues.jboss.org/projects/DBZ/issues/DBZ-321)
* Cover ByLogicalTableRouter SMT in reference documentation [DBZ-325](https://issues.jboss.org/projects/DBZ/issues/DBZ-325)
* Add documentation for JDBC url pass-through properties [DBZ-330](https://issues.jboss.org/projects/DBZ/issues/DBZ-330)

## 0.5.1

June 9, 2017 - [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12333615)

### New features since 0.5.0

* MySQL Connector should support 'Point' data type [DBZ-222](https://issues.jboss.org/projects/DBZ/issues/DBZ-222)
* Support tstzrange column type on Postgres [DBZ-280](https://issues.jboss.org/projects/DBZ/issues/DBZ-280)

### Breaking changes since 0.5.0

This release includes the following change that affect existing installations which capture system tables:

* MySQL connector should apply database and table filters to system dbs/tables [DBZ-242](https://issues.jboss.org/projects/DBZ/issues/DBZ-242)

### Fixes and changes since 0.5.0

* Control how Debezium connectors maps tables to topics for sharding and other use cases [DBZ-121](https://issues.jboss.org/projects/DBZ/issues/DBZ-121)
* MySqlConnector Table and Database recommenders cause timeouts on large instances [DBZ-232](https://issues.jboss.org/projects/DBZ/issues/DBZ-232)
* Option to disable SSL certificate validation for PostgreSQL [DBZ-244](https://issues.jboss.org/projects/DBZ/issues/DBZ-244)
* Let enum types implement EnumeratedValue [DBZ-262](https://issues.jboss.org/projects/DBZ/issues/DBZ-262)
* The  MySQL connector is failing with the DDL statements. [DBZ-198](https://issues.jboss.org/projects/DBZ/issues/DBZ-198)
* Correct MongoDB build [DBZ-213](https://issues.jboss.org/projects/DBZ/issues/DBZ-213)
* MongoDB connector should handle new primary better [DBZ-214](https://issues.jboss.org/projects/DBZ/issues/DBZ-214)
* Validate that database.server.name and database.history.kafka.topic have different values [DBZ-215](https://issues.jboss.org/projects/DBZ/issues/DBZ-215)
* When restarting Kafka Connect, we get io.debezium.text.ParsingException [DBZ-216](https://issues.jboss.org/projects/DBZ/issues/DBZ-216)
* Postgres connector crash on a database managed by Django [DBZ-223](https://issues.jboss.org/projects/DBZ/issues/DBZ-223)
* MySQL Connector doesn't handle any value above '2147483647' for 'INT UNSIGNED' types [DBZ-228](https://issues.jboss.org/projects/DBZ/issues/DBZ-228)
* MySqlJdbcContext#userHasPrivileges() is broken for multiple privileges [DBZ-229](https://issues.jboss.org/projects/DBZ/issues/DBZ-229)
* Postgres Connector does not work when "sslmode" is "require" [DBZ-238](https://issues.jboss.org/projects/DBZ/issues/DBZ-238)
* Test PostgresConnectorIT.shouldSupportSSLParameters is incorrect [DBZ-245](https://issues.jboss.org/projects/DBZ/issues/DBZ-245)
* Recommender and default value broken for EnumeratedValue type [DBZ-246](https://issues.jboss.org/projects/DBZ/issues/DBZ-246)
* PG connector is CPU consuming  [DBZ-250](https://issues.jboss.org/projects/DBZ/issues/DBZ-250)
* MySQL tests are interdependent [DBZ-251](https://issues.jboss.org/projects/DBZ/issues/DBZ-251)
* MySQL DDL parser fails on "ANALYZE TABLE" statement  [DBZ-253](https://issues.jboss.org/projects/DBZ/issues/DBZ-253)
* Binary fields with trailing "00" are truncated [DBZ-254](https://issues.jboss.org/projects/DBZ/issues/DBZ-254)
* Enable Maven repository caching on Travis [DBZ-274](https://issues.jboss.org/projects/DBZ/issues/DBZ-274)
* Memory leak and excessive CPU usage when using materialized views [DBZ-277](https://issues.jboss.org/projects/DBZ/issues/DBZ-277)
* Postgres task should fail when connection to server is lost [DBZ-281](https://issues.jboss.org/projects/DBZ/issues/DBZ-281)
* Fix some wrong textual descriptions of default values [DBZ-282](https://issues.jboss.org/projects/DBZ/issues/DBZ-282)
* Apply consistent default value for Postgres port [DBZ-237](https://issues.jboss.org/projects/DBZ/issues/DBZ-237)
* Make Docker images run on OpenShift [DBZ-240](https://issues.jboss.org/projects/DBZ/issues/DBZ-240)
* Don't mention default value for "database.server.name" [DBZ-243](https://issues.jboss.org/projects/DBZ/issues/DBZ-243)

## 0.5.0

March 27, 2017 - [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12334135)

### New features since 0.4.1

None

### Breaking changes since 0.4.1

This release includes the following changes that are likely to affect existing installations:

* Upgraded from Kafka 0.10.1.1 to 0.10.2.0. [DBZ-203](https://issues.jboss.org/projects/DBZ/issues/DBZ-203)

### Fixes and changes since 0.4.1

This release includes the following fixes, changes, or improvements since the [0.4.1](#041) release:

* MySQL connector now better handles DDL statements with `BEGIN...END` blocks, especially those that use `IF()` functions as well as `CASE...WHEN` statements. [DBZ-198](https://issues.jboss.org/projects/DBZ/issues/DBZ-198)
* MySQL connector handles 2-digit years in `DATETIME`, `DATE`, `TIMESTAMP`, and `YEAR` columns in the [same way as MySQL](https://dev.mysql.com/doc/refman/5.7/en/two-digit-years.html). [DBZ-205](https://issues.jboss.org/projects/DBZ/issues/DBZ-205)


## 0.4.1

March 17, 2017 - [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12333486)

### New features since 0.4.0

* Improved support for [Amazon RDS](https://aws.amazon.com/rds/mysql/) and [Amazon Aurora (MySQL compatibility)](https://aws.amazon.com/rds/aurora/). [DBZ-140](https://issues.jboss.org/projects/DBZ/issues/DBZ-140)

### Breaking changes since 0.4.0

None

### Fixes and changes since 0.4.0

This release includes the following fixes, changes, or improvements since the [0.4.0](#040) release:

* MySQL connector now allows filtering production of DML events by GTIDs. [DBZ-188](https://issues.jboss.org/projects/DBZ/issues/DBZ-188)
* Support InnoDB savepoints. [DBZ-196](https://issues.jboss.org/projects/DBZ/issues/DBZ-196)
* Corrected MySQL DDL parser. [DBZ-193](https://issues.jboss.org/projects/DBZ/issues/DBZ-193) [DBZ-198](https://issues.jboss.org/projects/DBZ/issues/DBZ-198)
* Improved handling of MySQL connector's built-in tables. [DBZ-194](https://issues.jboss.org/projects/DBZ/issues/DBZ-194)
* MySQL connector properly handles invalid/blank enum literal values. [DBZ-197](https://issues.jboss.org/projects/DBZ/issues/DBZ-197)
* MySQL connector properly handles reserved names as column names. [DBZ-200](https://issues.jboss.org/projects/DBZ/issues/DBZ-200)
* MongoDB connector properly generates event keys based upon ObjectID for updates. [DBZ-201](https://issues.jboss.org/projects/DBZ/issues/DBZ-201)


## 0.4.0

February 7, 2017 - [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12330743)

### New features since 0.3.6

* New PostgreSQL connector. [DBZ-3](https://issues.jboss.org/projects/DBZ/issues/DBZ-3)
* Preliminary support for [Amazon RDS](https://aws.amazon.com/rds/mysql/) and [Amazon Aurora (MySQL compatibility)](https://aws.amazon.com/rds/aurora/). [DBZ-140](https://issues.jboss.org/projects/DBZ/issues/DBZ-140)

### Breaking changes since 0.3.6

None

### Fixes and changes since 0.3.6

This release includes the following fixes, changes, or improvements since the [0.3.6](#036) release:

* Update Kafka dependencies to 0.10.1.1. [DBZ-173](https://issues.jboss.org/projects/DBZ/issues/DBZ-173)
* Update MySQL binary log client library to 0.9.0. [DBZ-186](https://issues.jboss.org/projects/DBZ/issues/DBZ-186)
* MySQL should apply GTID filters to database history. [DBZ-185](https://issues.jboss.org/projects/DBZ/issues/DBZ-185)
* Add names of database and table to the MySQL event metadata. [DBZ-184](https://issues.jboss.org/projects/DBZ/issues/DBZ-184)
* Add the MySQL thread ID to the MySQL event metadata. [DBZ-113](https://issues.jboss.org/projects/DBZ/issues/DBZ-113)
* Corrects MySQL connector to properly handle timezone information for `TIMESTAMP`. [DBZ-183](https://issues.jboss.org/projects/DBZ/issues/DBZ-183)
* Correct MySQL DDL parser to handle `CREATE TRIGGER` command with `DEFINER` clauses. [DBZ-176](https://issues.jboss.org/projects/DBZ/issues/DBZ-176)
* Update MongoDB Java driver and MongoDB server versions. [DBZ-187](https://issues.jboss.org/projects/DBZ/issues/DBZ-187)
* MongoDB connector should restart incomplete initial sync. [DBZ-182](https://issues.jboss.org/projects/DBZ/issues/DBZ-182)
* MySQL and PostgreSQL connectors should load JDBC driver independently of DriverManager. [DBZ-177](https://issues.jboss.org/projects/DBZ/issues/DBZ-177)
* Upgrade MySQL binlog client library to support new binlog events added with MySQL 5.7. [DBZ-174](https://issues.jboss.org/projects/DBZ/issues/DBZ-174)
* EmbeddedEngine should log all errors. [DBZ-178](https://issues.jboss.org/projects/DBZ/issues/DBZ-178)
* PostgreSQL containers' generated Protobuf source moved to separate directory. [DBZ-179](https://issues.jboss.org/projects/DBZ/issues/DBZ-179)


## 0.3.6

December 21, 2016 - [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12332775)

### New features since 0.3.5

None

### Breaking changes since 0.3.5

None

### Fixes since 0.3.5

This release includes the following fixes since the [0.3.5](#035) release:

* Deleting a Debezium connector in Kafka Connect no longer causes NPEs. [DBZ-138](https://issues.jboss.org/projects/DBZ/issues/DBZ-138)
* MongoDB connector properly connects to a sharded cluster and the primaries for each replica set. [DBZ-170](https://issues.jboss.org/projects/DBZ/issues/DBZ-170), [DBZ-167](https://issues.jboss.org/projects/DBZ/issues/DBZ-167)
* Stopping the MySQL connector while in the middle of a snapshot now cloasses all MySQL resources. [DBZ-166](https://issues.jboss.org/projects/DBZ/issues/DBZ-166)
* MySQL connector properly parses with `ON UPDATE` timestamp values. [DBZ-169](https://issues.jboss.org/projects/DBZ/issues/DBZ-169)
* MySQL connector ignores `CREATE FUNCTION` DDL statements. [DBZ-162](https://issues.jboss.org/projects/DBZ/issues/DBZ-162)
* MySQL connector properly parses `CREATE TABLE` script with ENUM type and default value 'b'. [DBZ-160]https://issues.jboss.org/projects/DBZ/issues/DBZ-160)
* MySQL connector now properly supports `NVARCHAR` columns. [DBZ-142](https://issues.jboss.org/projects/DBZ/issues/DBZ-142)
* MySQL connector's snapshot process now uses `SHOW TABLE STATUS ...` rather than `SELECT COUNT(\*)` to obtain an estimate of the number of rows for each table, and can even forgo this step if all tables are to be streamed. [DBZ-152](https://issues.jboss.org/projects/DBZ/issues/DBZ-152)
* MySQL connector's snaphot process ignores "artificial" database names exposed by MySQL. [DBZ-164](https://issues.jboss.org/projects/DBZ/issues/DBZ-164)
* MySQL connector ignores XA statements appearing in the binlog. [DBZ-168](https://issues.jboss.org/projects/DBZ/issues/DBZ-168)
* MySQL connector no longer expects GTID set information on older MySQL versions. [DBZ-161](https://issues.jboss.org/projects/DBZ/issues/DBZ-161)
* Improved the EmbeddedEngine and fixed several issues. [DBZ-156](https://issues.jboss.org/projects/DBZ/issues/DBZ-156)



## 0.3.5

November 9, 2016 - [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12332052)

### New features since 0.3.4

This release includes the following feature:

* MySQL connector now supports failover to MySQL masters that are slaves of multiple other MySQL servers/clusters. [DBZ-143](https://issues.jboss.org/projects/DBZ/issues/DBZ-143)


### Backwards-incompatible changes since 0.3.4

None

### Fixes since 0.3.4

This release includes the following significant fix, and all users are strongly encouraged to upgrade:

* Restarting MySQL connector may lose or miss events from the previous transaction that was incompletely processed prior to the easlier shutdown. This fix corrects this potential problem and slightly alters the offsets recorded by the connector. Production connectors should be stopped carefully to ensure transactions are processed atomically, if necessary by temporarily stopping updates on the MySQL server and letting the connector complete all transactions before stopping. [DBZ-144](https://issues.jboss.org/projects/DBZ/issues/DBZ-144)

Additionally, this release includes the following fixes since the [0.3.4](#034) release:

* Shutting down MySQL connector task database and quickly terminating the Kafka Connect process may cause connector to be restarted in a strange state when Kafka Connect is restarted, but this no longer results in a null pointer exception in the Kafka database history. [DBZ-146](https://issues.jboss.org/projects/DBZ/issues/DBZ-146)
* MySQL connector now has option to treat `DECIMAL` and `NUMERIC` columns as double values rather than `java.math.BigDecimal` values that are encoded in the messages by Kafka Connect in binary form. [DBZ-147](https://issues.jboss.org/projects/DBZ/issues/DBZ-147)
* MySQL connector tests now take into account daylight savings time in the expected results. [DBZ-148](https://issues.jboss.org/projects/DBZ/issues/DBZ-148)
* MySQL connector now properly treats `BINARY` columns as binary values rather than string values. [DBZ-149](https://issues.jboss.org/projects/DBZ/issues/DBZ-149)
* MySQL connector now handles updates to a row's primary/unique key by issuing `DELETE` and tombstone events for the row with the old key, and then an `INSERT` event for the row with the new key. Previously, the `INSERT` was emitted before the `DELETE`. [DBZ-150](https://issues.jboss.org/projects/DBZ/issues/DBZ-150)
* MySQL connector now handles `ENUM` and `SET` literals with parentheses. [DBZ-153](https://issues.jboss.org/projects/DBZ/issues/DBZ-153)


## 0.3.4

October 25, 2016 - [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12331759)

### New features since 0.3.3

* MySQL connector has new `SCHEMA_ONLY` snapshot mode. [DBZ-133](https://issues.jboss.org/projects/DBZ/issues/DBZ-133)
* MySQL connector supports the MySQL `JSON` datatype. [DBZ-126](https://issues.jboss.org/projects/DBZ/issues/DBZ-126)
* MySQL connector metrics exposed via JMX. [DBZ-134](https://issues.jboss.org/projects/DBZ/issues/DBZ-134)

### Backwards-incompatible changes since 0.3.3

None

### Fixes since 0.3.3

This release includes all of the fixes from the [0.3.3](#033) release, and also includes the following fixes:

* MySQL connector's `ts_sec` field now shows correct time from MySQL server events. [DBZ-139](https://issues.jboss.org/projects/DBZ/issues/DBZ-139)


## 0.3.3

October 18, 2016 - [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12331604)

### New features since 0.3.2

None

### Backwards-incompatible changes since 0.3.2

None

### Fixes since 0.3.2

This release includes all of the fixes from the [0.3.2](#032) release, and also includes the following fixes:

* MySQL connector now works with MySQL 5.5. [DBZ-115](https://issues.jboss.org/projects/DBZ/issues/DBZ-115)
* MySQL connector now handles `BIT(n)` column values. [DBZ-123](https://issues.jboss.org/projects/DBZ/issues/DBZ-123)
* MySQL connector supports failing over based on subset of GTIDs. [DBZ-129](https://issues.jboss.org/projects/DBZ/issues/DBZ-129)
* MySQL connector processes GTIDs with line feeds and carriage returns. [DBZ-135](https://issues.jboss.org/projects/DBZ/issues/DBZ-135)
* MySQL connector has improved output of GTIDs and status when reading the binary log. [DBZ-130](https://issues.jboss.org/projects/DBZ/issues/DBZ-130), [DBZ-131](https://issues.jboss.org/projects/DBZ/issues/DBZ-131)
* MySQL connector properly handles multi-character `ENUM` and `SET` values. [DBZ-132](https://issues.jboss.org/projects/DBZ/issues/DBZ-132)


## 0.3.2

September 26, 2016 - [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12331401)

### New features since 0.3.1

None

### Backwards-incompatible changes since 0.3.1

None

### Fixes since 0.3.1

This release includes all of the fixes from the [0.3.1](#031) release, and also includes the following fixes:

* MySQL connector now handles zero-value dates. [DBZ-114](https://issues.jboss.org/projects/DBZ/issues/DBZ-114)
* MySQL connector no longer prints out password-related configuration properties, though [KAFKA-4171](https://issues.apache.org/jira/browse/KAFKA-4171) for a similar issue with Kafka Connect. [DBZ-122](https://issues.jboss.org/projects/DBZ/issues/DBZ-122)
* MySQL connector no longer causes "Error registering AppInfo mbean" warning in Kafka Connect. [DBZ-124](https://issues.jboss.org/projects/DBZ/issues/DBZ-124)
* MySQL connector periodically outputs status when reading binlog. [DBZ-116](https://issues.jboss.org/projects/DBZ/issues/DBZ-116)
* MongoDB connector periodically outputs status when reading binlog. [DBZ-117](https://issues.jboss.org/projects/DBZ/issues/DBZ-117)
* MySQL connector correctly uses long for the `server.id` configuration property. [DBZ-118](https://issues.jboss.org/projects/DBZ/issues/DBZ-118)
* MySQL connector fails or warns when MySQL is not using row-level logging. [DBZ-128](https://issues.jboss.org/projects/DBZ/issues/DBZ-128)


## 0.3.1

August 30, 2016 - [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12331359)

### New features

* Added support for secure (encrypted) connections to MySQL. [DBZ-99](https://issues.jboss.org/projects/DBZ/issues/DBZ-99)

### Backwards-incompatible changes since 0.3.0

None

### Fixes since 0.3.0

This release includes all of the fixes from the [0.2.4](#024) release, and also includes the following fixes:

* MySQL connector now properly decodes string values from the binlog based upon the column's character set encoding as read by the DDL statement. Upon upgrade and restart, the connector will re-read the recorded database history and now associate the columns with their the character sets, and any newly processed events will use properly encoded strings values. As expected, previously generated events are never altered. Force a snapshot to regenerate events for the servers. [DBZ-102](https://issues.jboss.org/projects/DBZ/issues/DBZ-102)
* Corrected how the MySQL connector parses some DDL statements. [DBZ-106](https://issues.jboss.org/projects/DBZ/issues/DBZ-106)
* Corrected the MySQL connector to handle MySQL server GTID sets with newline characters. [DBZ-107](https://issues.jboss.org/projects/DBZ/issues/DBZ-107), [DBZ-111](https://issues.jboss.org/projects/DBZ/issues/DBZ-111)
* Corrected the MySQL connector's startup logic properly compare the MySQL SSL-related system properties to prevent overwriting them. The connector no longer fails when the system properties are the same, which can happen upon restart or starting a second MySQL connector with the same keystore. [DBZ-112](https://issues.jboss.org/projects/DBZ/issues/DBZ-112)
* Removed unused code and test case. [DBZ-108](https://issues.jboss.org/projects/DBZ/issues/DBZ-108)
* Ensure that the MySQL error code and SQLSTATE are included in exceptions reported by the connector. [DBZ-109](https://issues.jboss.org/projects/DBZ/issues/DBZ-109)


## 0.3.0

August 16, 2016 - [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12329661)

### New features

* New MongoDB connector supports capturing changes from a MongoDB replica set or a MongoDB sharded cluster. See the [documentation](http://debezium.io/docs/connectors/mongodb) for details. [DBZ-2](https://issues.jboss.org/projects/DBZ/issues/DBZ-2)

### Backwards-incompatible changes since 0.2.0

* Upgraded to Kafka 0.10.0.1, which means that the Debezium connectors can only be used with Kafka Connect 0.10.0.1. Check Kafka documentation for compatibility with other versions of Kafka brokers. [DBZ-62](https://issues.jboss.org/projects/DBZ/issues/DBZ-62), [DBZ-80](https://issues.jboss.org/projects/DBZ/issues/DBZ-80)
* By default the MySQL connector now represents temporal values with millisecond, microsecond, or nanosecond precision based upon the precision of the source database columns. This changes the schema name of these fields to Debezium-specific constants, and the meaning/interpretation of the literal values now depends on this schema name. To enable previous behavior that always used millisecond precision using only Kafka Connect logical types, set `time.precision.mode` connector property to `connect`. [DBZ-91](https://issues.jboss.org/projects/DBZ/issues/DBZ-91)
* Removed several methods in the `GtidSet` class inside the MySQL connector. The class was introduced in 0.2. This change will only affect applications explicitly using the class (by reusing the MySQL connector JAR), and will not affect how the MySQL connector works. _Changed in 0.2.2._ [DBZ-79](https://issues.jboss.org/projects/DBZ/issues/DBZ-79)
* The `source` field within each MySQL change event now contains the binlog position of that event (rather than the next event). Events persisted by earlier versions of the connector are unaffected. This change _may_ adversely clients that are directly using the position within the `source` field. _Changed in 0.2.2._ [DBZ-76](https://issues.jboss.org/projects/DBZ/issues/DBZ-76)
* Correted the names of the Avro-compliant Kafka Connect schemas generated by the MySQL connector for the `before` and `after` fields in its data change events. Consumers that require knowledge (by name) of the particular schemas used in 0.2 events may have trouble consuming events produced by the 0.2.1 (or later) connector. _Fixed in 0.2.1_. [DBZ-72](https://issues.jboss.org/projects/DBZ/issues/DBZ-72)

### Fixes since 0.2.0

* MySQL snapshots records DDL statements as separate events on the schema change topic. [DBZ-97](https://issues.jboss.org/browse/DBZ-97)
* MySQL connector tolerates binlog filename missing from ROTATE events in certain situations. [DBZ-95](https://issues.jboss.org/browse/DBZ-95)
* Stream result set rows when taking snapshot of MySQL databases to prevent out of memory problems with very large databases. _Fixed in 0.2.4._ [DBZ-94](https://issues.jboss.org/browse/DBZ-94)
* Add more verbose logging statements to the MySQL connector to show progress and activity. _Fixed in 0.2.4._ [DBZ-92](https://issues.jboss.org/browse/DBZ-92)
* Corrected potential error during graceful MySQL connector shutdown. _Fixed in 0.2.4._ [DBZ-103](https://issues.jboss.org/browse/DBZ-103)
* The Kafka Connect schema names used in the MySQL connector's change events are now always Avro-compatible schema names [DBZ-86](https://issues.jboss.org/projects/DBZ/issues/DBZ-86)
* Corrected parsing errors when MySQL DDL statements are generated by Liquibase. _Fixed in 0.2.3._ [DBZ-83](https://issues.jboss.org/browse/DBZ-83)
* Corrected support of MySQL `TINYINT` and `SMALLINT` types. _Fixed in 0.2.3._ [DBZ-84](https://issues.jboss.org/browse/DBZ-84), [DBZ-87](https://issues.jboss.org/browse/DBZ-87)
* Corrected support of MySQL temporal types, including `DATE`, `TIME`, and `TIMESTAMP`. _Fixed in 0.2.3._ [DBZ-85](https://issues.jboss.org/browse/DBZ-85)
* Corrected support of MySQL `ENUM` and `SET` types. [DBZ-100](https://issues.jboss.org/browse/DBZ-100)
* Corrected call to MySQL `SHOW MASTER STATUS` so that it works on pre-5.7 versions of MySQL. _Fixed in 0.2.3._ [DBZ-82](https://issues.jboss.org/browse/DBZ-82)
* Correct how the MySQL connector records offsets with multi-row MySQL events so that, even if the connector experiences a non-graceful shutdown (i.e., crash) after committing the offset of _some_ of the rows from such an event, upon restart the connector will resume with the remaining rows in that multi-row event. Previously, the connector might incorrectly restart at the next event. _Fixed in 0.2.2._ [DBZ-73](https://issues.jboss.org/projects/DBZ/issues/DBZ-73)
* Shutdown of the MySQL connector immediately after a snapshot completes (before another change event is reccorded) will now be properly marked as complete. _Fixed in 0.2.2._ [DBZ-77](https://issues.jboss.org/projects/DBZ/issues/DBZ-77)
* The MySQL connector's plugin archive now contains the MySQL JDBC driver JAR file required by the connector. _Fixed in 0.2.1._ [DBZ-71](https://issues.jboss.org/projects/DBZ/issues/DBZ-71)


## 0.2.4

August 16, 2016 - [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12331221)

### Fixes since 0.2.3

* Stream result set rows when taking snapshot of MySQL databases to prevent out of memory problems with very large databases. [DBZ-94](https://issues.jboss.org/browse/DBZ-94)
* Add more verbose logging statements to the MySQL connector to show progress and activity during snapshots. [DBZ-92](https://issues.jboss.org/browse/DBZ-92)
* Corrected potential error during graceful MySQL connector shutdown. [DBZ-103](https://issues.jboss.org/browse/DBZ-103)


## 0.2.3

July 26, 2016 - [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12330932)

### Backwards-incompatible changes since 0.2.2

None

### Fixes since 0.2.2

* Corrected parsing errors when MySQL DDL statements are generated by Liquibase. [DBZ-83](https://issues.jboss.org/browse/DBZ-83)
* Corrected support of MySQL `TINYINT` and `SMALLINT` types. [DBZ-84](https://issues.jboss.org/browse/DBZ-84), [DBZ-87](https://issues.jboss.org/browse/DBZ-87)
* Corrected support of MySQL temporal types, including `DATE`, `TIME`, and `TIMESTAMP`. [DBZ-85](https://issues.jboss.org/browse/DBZ-85)
* Corrected call to MySQL `SHOW MASTER STATUS` so that it works on pre-5.7 versions of MySQL. [DBZ-82](https://issues.jboss.org/browse/DBZ-82)


## 0.2.2

June 22, 2016 - [Detailed release notes](https://issues.jboss.org/browse/DBZ/versions/12330862)

### Backwards-incompatible changes since 0.2.1

* Removed several methods in the `GtidSet` class inside the MySQL connector. The class was introduced in 0.2. This change will only affect applications explicitly using the class (by reusing the MySQL connector JAR), and will not affect how the MySQL connector works. [DBZ-79](https://issues.jboss.org/projects/DBZ/issues/DBZ-79)
* The `source` field within each MySQL change event now contains the binlog position of that event (rather than the next event). Events persisted by earlier versions of the connector are unaffected. This change _may_ adversely clients that are directly using the position within the `source` field. [DBZ-76](https://issues.jboss.org/projects/DBZ/issues/DBZ-76)

### Fixes since 0.2.1

* Correct how the MySQL connector records offsets with multi-row MySQL events so that, even if the connector experiences a non-graceful shutdown (i.e., crash) after committing the offset of _some_ of the rows from such an event, upon restart the connector will resume with the remaining rows in that multi-row event. Previously, the connector might incorrectly restart at the next event. [DBZ-73](https://issues.jboss.org/projects/DBZ/issues/DBZ-73)
* Shutdown of the MySQL connector immediately after a snapshot completes (before another change event is reccorded) will now be properly marked as complete. [DBZ-77](https://issues.jboss.org/projects/DBZ/issues/DBZ-77)



## 0.2.1

June 10, 2016 - [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12330752)

### Backwards-incompatible changes since 0.2.0

* Correted the names of the Avro-compliant Kafka Connect schemas generated by the MySQL connector for the `before` and `after` fields in its data change events. Consumers that require knowledge (by name) of the particular schemas used in 0.2 events may have trouble consuming events produced by the 0.2.1 (or later) connector. ([DBZ-72](https://issues.jboss.org/projects/DBZ/issues/DBZ-72))

### Fixes since 0.2.0

* The MySQL connector's plugin archive now contains the MySQL JDBC driver JAR file required by the connector.([DBZ-71](https://issues.jboss.org/projects/DBZ/issues/DBZ-71))

## 0.2.0

June 8, 2016 - [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12329465)

### New features

* MySQL connector supports *high availability* MySQL cluster topologies. See the [documentation](http://debezium.io/docs/connectors/mysql) for details. ([DBZ-37](https://issues.jboss.org/projects/DBZ/issues/DBZ-37))
* MySQL connector now by default starts by performing a *consistent snapshot* of the schema and contents of the upstream MySQL databases in its current state. See the [documentation](http://debezium.io/docs/connectors/mysql#snapshots) for details about how this works and how it impacts other database clients. ([DBZ-31](https://issues.jboss.org/projects/DBZ/issues/DBZ-31))
* MySQL connector can be configured to *exclude*, *truncate*, or *mask* specific columns in events. ([DBZ-29](https://issues.jboss.org/projects/DBZ/issues/DBZ-29))
* MySQL connector events can be serialized using the [Confluent Avro converter](http://docs.confluent.io/3.0.0/avro.html) or the JSON converter. Previously, only the JSON converter could be used. ([DBZ-29](https://issues.jboss.org/projects/DBZ/issues/DBZ-29), [DBZ-63](https://issues.jboss.org/projects/DBZ/issues/DBZ-63), [DBZ-64](https://issues.jboss.org/projects/DBZ/issues/DBZ-64))

### Backwards-incompatible changes since 0.1

* Completely redesigned the structure of event messages produced by MySQL connector and stored in Kafka topics. Events now contain an _envelope_ structure with information about the source event, the kind of operation (create/insert, update, delete, read), the time that Debezium processed the event, and the state of the row before and/or after the event. The messages written to each topic have a distinct Avro-compliant Kafka Connect schema that reflects the structure of the source table, which may vary over time independently from the schemas of all other topics. See the [documentation](http://debezium.io/docs/connectors/mysql#events) for details. This envelope structure will likely be used by future connectors. ([DBZ-50](https://issues.jboss.org/projects/DBZ/issues/DBZ-50), [DBZ-52](https://issues.jboss.org/projects/DBZ/issues/DBZ-52), [DBZ-45](https://issues.jboss.org/projects/DBZ/issues/DBZ-45), [DBZ-60](https://issues.jboss.org/projects/DBZ/issues/DBZ-60))
* MySQL connector handles deletion of a row by recording a delete event message whose value contains the state of the removed row (and other metadata), followed by a _tombstone event_ message with a null value to signal *Kafka's log compaction* that all prior messages with the same key can be garbage collected. See the [documentation](http://debezium.io/docs/connectors/mysql#events) for details. ([DBZ-44](https://issues.jboss.org/projects/DBZ/issues/DBZ-44))
* Changed the format of events that the MySQL connector writes to its schema change topic, through which consumers can access events with the DDL statements applied to the database(s). The format change makes it possible for consumers to correlate these events with the data change events. ([DBZ-43](https://issues.jboss.org/projects/DBZ/issues/DBZ-43), [DBZ-55](https://issues.jboss.org/projects/DBZ/issues/DBZ-55))

### Changes since 0.1

* DDL parsing framework identifies table affected by statements via a new listener callback. ([DBZ-38](https://issues.jboss.org/projects/DBZ/issues/DBZ-38))
* The `database.binlog` configuration property was required in version 0.1 of the MySQL connector, but in 0.2 it is no longer used because of the new snapshot feature. If provided, it will be quietly ignored. ([DBZ-31](https://issues.jboss.org/projects/DBZ/issues/DBZ-31))

### Bug fixes since 0.1

* MySQL connector now properly parses `COMMIT` statements, the `REFERENCES` clauses of `CREATE TABLE` statements, and statements with `CHARSET` shorthand of `CHARACTER SET`. ([DBZ-48](https://issues.jboss.org/projects/DBZ/issues/DBZ-48), [DBZ-49](https://issues.jboss.org/projects/DBZ/issues/DBZ-49), [DBZ-57](https://issues.jboss.org/projects/DBZ/issues/DBZ-57))
* MySQL connector properly handles binary values that are hexadecimal strings ([DBZ-61](https://issues.jboss.org/projects/DBZ/issues/DBZ-61))

## 0.1

March 17, 2016 - [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12329464)

### New features

* MySQL connector for ingesting change events from MySQL databases. ([DBZ-1](https://issues.jboss.org/projects/DBZ/issues/DBZ-1))
* Kafka Connect plugin archive for MySQL connector. ([DBZ-17](https://issues.jboss.org/projects/DBZ/issues/DBZ-17))
* Simple DDL parsing framework that can be extended and used by various connectors. ([DBZ-1](https://issues.jboss.org/projects/DBZ/issues/DBZ-1))
* Framework for embedding a single Kafka Connect connector inside an application. ([DBZ-8](https://issues.jboss.org/projects/DBZ/issues/DBZ-8))
