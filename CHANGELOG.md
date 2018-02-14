# Change log

All notable changes are documented in this file. Release numbers follow [Semantic Versioning](http://semver.org)

## 0.7.3
February 14th, 2018 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12336643)

### New features since 0.7.2

* MySQL connector should automatically create database history topic [DBZ-278](https://issues.jboss.org/browse/DBZ-278)
* Change OpenShift instructions to use Strimzi [DBZ-545](https://issues.jboss.org/browse/DBZ-545)
* Create an internal namespace for configuration options not intended for general usage [DBZ-576](https://issues.jboss.org/browse/DBZ-576)
* Make ChainedReader immutable [DBZ-583](https://issues.jboss.org/browse/DBZ-583)
* Snapshots are not interruptable with the Postgres connector [DBZ-586](https://issues.jboss.org/browse/DBZ-586)
* Add optional field with Debezium version to "source" element of messages [DBZ-593](https://issues.jboss.org/browse/DBZ-593)
* Add the ability to control the strategy for committing offsets by the offset store [DBZ-537](https://issues.jboss.org/browse/DBZ-537)
* Create support for arrays of PostGIS types [DBZ-571](https://issues.jboss.org/browse/DBZ-571)
* Add option for controlling whether to produce tombstone records on DELETE operations [DBZ-582](https://issues.jboss.org/browse/DBZ-582)
* Add example for using the MongoDB event flattening SMT [DBZ-567](https://issues.jboss.org/browse/DBZ-567)
* Prefix the names of all threads spawned by Debezium with "debezium-" [DBZ-587](https://issues.jboss.org/browse/DBZ-587)


### Breaking changes since 0.7.2

A new namespace for parameters was [created](https://issues.jboss.org/browse/DBZ-576) - `internal` - that is used for parameters that are not documented and should not be used as they are subject of changes without warning. As a result of this change the undocumented parameter `database.history.ddl.filter` was renamed to `internal.database.history.ddl.filter`.

OpenShift deployment now uses templates and images from [Strimzi project](https://issues.jboss.org/browse/DBZ-545).


### Fixes and changes since 0.7.2

* Force DBZ to commit regularly [DBZ-220](https://issues.jboss.org/browse/DBZ-220)
* Carry over SourceInfo.restartEventsToSkip to next binlog file handling cause binlog events are not written to kafka [DBZ-572](https://issues.jboss.org/browse/DBZ-572)
* Numeric arrays not handled correctly [DBZ-577](https://issues.jboss.org/browse/DBZ-577)
* Debezium skipping binlog events silently [DBZ-588](https://issues.jboss.org/browse/DBZ-588)
* Stop the connector if WALs to continue from aren't available [DBZ-590](https://issues.jboss.org/browse/DBZ-590)
* Producer thread of DB history topic leaks after connector shut-down [DBZ-595](https://issues.jboss.org/browse/DBZ-595)
* Integration tests should have completely isolated environment and configuration/setup files [DBZ-300](https://issues.jboss.org/browse/DBZ-300)
* MongoDB integration tests should have completely isolated environment and configuration/setup files [DBZ-579](https://issues.jboss.org/browse/DBZ-579)
* Extract a separate change event class to be re-used across connectors [DBZ-580](https://issues.jboss.org/browse/DBZ-580)
* Propagate producer errors to Kafka Connect in MongoDB connector [DBZ-581](https://issues.jboss.org/browse/DBZ-581)
* Shutdown thread pool used for MongoDB snaphots once it's not needed anymore [DBZ-594](https://issues.jboss.org/browse/DBZ-594)
* Refactor type and array handling for Postgres [DBZ-609](https://issues.jboss.org/browse/DBZ-609)
* Avoid unneccessary schema refreshs [DBZ-616](https://issues.jboss.org/browse/DBZ-616)
* Incorrect type retrieved by stream producer for column TIMESTAMP (0) WITH TIME ZONE [DBZ-618](https://issues.jboss.org/browse/DBZ-618)


## 0.7.2
January 25th, 2018 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?version=12336456&projectId=12317320)

### New features since 0.7.1

* As a Debezium user, I would like MySQL Connector to support 'Spatial' data types [DBZ-208](https://issues.jboss.org/browse/DBZ-208)
* Allow easy consumption of MongoDB CDC events by other connectors [DBZ-409](https://issues.jboss.org/browse/DBZ-409)
* New snapshotting mode for recovery of DB history topic [DBZ-443](https://issues.jboss.org/browse/DBZ-443)
* Add support for Postgres VARCHAR array columns [DBZ-506](https://issues.jboss.org/browse/DBZ-506)
* Unified Geometry type support [DBZ-507](https://issues.jboss.org/browse/DBZ-507)
* Add support for "snapshot.select.statement.overrides" option for Postgres [DBZ-510](https://issues.jboss.org/browse/DBZ-510)
* Make PostGIS optional in Postgres Docker images [DBZ-526](https://issues.jboss.org/browse/DBZ-526)
* Provide an option to only store DDL statements referring to captured tables in DB history topic [DBZ-541](https://issues.jboss.org/browse/DBZ-541)
* Add ToC to tutorial and make section captions linkable [DBZ-369](https://issues.jboss.org/browse/DBZ-369)
* Remove Zulu JDK images [DBZ-449](https://issues.jboss.org/browse/DBZ-449)
* Add example for sending CDC events to Elasticsearch [DBZ-502](https://issues.jboss.org/browse/DBZ-502)
* Adapt examples to MongoDB 3.6 [DBZ-509](https://issues.jboss.org/browse/DBZ-509)
* Backport add-ons definition from add-ons repo [DBZ-520](https://issues.jboss.org/browse/DBZ-520)
* Set up pull request build job for testing the PG connector with wal2json [DBZ-568](https://issues.jboss.org/browse/DBZ-568)


### Breaking changes since 0.7.1

There are no breaking changes in this release.


### Fixes and changes since 0.7.1

* Debezium MySQL connector only works for lower-case table names on case-insensitive file systems [DBZ-392](https://issues.jboss.org/browse/DBZ-392)
* Numbers after decimal point are different between source and destination [DBZ-423](https://issues.jboss.org/browse/DBZ-423)
* Fix support for date arrays [DBZ-494](https://issues.jboss.org/browse/DBZ-494)
* Changes in type contraints will not trigger new schema [DBZ-504](https://issues.jboss.org/browse/DBZ-504)
* Task is still running after connector is paused [DBZ-516](https://issues.jboss.org/browse/DBZ-516)
* NPE happened for PAUSED task [DBZ-519](https://issues.jboss.org/browse/DBZ-519)
* Possibility of commit LSN before record is consumed/notified [DBZ-521](https://issues.jboss.org/browse/DBZ-521)
* Snapshot fails when encountering null MySQL TIME fields [DBZ-522](https://issues.jboss.org/browse/DBZ-522)
* Debezium unable to parse DDLs in MySql with RESTRICT contstraint [DBZ-524](https://issues.jboss.org/browse/DBZ-524)
* DateTimeFormatter Exception in wal2json [DBZ-525](https://issues.jboss.org/browse/DBZ-525)
* Multiple partitions does not work in ALTER TABLE [DBZ-530](https://issues.jboss.org/browse/DBZ-530)
* Incorrect lookup in List in MySqlDdlParser.parseCreateView [DBZ-534](https://issues.jboss.org/browse/DBZ-534)
* Improve invalid DDL statement logging [DBZ-538](https://issues.jboss.org/browse/DBZ-538)
* Fix required protobuf version in protobuf decoder documentation [DBZ-542](https://issues.jboss.org/browse/DBZ-542)
* EmbeddedEngine strips settings required to use KafkaOffsetBackingStore [DBZ-555](https://issues.jboss.org/browse/DBZ-555)
* Handling of date arrays collides with handling of type changes via wal2json [DBZ-558](https://issues.jboss.org/browse/DBZ-558)
* ROLLBACK to savepoint cannot be parsed [DBZ-411](https://issues.jboss.org/browse/DBZ-411)
* Avoid usage of deprecated numeric types constructors [DBZ-455](https://issues.jboss.org/browse/DBZ-455)
* Don't add source and JavaDoc JARs to Kafka image [DBZ-489](https://issues.jboss.org/browse/DBZ-489)


## 0.7.1
December 20th, 2017 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?version=12336215&projectId=12317320)

### New features since 0.7.0

* Provide a wal2json plug-in mode enforcing RDS environment [DBZ-517](https://issues.jboss.org/browse/DBZ-517)


### Breaking changes since 0.7.0

There are no breaking changes in this release.


### Fixes and changes since 0.7.0

* For old connector OID should be used to detect schema change [DBZ-512](https://issues.jboss.org/browse/DBZ-512)
* AWS RDS Postgresql 9.6.5 not supporting "include-not-null" = "true" in connector setup [DBZ-513](https://issues.jboss.org/browse/DBZ-513)
* RecordsStreamProducerIT.shouldNotStartAfterStop can make subsequent test dependent [DBZ-518](https://issues.jboss.org/browse/DBZ-518)


## 0.7.0
December 15th, 2017 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?version=12335366&projectId=12317320)

### New features since 0.6.2

* PostgreSQL connector should work on Amazon RDS and be able to use the available plugin [DBZ-256](https://issues.jboss.org/browse/DBZ-256)
* Build Debezium against Kafka 1.0.0 [DBZ-432](https://issues.jboss.org/browse/DBZ-432)
* Build Debezium images with Kafka 1.0.0 [DBZ-433](https://issues.jboss.org/browse/DBZ-433)
* Protobuf message should contain type modifiers [DBZ-485](https://issues.jboss.org/browse/DBZ-485)
* Protobuf message should contain optional flags [DBZ-486](https://issues.jboss.org/browse/DBZ-486)
* Better support for large append-only tables by making the snapshotting process restartable [DBZ-349](https://issues.jboss.org/browse/DBZ-349)
* Support new wal2json type specifiers [DBZ-453](https://issues.jboss.org/browse/DBZ-453)
* Optionally return raw value for unsupported column types [DBZ-498](https://issues.jboss.org/browse/DBZ-498)
* Provide Postgres example image for 0.7 [DBZ-382](https://issues.jboss.org/browse/DBZ-382)
* Create an automated build for Postgres example image in Docker Hub [DBZ-383](https://issues.jboss.org/browse/DBZ-383)
* Move configuration of ProtoBuf code generation to Postgres module [DBZ-416](https://issues.jboss.org/browse/DBZ-416)
* Provide MongoDB example image for Debezium 0.7 [DBZ-451](https://issues.jboss.org/browse/DBZ-451)
* Upgrade to Confluent Platform 4.0 [DBZ-492](https://issues.jboss.org/browse/DBZ-492)
* Set up CI job for testing Postgres with new wal2json type identifiers [DBZ-495](https://issues.jboss.org/browse/DBZ-495)
* Change PostgreSQL connector to support multiple plugins [DBZ-257](https://issues.jboss.org/browse/DBZ-257)
* PostgreSQL connector should support the wal2json logical decoding plugin [DBZ-258](https://issues.jboss.org/browse/DBZ-258)
* Provide instructions for using Debezium on Minishift [DBZ-364](https://issues.jboss.org/browse/DBZ-364)
* Modify BinlogReader to process transactions via buffer [DBZ-405](https://issues.jboss.org/browse/DBZ-405)
* Modify BinlogReader to support transactions of unlimited size [DBZ-406](https://issues.jboss.org/browse/DBZ-406)


### Breaking changes since 0.6.2

This release includes the following changes that can affect existing installations

* Change default setting for BIGINT UNSIGNED handling [DBZ-461](https://issues.jboss.org/browse/DBZ-461)
* Invalid value for HourOfDay ConnectException when the value of MySQL TIME filed is above 23:59:59 [DBZ-342](https://issues.jboss.org/browse/DBZ-342)
* Postgres connectors stops to work after concurrent schema changes and updates [DBZ-379](https://issues.jboss.org/browse/DBZ-379)
* Hardcoded schema version overrides schema registry version [DBZ-466](https://issues.jboss.org/browse/DBZ-466)


### Fixes and changes since 0.6.2

* Data are read from the binlog and not written into Kafka [DBZ-390](https://issues.jboss.org/browse/DBZ-390)
* MySQL connector may not read database history to end [DBZ-464](https://issues.jboss.org/browse/DBZ-464)
* connect-base image advertises wrong port by default [DBZ-467](https://issues.jboss.org/browse/DBZ-467)
* INSERT statements being written to db history topic [DBZ-469](https://issues.jboss.org/browse/DBZ-469)
* MySQL Connector does not handle properly startup/shutdown [DBZ-473](https://issues.jboss.org/browse/DBZ-473)
* Cannot parse NOT NULL COLLATE in DDL [DBZ-474](https://issues.jboss.org/browse/DBZ-474)
* Failed to parse the sql statement of RENAME user [DBZ-475](https://issues.jboss.org/browse/DBZ-475)
* Exception when parsing enum field with escaped characters values [DBZ-476](https://issues.jboss.org/browse/DBZ-476)
* All to insert null value into numeric array columns [DBZ-478](https://issues.jboss.org/browse/DBZ-478)
* produceStrings method slow down on sending messages [DBZ-479](https://issues.jboss.org/browse/DBZ-479)
* Failing unit tests when run in EST timezone [DBZ-491](https://issues.jboss.org/browse/DBZ-491)
* PostgresConnector falls with RejectedExecutionException [DBZ-501](https://issues.jboss.org/browse/DBZ-501)
* Docker images cannot be re-built when a new version of ZooKeeper/Kafka is released [DBZ-503](https://issues.jboss.org/browse/DBZ-503)
* Insert ids as long instead of float for MongoDB example image [DBZ-470](https://issues.jboss.org/browse/DBZ-470)
* Port changes in 0.6 Docker files into 0.7 files [DBZ-463](https://issues.jboss.org/browse/DBZ-463)
* Add check to release process to make sure all issues are assigned to a component [DBZ-468](https://issues.jboss.org/browse/DBZ-468)
* Document requirement for database history topic to be not partitioned [DBZ-482](https://issues.jboss.org/browse/DBZ-482)
* Remove dead code from MySqlSchema [DBZ-483](https://issues.jboss.org/browse/DBZ-483)
* Remove redundant calls to pfree [DBZ-496](https://issues.jboss.org/browse/DBZ-496)


## 0.6.2
November 15th, 2017 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?version=12335989&projectId=12317320)

### New features since 0.6.1

* Log current position in MySQL binlog to simplify debugging [DBZ-401](https://issues.jboss.org/projects/DBZ/issues/DBZ-401)
* Support PostgreSQL 10 [DBZ-424](https://issues.jboss.org/projects/DBZ/issues/DBZ-424)
* Create a Docker image for PostgreSQL 10 [DBZ-426](https://issues.jboss.org/projects/DBZ/issues/DBZ-426)
* Add example for using Avro messages [DBZ-430](https://issues.jboss.org/projects/DBZ/issues/DBZ-430)
* Make postGIS dependency optional [DBZ-445](https://issues.jboss.org/projects/DBZ/issues/DBZ-445)
* Avro console-consumer example in docs [DBZ-458](https://issues.jboss.org/projects/DBZ/issues/DBZ-458)
* Docker micro version tags (e.g., 0.6.1) [DBZ-418](https://issues.jboss.org/projects/DBZ/issues/DBZ-418)
* Create a CI job for testing with PostgreSQL 10 [DBZ-427](https://issues.jboss.org/projects/DBZ/issues/DBZ-427)
* Upgrade dependencies in Docker images to match Kafka 0.11.0.1 [DBZ-450](https://issues.jboss.org/projects/DBZ/issues/DBZ-450)


### Breaking changes since 0.6.1

* Timestamp field not handle time zone correctly [DBZ-260](https://issues.jboss.org/projects/DBZ/issues/DBZ-260)
  * This issue finally fixes a long standing bug in timestamp timezone handling. If there is a client that was depending on this bug to provide value without the correct offset then it has to be fixed.


### Fixes and changes since 0.6.1

* Connector fails and stops when coming across corrupt event [DBZ-217](https://issues.jboss.org/projects/DBZ/issues/DBZ-217)
* [Postgres] Interval column causes exception during handling of DELETE [DBZ-259](https://issues.jboss.org/projects/DBZ/issues/DBZ-259)
* The scope of the Kafka Connect dependency should be "provided" [DBZ-285](https://issues.jboss.org/projects/DBZ/issues/DBZ-285)
* KafkaCluster#withKafkaConfiguration() does not work [DBZ-323](https://issues.jboss.org/projects/DBZ/issues/DBZ-323)
* MySQL connector "initial_only" snapshot mode results in CPU spike from ConnectorTask polling [DBZ-396](https://issues.jboss.org/projects/DBZ/issues/DBZ-396)
* Allow to omit COLUMN word in ALTER TABLE MODIFY/ALTER/CHANGE [DBZ-412](https://issues.jboss.org/projects/DBZ/issues/DBZ-412)
* MySQL connector should handle stored procedure definitions [DBZ-415](https://issues.jboss.org/projects/DBZ/issues/DBZ-415)
* Support constraints without name in DDL statement [DBZ-419](https://issues.jboss.org/projects/DBZ/issues/DBZ-419)
* Short field not null throw an exception [DBZ-422](https://issues.jboss.org/projects/DBZ/issues/DBZ-422)
* ALTER TABLE cannot change default value of column [DBZ-425](https://issues.jboss.org/projects/DBZ/issues/DBZ-425)
* DDL containing text column with length specification cannot be parsed [DBZ-428](https://issues.jboss.org/projects/DBZ/issues/DBZ-428)
* Integer column with negative default value causes MySQL connector to crash [DBZ-429](https://issues.jboss.org/projects/DBZ/issues/DBZ-429)
* MySQL procedure parser handles strings and keywords as same tokens [DBZ-437](https://issues.jboss.org/projects/DBZ/issues/DBZ-437)
* Mongo initial sync misses records with initial.sync.max.threads > 1 [DBZ-438](https://issues.jboss.org/projects/DBZ/issues/DBZ-438)
* Can't parse DDL containing PRECISION clause without parameters [DBZ-439](https://issues.jboss.org/projects/DBZ/issues/DBZ-439)
* Task restart triggers MBean to register twice [DBZ-447](https://issues.jboss.org/projects/DBZ/issues/DBZ-447)
* Remove slowness in KafkaDatabaseHistoryTest [DBZ-456](https://issues.jboss.org/projects/DBZ/issues/DBZ-456)


## 0.6.1
October 26th, 2017 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?version=12335619&projectId=12317320)

### New features since 0.6.0

* Support for UNSIGNED BIGINT to not be treated as byte[] [DBZ-363](https://issues.jboss.org/projects/DBZ/issues/DBZ-363)
* Make Debezium build on Java 9 [DBZ-227](https://issues.jboss.org/projects/DBZ/issues/DBZ-227)
* Add a test for "PAGE_CHECKSUM" DDL option [DBZ-336](https://issues.jboss.org/projects/DBZ/issues/DBZ-336)
* Provide tutorial Docker Compose files for MongoDB and Postgres [DBZ-361](https://issues.jboss.org/projects/DBZ/issues/DBZ-361)
* Upgrade to latest Kafka 0.11.x [DBZ-367](https://issues.jboss.org/projects/DBZ/issues/DBZ-367)
* Prevent warning when building the plug-ins [DBZ-370](https://issues.jboss.org/projects/DBZ/issues/DBZ-370)
* Replace hard-coded version references with variables [DBZ-371](https://issues.jboss.org/projects/DBZ/issues/DBZ-371)
* Upgrade to latest version of mysql-binlog-connector-java [DBZ-398](https://issues.jboss.org/projects/DBZ/issues/DBZ-398)
* Create wal2json CI job [DBZ-403](https://issues.jboss.org/projects/DBZ/issues/DBZ-403)
* Travis jobs tests are failing due to Postgres [DBZ-404](https://issues.jboss.org/projects/DBZ/issues/DBZ-404)


### Breaking changes since 0.6.0

There should be no breaking changes in this relese.


### Fixes and changes since 0.6.0

* Avoid NullPointerException when closing MySQL connector after another error [DBZ-378](https://issues.jboss.org/projects/DBZ/issues/DBZ-378)
* RecordsStreamProducer#streamChanges() can die on an exception without failing the connector [DBZ-380](https://issues.jboss.org/projects/DBZ/issues/DBZ-380)
* Encoding to JSON does not support all MongoDB types [DBZ-385](https://issues.jboss.org/projects/DBZ/issues/DBZ-385)
* MySQL connector does not filter out DROP TEMP TABLE statements from DB history topic [DBZ-395](https://issues.jboss.org/projects/DBZ/issues/DBZ-395)
* Binlog Reader is registering MXBean when using "initial_only" snapshot mode [DBZ-402](https://issues.jboss.org/projects/DBZ/issues/DBZ-402)
* A column named `column`, even when properly escaped, causes exception [DBZ-408](https://issues.jboss.org/projects/DBZ/issues/DBZ-408)


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

There should be no breaking changes in this release.

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
