# Change log

All notable changes are documented in this file. Release numbers follow [Semantic Versioning](http://semver.org)

## 3.3.0.CR1
September 24th 2025 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12467279)

### New features since 3.3.0.Beta1

* Make database exceptions retriable in JDBC sink connector [DBZ-7772](https://issues.redhat.com/browse/DBZ-7772)
* Add Support for Multiple Oracle Log Miner Destinations by Precedence [DBZ-9041](https://issues.redhat.com/browse/DBZ-9041)
* Upgrade to Kafka 4.1.0 [DBZ-9460](https://issues.redhat.com/browse/DBZ-9460)


### Breaking changes since 3.3.0.Beta1

* Offset position validation for Db2 is not reliable [DBZ-9470](https://issues.redhat.com/browse/DBZ-9470)
* Update JDBC sink Hibernate dependency to 7.1.0.Final [DBZ-9481](https://issues.redhat.com/browse/DBZ-9481)


### Fixes since 3.3.0.Beta1

* Dropping in process batch transactions when shutting down [DBZ-8060](https://issues.redhat.com/browse/DBZ-8060)
* A transaction mined across two queries can randomly cause unsupported operations [DBZ-8747](https://issues.redhat.com/browse/DBZ-8747)
* Non-standard number format causing NumberFormatException [DBZ-9181](https://issues.redhat.com/browse/DBZ-9181)
* Oracle connector reselect  exception handling (ORA-01555 + ORA-22924) [DBZ-9446](https://issues.redhat.com/browse/DBZ-9446)
* In case of readonly usage the DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG should not be used [DBZ-9452](https://issues.redhat.com/browse/DBZ-9452)
* Debezium server fails with CNFE [DBZ-9468](https://issues.redhat.com/browse/DBZ-9468)
* OutOfMemory exception when recreating list of tables for snapshot callables [DBZ-9472](https://issues.redhat.com/browse/DBZ-9472)
* Debezium Server  raise  "AttributeNotFoundException QueueTotalCapacity" with SqlServer source [DBZ-9477](https://issues.redhat.com/browse/DBZ-9477)
* Getting "Unknown column in 'field list'" when column name contains backtick [DBZ-9479](https://issues.redhat.com/browse/DBZ-9479)
* MySQL Event get header throws NullPointerException [DBZ-9483](https://issues.redhat.com/browse/DBZ-9483)


### Other changes since 3.3.0.Beta1

* Add unit tests to WalPositionLocator [DBZ-5978](https://issues.redhat.com/browse/DBZ-5978)
* Add REST API to get retrieve the list of tables [DBZ-9317](https://issues.redhat.com/browse/DBZ-9317)
* Source and Destination entities must be linked to the Connection entity [DBZ-9333](https://issues.redhat.com/browse/DBZ-9333)
* Implement Kafka connection validator [DBZ-9334](https://issues.redhat.com/browse/DBZ-9334)
* Unpin netty image pin [DBZ-9390](https://issues.redhat.com/browse/DBZ-9390)
* Update JDBC sink connector doc to identify the data types that Debezium does not support [DBZ-9403](https://issues.redhat.com/browse/DBZ-9403)
* Expose endpoint for get json schemas about connection [DBZ-9420](https://issues.redhat.com/browse/DBZ-9420)
* Add missing destinations to Debezium Platform  [DBZ-9442](https://issues.redhat.com/browse/DBZ-9442)
* Improve maven compiler config for Debezium Platform [DBZ-9453](https://issues.redhat.com/browse/DBZ-9453)
* Zookeeperless kafka for DockerRhel executions [DBZ-9462](https://issues.redhat.com/browse/DBZ-9462)
* Publish in Quarkus the extension for Postgres [DBZ-9478](https://issues.redhat.com/browse/DBZ-9478)
* Update Mockito to 5.19.0 [DBZ-9480](https://issues.redhat.com/browse/DBZ-9480)
* Update to AssertJ 3.27.5 [DBZ-9482](https://issues.redhat.com/browse/DBZ-9482)
* Declate source/transforms in ServiceLoader manifests so it can be compatible with new plugin discovery mode  [DBZ-9493](https://issues.redhat.com/browse/DBZ-9493)



## 3.3.0.Beta1
September 5th 2025 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12466490)

### New features since 3.3.0.Alpha2

* Inform the user about any potential impact on the existing pipelines on updating one of the dependant resource [DBZ-9104](https://issues.redhat.com/browse/DBZ-9104)
* Add support for tracing.* config options in MongoEventRouter [DBZ-9328](https://issues.redhat.com/browse/DBZ-9328)
* Migrate External Connectors to ScheduledHeartbeat [DBZ-9377](https://issues.redhat.com/browse/DBZ-9377)
* Prevent ALTER PUBLICATION queries for filtered publications when not required [DBZ-9395](https://issues.redhat.com/browse/DBZ-9395)
* Oracle LastBatchProcessingThroughput should use JdbcRows rather than CountedChanges [DBZ-9399](https://issues.redhat.com/browse/DBZ-9399)


### Breaking changes since 3.3.0.Alpha2

* Standardize Cassandra connector JMX metrics naming convention to match other Debezium connectors [DBZ-9281](https://issues.redhat.com/browse/DBZ-9281)
* Embedded Engine: ClassLoader management for frameworks like SpringBoot [DBZ-9375](https://issues.redhat.com/browse/DBZ-9375)


### Fixes since 3.3.0.Alpha2

* Debezium not recovering from connection errors [DBZ-7872](https://issues.redhat.com/browse/DBZ-7872)
* JdbcSchemaHistory Fails to Handle Data Sharding When Recovering Records [DBZ-8979](https://issues.redhat.com/browse/DBZ-8979)
*  Quarkus-Debezium-Extension does not work with Hibernate ORM 7  [DBZ-9193](https://issues.redhat.com/browse/DBZ-9193)
* Data loss occurs when connector restarts after failed ad-hoc blocking snapshot [DBZ-9337](https://issues.redhat.com/browse/DBZ-9337)
* Oracle connector does not support large CLOB and BLOB values [DBZ-9392](https://issues.redhat.com/browse/DBZ-9392)
* Oracle DDL parser exception - DROP MATERIALIZED [DBZ-9397](https://issues.redhat.com/browse/DBZ-9397)
* Debezium Platform OpenAPI spec miss returns schemas [DBZ-9405](https://issues.redhat.com/browse/DBZ-9405)
* Oracle connector does not parse syntax : PARALLEL in DDL  [DBZ-9406](https://issues.redhat.com/browse/DBZ-9406)
* Increase max allowed json string length [DBZ-9407](https://issues.redhat.com/browse/DBZ-9407)
* Wrong default value of task.management.timeout.ms [DBZ-9408](https://issues.redhat.com/browse/DBZ-9408)
* LCR flushing can cause low watermark to be invalidated [DBZ-9413](https://issues.redhat.com/browse/DBZ-9413)
* Oracle connector will fail with cryptic error when offset position is no longer valid [DBZ-9416](https://issues.redhat.com/browse/DBZ-9416)
* Context headers are added two times during an incremental snapshot [DBZ-9422](https://issues.redhat.com/browse/DBZ-9422)
* Operator CI fails on main trying to checkout core repo [DBZ-9429](https://issues.redhat.com/browse/DBZ-9429)


### Other changes since 3.3.0.Alpha2

* Stop building zookeeper images [DBZ-9189](https://issues.redhat.com/browse/DBZ-9189)
* Debezium Engine Quarkus Extension: create documentation section [DBZ-9294](https://issues.redhat.com/browse/DBZ-9294)
* Debezium Engine Quarkus Extension: add a quick start in the example repository [DBZ-9301](https://issues.redhat.com/browse/DBZ-9301)
* Debezium Engine Quarkus Extension: blog post [DBZ-9311](https://issues.redhat.com/browse/DBZ-9311)
* Add a rest API to validate a connection [DBZ-9315](https://issues.redhat.com/browse/DBZ-9315)
* MariaDB connector documentation should make clear compressed logs are not supported [DBZ-9367](https://issues.redhat.com/browse/DBZ-9367)
* EmbeddedEngineTest#shouldHandleNoDefaultOffsetFlushInterval fail randomly [DBZ-9398](https://issues.redhat.com/browse/DBZ-9398)
* Improve blocking snapshot test resilience [DBZ-9410](https://issues.redhat.com/browse/DBZ-9410)
* Infinispan Protostream compatibility for Java compiler > 22 [DBZ-9417](https://issues.redhat.com/browse/DBZ-9417)



## 3.3.0.Alpha2
August 26th 2025 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12465252)

### New features since 3.3.0.Alpha1

* Support for TSVECTOR data types for postgres Sink connector [DBZ-8471](https://issues.redhat.com/browse/DBZ-8471)
* Improve error management and messages on Debezium Platform backend [DBZ-8836](https://issues.redhat.com/browse/DBZ-8836)
* Add UI support for Fine-grained logging configuration [DBZ-8890](https://issues.redhat.com/browse/DBZ-8890)
* Update tutorial to use KRaft mode [DBZ-9184](https://issues.redhat.com/browse/DBZ-9184)
* Add support for MongoDB Source Connector to start CDC from a specified point [DBZ-9240](https://issues.redhat.com/browse/DBZ-9240)
* Introduce timeout for ALTER/CREATE Publication statment [DBZ-9310](https://issues.redhat.com/browse/DBZ-9310)
* Allow redo thread flush scn adjustment to be configurable [DBZ-9344](https://issues.redhat.com/browse/DBZ-9344)
* Improve performance validation on whether a supplied value is a toast column [DBZ-9345](https://issues.redhat.com/browse/DBZ-9345)
* SQL Server emits heartbeats while catching up [DBZ-9364](https://issues.redhat.com/browse/DBZ-9364)
* Add detail pages to source and destination [DBZ-9373](https://issues.redhat.com/browse/DBZ-9373)


### Breaking changes since 3.3.0.Alpha1

None


### Fixes since 3.3.0.Alpha1

* Incremental snapshot offset failing to load on task restart [DBZ-9209](https://issues.redhat.com/browse/DBZ-9209)
* Update kafka.version  to 4.0.0 in parent pom [DBZ-9300](https://issues.redhat.com/browse/DBZ-9300)
* Debezium Server Azure Event Hubs sink duplicates all previous events [DBZ-9304](https://issues.redhat.com/browse/DBZ-9304)
* Archive log only mode does not pause mining when no more data available [DBZ-9306](https://issues.redhat.com/browse/DBZ-9306)
* Mockit error in EventDispatcherTest [DBZ-9332](https://issues.redhat.com/browse/DBZ-9332)
* Events may be mistakenly processed multiple times using multiple tasks [DBZ-9338](https://issues.redhat.com/browse/DBZ-9338)
* Debezium constatly performs heartbeat.action.query instead of honoring heartbeat.interval.ms [DBZ-9340](https://issues.redhat.com/browse/DBZ-9340)
* OCP tests fails to start [DBZ-9342](https://issues.redhat.com/browse/DBZ-9342)
* Fetching transaction event count can result in NullPointerException [DBZ-9349](https://issues.redhat.com/browse/DBZ-9349)
* Ensure JAVA_OPTS env var is correct on dbz server startup [DBZ-9352](https://issues.redhat.com/browse/DBZ-9352)
* Issue in ReselectColumnsPostProcessor when field's schema type is BYTES [DBZ-9356](https://issues.redhat.com/browse/DBZ-9356)
* MariaDB fails to parse ALTER TABLE using RENAME COLUMN IF EXISTS syntax [DBZ-9358](https://issues.redhat.com/browse/DBZ-9358)
* Oracle fails to reselect columns when table structure changes and throws ORA-01466 [DBZ-9359](https://issues.redhat.com/browse/DBZ-9359)
* Single quotes getting double quotes in a create operation [DBZ-9366](https://issues.redhat.com/browse/DBZ-9366)
* Mining upper boundary is miscalculated when using archive log only mode [DBZ-9370](https://issues.redhat.com/browse/DBZ-9370)
* Proper Kafka producer exception not logged due to record.key serialisation error [DBZ-9378](https://issues.redhat.com/browse/DBZ-9378)


### Other changes since 3.3.0.Alpha1

* Include XStream classes in downstream builds [DBZ-8828](https://issues.redhat.com/browse/DBZ-8828)
* Remove any dummy value that are are using from showing them in UI [DBZ-9106](https://issues.redhat.com/browse/DBZ-9106)
* Clarify IBM DB2 IIDR licensing requirement in Db2 connector documentation [DBZ-9121](https://issues.redhat.com/browse/DBZ-9121)
* Document more restrictive configuration for creating Oracle LogMiner user  [DBZ-9129](https://issues.redhat.com/browse/DBZ-9129)
* Create rest resource for the connection [DBZ-9313](https://issues.redhat.com/browse/DBZ-9313)
* Improve the Github Action that checks the commit messages [DBZ-9327](https://issues.redhat.com/browse/DBZ-9327)
* Update assertj to 3.27.4 [DBZ-9343](https://issues.redhat.com/browse/DBZ-9343)
* Add, remove, or convert Dev Preview and Tech Preview notes [DBZ-9347](https://issues.redhat.com/browse/DBZ-9347)
* Add documentation for the LogMiner unbuffered mode [DBZ-9351](https://issues.redhat.com/browse/DBZ-9351)



## 3.3.0.Alpha1
August 5th 2025 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12464154)

### New features since 3.2.0.Final

* Execute Debezium in oracle readonly replica [DBZ-8319](https://issues.redhat.com/browse/DBZ-8319)
* Support for TSVECTOR data types for postgres source connector [DBZ-8470](https://issues.redhat.com/browse/DBZ-8470)
* Support MariaDB 11.7+ vector data type [DBZ-8582](https://issues.redhat.com/browse/DBZ-8582)
* Fine-grained logging configuration [DBZ-8638](https://issues.redhat.com/browse/DBZ-8638)
* During a mining session treat ORA-00310 redo logs being inconsistent [DBZ-8870](https://issues.redhat.com/browse/DBZ-8870)
* Smart editor: Add the ability to create a complete pipeline directly using the debezium server configuration [DBZ-8873](https://issues.redhat.com/browse/DBZ-8873)
* Smart editor:  Provide support for an auto conversion between formats [DBZ-8888](https://issues.redhat.com/browse/DBZ-8888)
* Add JMX metrics/statistics for cached events [DBZ-8991](https://issues.redhat.com/browse/DBZ-8991)
* Introduce a way to restore the legacy numeric behavior with decimal.handling.mode [DBZ-9166](https://issues.redhat.com/browse/DBZ-9166)
* Remove heartbeat creation from configuration in favor of HeartbeatFactory [DBZ-9176](https://issues.redhat.com/browse/DBZ-9176)
* Simplify dockerfile for debezium-connector-vitess itests [DBZ-9216](https://issues.redhat.com/browse/DBZ-9216)
* Update Outbox Extension to Quarkus 3.23.4 & Align to Hibernate 7.x [DBZ-9219](https://issues.redhat.com/browse/DBZ-9219)
* Add Lightbox on website for images [DBZ-9227](https://issues.redhat.com/browse/DBZ-9227)
* Azure Event Hub sink - built-in support for hashing partition keys [DBZ-9245](https://issues.redhat.com/browse/DBZ-9245)
* Throw an exception on missing heartbeat table on Debezium-connector-postgres [DBZ-9247](https://issues.redhat.com/browse/DBZ-9247)
* Add configuration to disable Context headers added with OpenLineage [DBZ-9248](https://issues.redhat.com/browse/DBZ-9248)
* Add ability to specify whether to use a CTE-based query for LogMiner [DBZ-9272](https://issues.redhat.com/browse/DBZ-9272)
* Allow Oracle heartbeat action query error handler to be resilient to ORA-02396 [DBZ-9280](https://issues.redhat.com/browse/DBZ-9280)
* Uppdate Informix JDBC Driver to 4.50.12 [DBZ-9288](https://issues.redhat.com/browse/DBZ-9288)
* Release CockroachDB connector [DBZ-9289](https://issues.redhat.com/browse/DBZ-9289)
* Chart versioning must be SemVer compliant [DBZ-8913](https://issues.redhat.com/browse/DBZ-8913)
* Add additional tag to images compliant with SemVer [DBZ-8914](https://issues.redhat.com/browse/DBZ-8914)
* Debezium Engine Quarkus Extension: Support Heartbeat Event Listener [DBZ-8960](https://issues.redhat.com/browse/DBZ-8960)
* Debezium Engine Quarkus Extension: custom deserializer [DBZ-8962](https://issues.redhat.com/browse/DBZ-8962)
* Debezium Engine Quarkus Extension: introduce PostProcessor handler [DBZ-8965](https://issues.redhat.com/browse/DBZ-8965)
* Debezium Engine Quarkus Extension: Add Support to Custom Converter  [DBZ-8966](https://issues.redhat.com/browse/DBZ-8966)
* Add EOS support into main Debezium connectors [DBZ-9177](https://issues.redhat.com/browse/DBZ-9177)
* Avoid storing irrelevant DDL statements in history topic [DBZ-9186](https://issues.redhat.com/browse/DBZ-9186)


### Breaking changes since 3.2.0.Final

* Remove deprecated snapshot mode [DBZ-8171](https://issues.redhat.com/browse/DBZ-8171)


### Fixes since 3.2.0.Final

* User selection is not persistence in pipeline designer [DBZ-8761](https://issues.redhat.com/browse/DBZ-8761)
* MongoDB example image not working [DBZ-9061](https://issues.redhat.com/browse/DBZ-9061)
* '||' in ORACLE NVARCHAR data will cause exception [DBZ-9132](https://issues.redhat.com/browse/DBZ-9132)
* [ORACLE] DDL parsing failed [DBZ-9172](https://issues.redhat.com/browse/DBZ-9172)
* When using non-recovery snapshot modes, offsets are not reset [DBZ-9208](https://issues.redhat.com/browse/DBZ-9208)
* Validation for Log Position for SqlServer can fail [DBZ-9212](https://issues.redhat.com/browse/DBZ-9212)
* Possible regression with throwing DebeziumException rather than warning [DBZ-9217](https://issues.redhat.com/browse/DBZ-9217)
* Double event publishing via NATS Jetstream sink [DBZ-9221](https://issues.redhat.com/browse/DBZ-9221)
* Debezium examples github actions fails due to outdated action version [DBZ-9222](https://issues.redhat.com/browse/DBZ-9222)
* NullPointerException is thrown because DebeziumHeaderProducer is not registered [DBZ-9225](https://issues.redhat.com/browse/DBZ-9225)
* MongoDB ExtractNewDocumentState SMT crash with nested struct in array in 3.2 [DBZ-9231](https://issues.redhat.com/browse/DBZ-9231)
* Mongodb incremental snapshot is not honoring additional conditions [DBZ-9232](https://issues.redhat.com/browse/DBZ-9232)
* WithClause inside an INSERT statement throws DDL parser exception [DBZ-9233](https://issues.redhat.com/browse/DBZ-9233)
* Oracle snapshot boundary mode does not have a field display name [DBZ-9236](https://issues.redhat.com/browse/DBZ-9236)
* Request fix for muti-task CREATE TABLE collisions for jdbc postgres target causing task to crash [DBZ-9237](https://issues.redhat.com/browse/DBZ-9237)
* Oracle split table partition does not support online mode [DBZ-9238](https://issues.redhat.com/browse/DBZ-9238)
* Exceptionally large mining windows can lead unintended metrics/performance issues [DBZ-9241](https://issues.redhat.com/browse/DBZ-9241)
* zstd-jni should not be included in connector package [DBZ-9273](https://issues.redhat.com/browse/DBZ-9273)
* OpenLineage output dataset uses the wrong datatype [DBZ-9285](https://issues.redhat.com/browse/DBZ-9285)
* Debezium platform verify signal data collection fails [DBZ-9290](https://issues.redhat.com/browse/DBZ-9290)
* Unchecked exception from OffsetStorageWriter.doFlush() in AsyncEmbeddedEngine leaves semaphore in OffsetStorageWriter unreleased and probably causes engine to fail [DBZ-9292](https://issues.redhat.com/browse/DBZ-9292)
* Reselect post processor does not work with VariableScaleDecimal primary keys [DBZ-9293](https://issues.redhat.com/browse/DBZ-9293)
* Duplicate key exception when using postgres connector based on pgoutput plugin [DBZ-9305](https://issues.redhat.com/browse/DBZ-9305)


### Other changes since 3.2.0.Final

* Document Debezium operator installation and usage [DBZ-8440](https://issues.redhat.com/browse/DBZ-8440)
* Create a showcase example for Openlineage [DBZ-9058](https://issues.redhat.com/browse/DBZ-9058)
* Update the tutorial [DBZ-9187](https://issues.redhat.com/browse/DBZ-9187)
* Update debezium examples [DBZ-9188](https://issues.redhat.com/browse/DBZ-9188)
* Document exactly once delivery [DBZ-9230](https://issues.redhat.com/browse/DBZ-9230)
* Include the raw SinkRecord in the logged output of a JDBC sink record [DBZ-9239](https://issues.redhat.com/browse/DBZ-9239)
* Switch to smallrye jandex maven plugin [DBZ-9246](https://issues.redhat.com/browse/DBZ-9246)
* Document JMX setup for Debezium Server [DBZ-9282](https://issues.redhat.com/browse/DBZ-9282)
* Remove hard line breaks from MariaDB and MySQL properties lists [DBZ-9286](https://issues.redhat.com/browse/DBZ-9286)
* Support configuring Mockito java agent in java 21+ [DBZ-9296](https://issues.redhat.com/browse/DBZ-9296)
* Log all rows from LogMiner session logs during LogMiner failures  [DBZ-9322](https://issues.redhat.com/browse/DBZ-9322)



## 3.2.0.Final
July 9th 2025 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12463706)

### New features since 3.2.0.CR1

* Add SnapshotSkipped status to metrics [DBZ-8610](https://issues.redhat.com/browse/DBZ-8610)
* Introduce publish.via.partition.root flag in PostgresCDC Connector [DBZ-9158](https://issues.redhat.com/browse/DBZ-9158)
* Support dynamic message key in Event Hub Sink [DBZ-9195](https://issues.redhat.com/browse/DBZ-9195)
* Debezium Engine Quarkus Extension: expose Debezium Notification Events [DBZ-8964](https://issues.redhat.com/browse/DBZ-8964)
* Provide a dedicate module for OpenLineage integration [DBZ-9110](https://issues.redhat.com/browse/DBZ-9110)
* Add support for headers in NATS Jetstream sink [DBZ-9171](https://issues.redhat.com/browse/DBZ-9171)


### Breaking changes since 3.2.0.CR1

* Ibmi connector default to avro for SchemaNameAdjuster [DBZ-9183](https://issues.redhat.com/browse/DBZ-9183)


### Fixes since 3.2.0.CR1

* Events inserted during snapshot are being duplicated [DBZ-9006](https://issues.redhat.com/browse/DBZ-9006)
* DmlParserException: DML statement couldn't be parsed [DBZ-9191](https://issues.redhat.com/browse/DBZ-9191)
* OpenLineage - Postgres connector emits Lineage Event with incorrect dataset name [DBZ-9192](https://issues.redhat.com/browse/DBZ-9192)
* Data structures for communicating to DB2 are static [DBZ-9196](https://issues.redhat.com/browse/DBZ-9196)
* Add filtering for journal types and codes [DBZ-9197](https://issues.redhat.com/browse/DBZ-9197)
* Oracle connector crashes on redo entry without SQL_REDO for temporary tables [DBZ-9199](https://issues.redhat.com/browse/DBZ-9199)
* Fix test and noise in Vitess Connector [DBZ-9207](https://issues.redhat.com/browse/DBZ-9207)


### Other changes since 3.2.0.CR1

* Debezium Engine Quarkus Extension: Use Quarkus-style Configuration Properties [DBZ-8957](https://issues.redhat.com/browse/DBZ-8957)
* ORA-00600: possible solutions to be added to the documentation FAQ [DBZ-9155](https://issues.redhat.com/browse/DBZ-9155)
* Debezium uses deprecated JdbcConnection#quotedColumnIdString() [DBZ-9169](https://issues.redhat.com/browse/DBZ-9169)
* Add documentation on running integration tests on Apple Silicon [DBZ-9173](https://issues.redhat.com/browse/DBZ-9173)
* Upgrade PostgreSQL JDBC driver to 42.7.7 [DBZ-9200](https://issues.redhat.com/browse/DBZ-9200)
* Fix OpenLineage tests [DBZ-9201](https://issues.redhat.com/browse/DBZ-9201)
* Misleading wording in the WalPositionLocator JavaDoc [DBZ-9203](https://issues.redhat.com/browse/DBZ-9203)



## 3.2.0.CR1
June 25th 2025 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12462697)

### New features since 3.2.0.Beta2

* PubSub Sink default maxBufferBytes allows the request to exceed the limit [DBZ-9144](https://issues.redhat.com/browse/DBZ-9144)
* Remove nominalTime facet from OpenLineage events [DBZ-9146](https://issues.redhat.com/browse/DBZ-9146)
* Debezium Engine Quarkus Extension: Introduce Lifecycle Annotations for Debezium Engine [DBZ-8959](https://issues.redhat.com/browse/DBZ-8959)


### Breaking changes since 3.2.0.Beta2

* Fix warn log if snapshot mode when needed is required but is not set [DBZ-9118](https://issues.redhat.com/browse/DBZ-9118)


### Fixes since 3.2.0.Beta2

* Full screen button on the pipeline log page is not working [DBZ-9105](https://issues.redhat.com/browse/DBZ-9105)
* SQL Server connector doesn't properly handle special characters in schema names [DBZ-9117](https://issues.redhat.com/browse/DBZ-9117)
* The presence of "_" in the ORACLE table name caused the cdc to fail [DBZ-9131](https://issues.redhat.com/browse/DBZ-9131)
* JDBC Connector is lost when filter.include.list is used [DBZ-9141](https://issues.redhat.com/browse/DBZ-9141)
* Typo in the example of the Custom Converter [DBZ-9153](https://issues.redhat.com/browse/DBZ-9153)
* Postgres: log errors from keepalive thread for replication [DBZ-9161](https://issues.redhat.com/browse/DBZ-9161)
* Fix truncate present in history topic if it is a skipped operation [DBZ-9162](https://issues.redhat.com/browse/DBZ-9162)
* Cannot use idenitifier named LOCKED [DBZ-9163](https://issues.redhat.com/browse/DBZ-9163)
* MySQL connector doesn't properly handle special characters in database object names [DBZ-9168](https://issues.redhat.com/browse/DBZ-9168)
* Align configuration for post processors to transforms, predicates [DBZ-9170](https://issues.redhat.com/browse/DBZ-9170)


### Other changes since 3.2.0.Beta2

* Upgrade postgresql driver from 42.6.0 to 42.7.2 [DBZ-7533](https://issues.redhat.com/browse/DBZ-7533)
* Fix some problems experienced when running with Postgres JDBC driver 42.7.5. [DBZ-9018](https://issues.redhat.com/browse/DBZ-9018)
* Fix test shouldRegularlyFlushLsnWithTxMonitoring [DBZ-9125](https://issues.redhat.com/browse/DBZ-9125)
* Implement caching in table inclusion filter [DBZ-9128](https://issues.redhat.com/browse/DBZ-9128)
* Prevent multiple initializations of DataTypeResolver [DBZ-9143](https://issues.redhat.com/browse/DBZ-9143)



## 3.2.0.Beta2
June 9th 2025 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12462099)

### New features since 3.2.0.Beta1

* Move schema history recovery out of task start method [DBZ-8562](https://issues.redhat.com/browse/DBZ-8562)
* OpenLineage integration [DBZ-9020](https://issues.redhat.com/browse/DBZ-9020)
* Reduce calls to getEndOffset during kafka schema history recovery [DBZ-9098](https://issues.redhat.com/browse/DBZ-9098)


### Breaking changes since 3.2.0.Beta1

None


### Fixes since 3.2.0.Beta1

* Error validating connector with special character $ [DBZ-8660](https://issues.redhat.com/browse/DBZ-8660)
* Error when converting table and column names to uppercase [DBZ-9017](https://issues.redhat.com/browse/DBZ-9017)
* Oracle LogMiner mistakenly emits rollback transactions in commit data only mode [DBZ-9074](https://issues.redhat.com/browse/DBZ-9074)
* UI is breaking on the pipeline details page. [DBZ-9084](https://issues.redhat.com/browse/DBZ-9084)
* SQL Server connector doesn't properly handle special characters in database object names [DBZ-9091](https://issues.redhat.com/browse/DBZ-9091)
* Column name encrypted is not supported by MySqlParser [DBZ-9092](https://issues.redhat.com/browse/DBZ-9092)
* Typo in the registry link for amq streams kafka container  [DBZ-9094](https://issues.redhat.com/browse/DBZ-9094)
* Removal of REST extension left service loader definition in Oracle connector [DBZ-9101](https://issues.redhat.com/browse/DBZ-9101)


### Other changes since 3.2.0.Beta1

* Support signals in UI frontend [DBZ-8422](https://issues.redhat.com/browse/DBZ-8422)
* Replace Postgres txid_current() by pg_current_xact_id() [DBZ-9011](https://issues.redhat.com/browse/DBZ-9011)
* Create documentation for embeddings SMT [DBZ-9053](https://issues.redhat.com/browse/DBZ-9053)
* Update Docker Build and CI issues [DBZ-9076](https://issues.redhat.com/browse/DBZ-9076)
* Lots of time spent in parsing column type modifiers [DBZ-9093](https://issues.redhat.com/browse/DBZ-9093)
* Bump Chicory to 1.4.0 [DBZ-9100](https://issues.redhat.com/browse/DBZ-9100)
* Improve the messaging in the "Table is not a relational table" logged exception [DBZ-9111](https://issues.redhat.com/browse/DBZ-9111)



## 3.2.0.Beta1
May 29th 2025 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12455584)

### New features since 3.2.0.Alpha1

* Qdrant sink in Debezium Server [DBZ-8635](https://issues.redhat.com/browse/DBZ-8635)
* Oracle username returns unknown when transaction mined in two steps [DBZ-8884](https://issues.redhat.com/browse/DBZ-8884)
* Include JSON source when throwing deserialization error with incremental snapshots [DBZ-8974](https://issues.redhat.com/browse/DBZ-8974)
* Stop forced flushing with reduction buffer in JDBC Sink Connector [DBZ-8982](https://issues.redhat.com/browse/DBZ-8982)
* Add validation for signal.data.collection [DBZ-9001](https://issues.redhat.com/browse/DBZ-9001)
* Introduce connection validation timeout [DBZ-9004](https://issues.redhat.com/browse/DBZ-9004)
* Improve failed connection logging [DBZ-9008](https://issues.redhat.com/browse/DBZ-9008)
* Allow custom load balancing policy [DBZ-9014](https://issues.redhat.com/browse/DBZ-9014)
* Add logic to `ExtractNewRecordState` to convert all deletes to tombstone records [DBZ-9022](https://issues.redhat.com/browse/DBZ-9022)
* Add option to enable hostname verification for Redis sink [DBZ-9042](https://issues.redhat.com/browse/DBZ-9042)
* Add basic notification support [DBZ-9046](https://issues.redhat.com/browse/DBZ-9046)
* Support for Informix 15 [DBZ-9049](https://issues.redhat.com/browse/DBZ-9049)
* Implement support for stopLoggingOnClose [DBZ-9050](https://issues.redhat.com/browse/DBZ-9050)
* Uppdate Informix JDBC Driver to 4.50.11.2 [DBZ-9072](https://issues.redhat.com/browse/DBZ-9072)
* Implement heartbeat.action.query [DBZ-9081](https://issues.redhat.com/browse/DBZ-9081)
* Add option to specify custom keystore and truststore for Redis sink [DBZ-9082](https://issues.redhat.com/browse/DBZ-9082)


### Breaking changes since 3.2.0.Alpha1

* Optimize ExtractNewRecordState "delete.handling.mode" and "drop.tombstones" configuration [DBZ-6068](https://issues.redhat.com/browse/DBZ-6068)
* Migrate to new Maven Central [DBZ-9025](https://issues.redhat.com/browse/DBZ-9025)
* Remove embeddings prefix from embeddgins SMT configuration [DBZ-9056](https://issues.redhat.com/browse/DBZ-9056)
* Exclude TRUNCATE and REPLACE statements from schema history [DBZ-9085](https://issues.redhat.com/browse/DBZ-9085)


### Fixes since 3.2.0.Alpha1

* Ingestion issues with Mongodb when empty [] or empty {} appear in the Json feed [DBZ-5920](https://issues.redhat.com/browse/DBZ-5920)
* Incremental snapshot in-progress notification doesn't contain full composite PK [DBZ-8207](https://issues.redhat.com/browse/DBZ-8207)
* Connector errors.max.retries is ignored [DBZ-8711](https://issues.redhat.com/browse/DBZ-8711)
* Oracle log consistency check always fails after database refresh with residual archive logs [DBZ-8744](https://issues.redhat.com/browse/DBZ-8744)
* Debezium*ConnectorResourceIT are skipped [DBZ-8777](https://issues.redhat.com/browse/DBZ-8777)
* DDL statement couldn't be parsed. PAGE_COMPRESSED [DBZ-8916](https://issues.redhat.com/browse/DBZ-8916)
* Charts release pipeline must correctly honor the dry-run option [DBZ-8955](https://issues.redhat.com/browse/DBZ-8955)
* debezium/server mongodb org.apache.kafka.connect.errors.DataException: is not a valid field name [DBZ-8972](https://issues.redhat.com/browse/DBZ-8972)
* NatsJetStreamIT fails [DBZ-8985](https://issues.redhat.com/browse/DBZ-8985)
* Make StreamingChangeEventSource closeable [DBZ-8995](https://issues.redhat.com/browse/DBZ-8995)
* DDL is logged with sensitive information in AbstractSchemaHistory [DBZ-8999](https://issues.redhat.com/browse/DBZ-8999)
* The in-progress notification is sent before the snapshot job starts [DBZ-9002](https://issues.redhat.com/browse/DBZ-9002)
* Connection left in "idle in transaction" state when setting snapshot mode to initial only [DBZ-9003](https://issues.redhat.com/browse/DBZ-9003)
* DB2 for Z/OS Fixes [DBZ-9007](https://issues.redhat.com/browse/DBZ-9007)
* While the low watermark scn updates across iterations, it is never flushed to the offsets. [DBZ-9013](https://issues.redhat.com/browse/DBZ-9013)
* IBMi connector is not included in Debezium Server [DBZ-9015](https://issues.redhat.com/browse/DBZ-9015)
* PostgresSQL Read-only incremental snapshot continue to read chunks even with completed snapshot [DBZ-9016](https://issues.redhat.com/browse/DBZ-9016)
* Oracle database PDB name in lowercase is not connecting to the connector. [DBZ-9019](https://issues.redhat.com/browse/DBZ-9019)
* Error parsing MariaDB DDL [DBZ-9027](https://issues.redhat.com/browse/DBZ-9027)
* LogMiner performance regression with buffered implementation [DBZ-9030](https://issues.redhat.com/browse/DBZ-9030)
* Column named SEQUENCE, a MySQL keyword fails to be parsed [DBZ-9031](https://issues.redhat.com/browse/DBZ-9031)
* MySQL parser fails when using a JSON_TABLE in a join clause [DBZ-9034](https://issues.redhat.com/browse/DBZ-9034)
* Default values may be misinterpreted as bind parameters due to nested quotes [DBZ-9040](https://issues.redhat.com/browse/DBZ-9040)
* Missing configuration properties for signal channel readers can lead to NullPointerException [DBZ-9052](https://issues.redhat.com/browse/DBZ-9052)
* Blocking snapshot does not always resume streaming thread when task is in shutdown [DBZ-9055](https://issues.redhat.com/browse/DBZ-9055)
* Field deprecatedAliases are nullified by other options [DBZ-9060](https://issues.redhat.com/browse/DBZ-9060)
* Oracle-specific lag metric is being updated based on empty transaction commits [DBZ-9062](https://issues.redhat.com/browse/DBZ-9062)
* EmbeddingsOllamaIT fails in CI [DBZ-9063](https://issues.redhat.com/browse/DBZ-9063)
* Field.withDeprecatedAliases() skips deprecatedFieldWarning validator [DBZ-9064](https://issues.redhat.com/browse/DBZ-9064)
* Error while parsing a MariaDB DDL [DBZ-9065](https://issues.redhat.com/browse/DBZ-9065)
* Field::deprecatedFieldWarning results into validation failure [DBZ-9066](https://issues.redhat.com/browse/DBZ-9066)
* Postgres Reselector fails on serial primary keys [DBZ-9086](https://issues.redhat.com/browse/DBZ-9086)


### Other changes since 3.2.0.Alpha1

* Remove skipping (de)serialization tests from the testsuite [DBZ-7356](https://issues.redhat.com/browse/DBZ-7356)
* Remove EmebeddedEngine [DBZ-8029](https://issues.redhat.com/browse/DBZ-8029)
* Document Oracle mTLS connection configuration [DBZ-8159](https://issues.redhat.com/browse/DBZ-8159)
*  Add an examples in debezium-example repository with easy local deployment for Debezium-platfrom [DBZ-8664](https://issues.redhat.com/browse/DBZ-8664)
* [Conductor] Add endpoint to send signals  [DBZ-8942](https://issues.redhat.com/browse/DBZ-8942)
* Create Maven Modules for Debezium Engine Quarkus Extension [DBZ-8956](https://issues.redhat.com/browse/DBZ-8956)
* Debezium Engine Quarkus Extension: Introduce Debezium Capturing Listener [DBZ-8961](https://issues.redhat.com/browse/DBZ-8961)
* Add MariaDB example in tutorial [DBZ-8983](https://issues.redhat.com/browse/DBZ-8983)
* Unify behavior across LogMiner buffered/unbuffered implementations [DBZ-8986](https://issues.redhat.com/browse/DBZ-8986)
* Create emdeddings SMT extension for Hugging face [DBZ-8992](https://issues.redhat.com/browse/DBZ-8992)
* Create emdeddings SMT extension for Voyage AI models [DBZ-8993](https://issues.redhat.com/browse/DBZ-8993)
* Enable Dependabot on debezium operator repo [DBZ-9010](https://issues.redhat.com/browse/DBZ-9010)
* Prevent shared resource race condition in tests [DBZ-9029](https://issues.redhat.com/browse/DBZ-9029)
* Upgrade Outbox Extension to Quarkus 3.22.2 [DBZ-9033](https://issues.redhat.com/browse/DBZ-9033)
* Update Debezium base images to Fedora 41 [DBZ-9035](https://issues.redhat.com/browse/DBZ-9035)
* Upgrade Antora to 3.1.10 [DBZ-9036](https://issues.redhat.com/browse/DBZ-9036)
* Update to the latest version of Mockito 5.17.0 [DBZ-9037](https://issues.redhat.com/browse/DBZ-9037)
* Update to Infinispan 15.2.1.Final [DBZ-9038](https://issues.redhat.com/browse/DBZ-9038)
* Create a GitHub action/workflow to run some connectors with apicurio profile [DBZ-9043](https://issues.redhat.com/browse/DBZ-9043)
* Add SQLException to retriable exceptions [DBZ-9051](https://issues.redhat.com/browse/DBZ-9051)
* Remove duplicate version configuration for rest-assured in bom [DBZ-9069](https://issues.redhat.com/browse/DBZ-9069)
* move package from `io.quarkus.*` to `io.debezium.*` [DBZ-9075](https://issues.redhat.com/browse/DBZ-9075)
* Enforce CI to build all dependent modules [DBZ-9077](https://issues.redhat.com/browse/DBZ-9077)



## 3.2.0.Alpha1
April 29th 2025 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12449948)

### New features since 3.1.0.Final

* Pass connector configuration to Column/Table naming strategies [DBZ-7051](https://issues.redhat.com/browse/DBZ-7051)
* Support BOOLEAN [DBZ-7796](https://issues.redhat.com/browse/DBZ-7796)
* Add decimal handling mode support to IBMi connector [DBZ-8301](https://issues.redhat.com/browse/DBZ-8301)
* Option to add new Transform from the UI and ability to pass transform in Pipeline [DBZ-8328](https://issues.redhat.com/browse/DBZ-8328)
* Prevent write operations in PostgreSQL in read-only mode. [DBZ-8743](https://issues.redhat.com/browse/DBZ-8743)
* The method removeTransactionEventWithRowId creates high CPU load in certain scenarios [DBZ-8860](https://issues.redhat.com/browse/DBZ-8860)
* Log JMX MBean name when registration fails due to name conflict [DBZ-8862](https://issues.redhat.com/browse/DBZ-8862)
* HistorizedRelationalDatabaseConnectorConfig#getHistoryRecordComparator() should be public for external use. [DBZ-8868](https://issues.redhat.com/browse/DBZ-8868)
*  Improve MySQL/MariaDB connector resilience during post-schema recovery reconnect [DBZ-8877](https://issues.redhat.com/browse/DBZ-8877)
* Fix performance regression in debezium-core [DBZ-8879](https://issues.redhat.com/browse/DBZ-8879)
* Expose option to reset (streaming) metrics individually [DBZ-8885](https://issues.redhat.com/browse/DBZ-8885)
* Raise more meaningful exception in case of inconsistent post processor config [DBZ-8901](https://issues.redhat.com/browse/DBZ-8901)
* Allow filtering Oracle LogMiner results by client id [DBZ-8904](https://issues.redhat.com/browse/DBZ-8904)
* Allow timeout to be configured for Ollama embedding model [DBZ-8908](https://issues.redhat.com/browse/DBZ-8908)
* Allow unwinding of JSON datatype in Milvus sink [DBZ-8909](https://issues.redhat.com/browse/DBZ-8909)
* Add configuration to skip heartbeat messages in Redis Stream consumer [DBZ-8911](https://issues.redhat.com/browse/DBZ-8911)
* Implement LogMiner committed data only unbuffered adapter [DBZ-8924](https://issues.redhat.com/browse/DBZ-8924)
* Improve lookup performance for the Oracle ObjectId cache when using the Hybrid mining strategy [DBZ-8925](https://issues.redhat.com/browse/DBZ-8925)
* Exclude unknown tables when query filter is enabled and using a non-Hybrid strategy [DBZ-8926](https://issues.redhat.com/browse/DBZ-8926)
* ArrayIndexOutOfBoundsException in Cassandra Connector's FieldFilterSelector when parsing field exclude list [DBZ-8933](https://issues.redhat.com/browse/DBZ-8933)
* Improve log message when failing to apply a partial rollback [DBZ-8944](https://issues.redhat.com/browse/DBZ-8944)
* [Doc] Apicurio registry configuration should include instructions for confluent compatibility mode [DBZ-8945](https://issues.redhat.com/browse/DBZ-8945)
* passing topic name as well in error in case a single connector is configured with multiple topics  [DBZ-8946](https://issues.redhat.com/browse/DBZ-8946)
* add polling_tasks connector callback in asyncEmbeddedEngine [DBZ-8948](https://issues.redhat.com/browse/DBZ-8948)


### Breaking changes since 3.1.0.Final

* Document using TLS encryption of Oracle connectors using JKS instead of Oracle Wallet [DBZ-8788](https://issues.redhat.com/browse/DBZ-8788)
* Upgrade to Kafka 4.0.0 [DBZ-8875](https://issues.redhat.com/browse/DBZ-8875)
* When the journal receiver is deleted before debezium finishes processing it can timeout when it resets to the beginning [DBZ-8898](https://issues.redhat.com/browse/DBZ-8898)


### Fixes since 3.1.0.Final

* duplicate change events on ibmi connector [DBZ-8214](https://issues.redhat.com/browse/DBZ-8214)
* heartbeat.interval.ms not honored [DBZ-8551](https://issues.redhat.com/browse/DBZ-8551)
* Incorrect NumberOfEventsFiltered metrics in streaming [DBZ-8576](https://issues.redhat.com/browse/DBZ-8576)
* Signal table column names are arbitrary, but delete strategy expects column named id [DBZ-8723](https://issues.redhat.com/browse/DBZ-8723)
* DB2 Signaling creates watermarking in the wrong schema [DBZ-8833](https://issues.redhat.com/browse/DBZ-8833)
* Debezium Server keeps up after timeout on Pulsar and Postgres disconnection (Outbox Pattern) [DBZ-8843](https://issues.redhat.com/browse/DBZ-8843)
* When using the Oracle relaxed SQL parser setup, strings with apostrophe followed by comma are trimmed [DBZ-8869](https://issues.redhat.com/browse/DBZ-8869)
* Oracle Ehcache buffer will silently evict entries when configured size limits are reached [DBZ-8874](https://issues.redhat.com/browse/DBZ-8874)
* Transaction events are not removed when transaction event count over threshold  [DBZ-8880](https://issues.redhat.com/browse/DBZ-8880)
* InstructLabIT can randomly fail due to file read/write race condition between threads [DBZ-8883](https://issues.redhat.com/browse/DBZ-8883)
* Setting Oracle buffer type to an unsupported/invalid value is not validated properly [DBZ-8886](https://issues.redhat.com/browse/DBZ-8886)
* Oracle timestamp columns are ignored when temporal mode set to ISOSTRING [DBZ-8889](https://issues.redhat.com/browse/DBZ-8889)
* Kinesis Connector does not send failed records during retry, it sends records in original batch [DBZ-8893](https://issues.redhat.com/browse/DBZ-8893)
* DDL parsing fails on "BY USER FOR STATISTICS" virtual column clause [DBZ-8895](https://issues.redhat.com/browse/DBZ-8895)
* Postgres CapturedTables metric isn't populated. [DBZ-8897](https://issues.redhat.com/browse/DBZ-8897)
* FieldToEmbedding SMT fails with NPE for delete records [DBZ-8907](https://issues.redhat.com/browse/DBZ-8907)
* FieldToEmbedding SMT crashes when source field name is substring of embedding name [DBZ-8910](https://issues.redhat.com/browse/DBZ-8910)
* Setting continuous mining for Oracle 18 or later causes NPE [DBZ-8919](https://issues.redhat.com/browse/DBZ-8919)
* Improve performance by removing unnecessary filter check [DBZ-8921](https://issues.redhat.com/browse/DBZ-8921)
* NullPointerException happens when a transaction commits that is unknown to the connector [DBZ-8929](https://issues.redhat.com/browse/DBZ-8929)
* Async engine doesn't termiate gracefully upon StopEngineException [DBZ-8936](https://issues.redhat.com/browse/DBZ-8936)
* Processing error because of incomplete date part of DATETIME datatype in MariaDB [DBZ-8940](https://issues.redhat.com/browse/DBZ-8940)
* ORA-08186 invalid timestamp specified occurs when connector is started [DBZ-8943](https://issues.redhat.com/browse/DBZ-8943)
* GracefulRestartIT fails after Kafka upgrade [DBZ-8947](https://issues.redhat.com/browse/DBZ-8947)
* Unable to delete DS resource after a pipeline has been delete [DBZ-8970](https://issues.redhat.com/browse/DBZ-8970)
* Multiple Predicates Don't Function with the Operator API [DBZ-8975](https://issues.redhat.com/browse/DBZ-8975)


### Other changes since 3.1.0.Final

* Document Debezium Storage modules [DBZ-6532](https://issues.redhat.com/browse/DBZ-6532)
* Review EmbeddedEngine tests [DBZ-8442](https://issues.redhat.com/browse/DBZ-8442)
* Remove all the EmbeddedEngine remnants from the codebase [DBZ-8443](https://issues.redhat.com/browse/DBZ-8443)
* Migrate performance microbenchmarks to async engine [DBZ-8444](https://issues.redhat.com/browse/DBZ-8444)
* Update test suite to support ISOSTRING temporal precision mode [DBZ-8574](https://issues.redhat.com/browse/DBZ-8574)
* Upgrade MariaDB driver to 3.5.3 [DBZ-8758](https://issues.redhat.com/browse/DBZ-8758)
* Switch default builder facotry to async builder factory [DBZ-8779](https://issues.redhat.com/browse/DBZ-8779)
* Update SQL Server doc to correct schema history MBean name  [DBZ-8840](https://issues.redhat.com/browse/DBZ-8840)
* Expose Oracle connector XStreams content in product edition [DBZ-8841](https://issues.redhat.com/browse/DBZ-8841)
* Get rid of lombok from Debezium Platform/Operator [DBZ-8857](https://issues.redhat.com/browse/DBZ-8857)
* Add Localization support to UI [DBZ-8859](https://issues.redhat.com/browse/DBZ-8859)
* Upgrade RocketMQ version from 5.1.4 to 5.2.0 [DBZ-8864](https://issues.redhat.com/browse/DBZ-8864)
* Bump Chicory version and take advantage of latest improvements [DBZ-8867](https://issues.redhat.com/browse/DBZ-8867)
* Chart release pipeline doesn't need to be run on release node [DBZ-8878](https://issues.redhat.com/browse/DBZ-8878)
* Prefix Oracle Infinispan buffer profiles with "oracle-" [DBZ-8882](https://issues.redhat.com/browse/DBZ-8882)
* Add scripts/procedures to creating Oracle images [DBZ-8896](https://issues.redhat.com/browse/DBZ-8896)
* Update Outbox Extension Quarkus version to 3.21.2 [DBZ-8905](https://issues.redhat.com/browse/DBZ-8905)
* Update to latest LTS of Quarkus 3.15.4 [DBZ-8906](https://issues.redhat.com/browse/DBZ-8906)
* Add MariaDB download link to Installation Guide [DBZ-8927](https://issues.redhat.com/browse/DBZ-8927)
* DebeziumServerPostgresIT shouldSnapshot randomly fails [DBZ-8928](https://issues.redhat.com/browse/DBZ-8928)
* Remove unncessary metadata query and map fetch calls [DBZ-8938](https://issues.redhat.com/browse/DBZ-8938)
* [Conductor] Add endpoint to verify correct setup of signal data collection  [DBZ-8941](https://issues.redhat.com/browse/DBZ-8941)
* Remove the Cassandra from the source  [DBZ-8952](https://issues.redhat.com/browse/DBZ-8952)
* Turn off opentelemetry logging in the tests [DBZ-8971](https://issues.redhat.com/browse/DBZ-8971)



## 3.1.0.Final
April 2nd 2025 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12448671)

### New features since 3.1.0.CR1

* Add incremental snapshot configuration to the Debezium UI [DBZ-5326](https://issues.redhat.com/browse/DBZ-5326)
* [DOC] Documentation for using the JDBC Sink Connector in an OpenShift environment [DBZ-8549](https://issues.redhat.com/browse/DBZ-8549)
* Milvus sink in Debezium Server [DBZ-8634](https://issues.redhat.com/browse/DBZ-8634)
* InstructLab sink [DBZ-8637](https://issues.redhat.com/browse/DBZ-8637)
* Implement SMT for adding embeddings to the records [DBZ-8702](https://issues.redhat.com/browse/DBZ-8702)
* Improve debezium-vitess-connector enqueue speed [DBZ-8757](https://issues.redhat.com/browse/DBZ-8757)
* Doc should mention the the correlation ID does not map 1:1 with the original signal when multiple signals are sent. [DBZ-8817](https://issues.redhat.com/browse/DBZ-8817)
* Create a dedicated section in the doc for Debezium Server and related components [DBZ-8846](https://issues.redhat.com/browse/DBZ-8846)
* ExtractChangedRecordState should always add configured headers, even if field list is empty [DBZ-8855](https://issues.redhat.com/browse/DBZ-8855)
* Use the clustered index in sqlserver connector queries  [DBZ-8858](https://issues.redhat.com/browse/DBZ-8858)
* All queries issued by Debezium should be marked with workload tag [DBZ-8861](https://issues.redhat.com/browse/DBZ-8861)


### Breaking changes since 3.1.0.CR1

* Align JDBC storage configuration naming [DBZ-8573](https://issues.redhat.com/browse/DBZ-8573)


### Fixes since 3.1.0.CR1

* Negative binlog position values for MariaDB [DBZ-8755](https://issues.redhat.com/browse/DBZ-8755)
* Support CURDATE and CURTIME functions for MySQL DDL [DBZ-8834](https://issues.redhat.com/browse/DBZ-8834)
* ibmi connector leaks jobs when interrupted [DBZ-8839](https://issues.redhat.com/browse/DBZ-8839)
* In-progress transaction over boundary detected but skipped during streaming [DBZ-8844](https://issues.redhat.com/browse/DBZ-8844)
* Unparseable statements : Default column values can be scalar functions [DBZ-8849](https://issues.redhat.com/browse/DBZ-8849)
* AsyncEmbeddedEngine treats millisecond commit timeout as microseconds mistakenly [DBZ-8856](https://issues.redhat.com/browse/DBZ-8856)
* Transitive filtering of debezium-connector-dse excludes snakeyaml from /lib/ [DBZ-8863](https://issues.redhat.com/browse/DBZ-8863)


### Other changes since 3.1.0.CR1

* Implement the storybook for UI component [DBZ-6868](https://issues.redhat.com/browse/DBZ-6868)
* Document Debezium Platform  [DBZ-8827](https://issues.redhat.com/browse/DBZ-8827)
* Conditionalize note about Extended max string size in oracle.adoc that refers to Jira issue [DBZ-8838](https://issues.redhat.com/browse/DBZ-8838)
* cassandra4-connector: Update to cassandra 4.1 [DBZ-8842](https://issues.redhat.com/browse/DBZ-8842)
* Add missing oracle and mariadb source in UI [DBZ-8851](https://issues.redhat.com/browse/DBZ-8851)
* Change JDBC sink test default logging level to INFO [DBZ-8853](https://issues.redhat.com/browse/DBZ-8853)



## 3.1.0.CR1
March 24th 2025 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12447571)

### New features since 3.1.0.Beta1

* Centralise sensitive data logging using the Loggings Class [DBZ-8525](https://issues.redhat.com/browse/DBZ-8525)
* Reduce frequency table exists and column metadata queries [DBZ-8570](https://issues.redhat.com/browse/DBZ-8570)
* Unify helm charts [DBZ-8705](https://issues.redhat.com/browse/DBZ-8705)
* Implement ErrorHandler to throw RetriableException during SinkTask put operations [DBZ-8727](https://issues.redhat.com/browse/DBZ-8727)
* Add support for event key routing in RabbitMQ sink [DBZ-8752](https://issues.redhat.com/browse/DBZ-8752)
* Develop a Quarkus Application Example with Debezium Optimized for GraalVM [DBZ-8754](https://issues.redhat.com/browse/DBZ-8754)
* Support keyspace heartbeats feature [DBZ-8775](https://issues.redhat.com/browse/DBZ-8775)
* Improve Error Handling for Duplicate server_id / server_uuid in MySQL Connector [DBZ-8786](https://issues.redhat.com/browse/DBZ-8786)
* Handle BYTES as VARBINARY in SQLServer sink [DBZ-8790](https://issues.redhat.com/browse/DBZ-8790)
* Support string with temporal precision mode [DBZ-8826](https://issues.redhat.com/browse/DBZ-8826)


### Breaking changes since 3.1.0.Beta1

* Oracle connector remains waiting indefinitely for Logminer response upon starting new session [DBZ-8830](https://issues.redhat.com/browse/DBZ-8830)


### Fixes since 3.1.0.Beta1

* The first cdc message always lost when using debezium engine to capture oracle data [DBZ-8141](https://issues.redhat.com/browse/DBZ-8141)
* Signal Channel Kafka restart snapshot multiple snapshot after connector restart [DBZ-8780](https://issues.redhat.com/browse/DBZ-8780)
* Sources and home in debezium platform helm chart points to old repo [DBZ-8784](https://issues.redhat.com/browse/DBZ-8784)
* DebeziumServerPostgresIT randomly fails [DBZ-8821](https://issues.redhat.com/browse/DBZ-8821)
* Unexpected null value for Field Configuration deprecated aliases [DBZ-8832](https://issues.redhat.com/browse/DBZ-8832)


### Other changes since 3.1.0.Beta1

* Update format-maven-plugin to 2.26.0 [DBZ-8695](https://issues.redhat.com/browse/DBZ-8695)
* Centralize helm chart repo [DBZ-8707](https://issues.redhat.com/browse/DBZ-8707)
* OTEL libs are not loaded to Docker image [DBZ-8767](https://issues.redhat.com/browse/DBZ-8767)
* Change the documentation of minimum Java version requirement from 11 to 21 [DBZ-8771](https://issues.redhat.com/browse/DBZ-8771)
* Add delete.tombstone.handling.mode to ConfigDef returned by config method and change its display name [DBZ-8776](https://issues.redhat.com/browse/DBZ-8776)
* Update Debezium platform images in values.yaml [DBZ-8781](https://issues.redhat.com/browse/DBZ-8781)
* Allow Debezium server to use Kafka Connect format for the records [DBZ-8782](https://issues.redhat.com/browse/DBZ-8782)
* Write README for debezium-chart repo [DBZ-8785](https://issues.redhat.com/browse/DBZ-8785)
* Remove Helm from Debezium operator manifest README [DBZ-8791](https://issues.redhat.com/browse/DBZ-8791)
* Write blog post about the recent changes on charts.debezium.io [DBZ-8792](https://issues.redhat.com/browse/DBZ-8792)
* Test keyspace heartbeats during snapshot [DBZ-8824](https://issues.redhat.com/browse/DBZ-8824)
* Make methods for adding fields into the record reuseable [DBZ-8825](https://issues.redhat.com/browse/DBZ-8825)
* Enable build of debezium platform images  [DBZ-8829](https://issues.redhat.com/browse/DBZ-8829)



## 3.1.0.Beta1
March 11th 2025 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12446461)

### New features since 3.1.0.Alpha2

* A CloudEvent can contain `traceparent` field which value is obtained from header [DBZ-8669](https://issues.redhat.com/browse/DBZ-8669)
* Disable the delete option for the resources(source, destination, transform) that are in use in pipeline. [DBZ-8683](https://issues.redhat.com/browse/DBZ-8683)
* Differentiate between epoch & zero dates when columns it not nullable [DBZ-8689](https://issues.redhat.com/browse/DBZ-8689)
* Set string data type for medium/tiny/long text cols with binary collation [DBZ-8694](https://issues.redhat.com/browse/DBZ-8694)
* Dependencies in connect-base can be excluded in build time [DBZ-8709](https://issues.redhat.com/browse/DBZ-8709)
* Add concurrency and compression to pub/sub change consumer [DBZ-8715](https://issues.redhat.com/browse/DBZ-8715)
* Prevent table-level read locks when minimal locking is enabled [DBZ-8717](https://issues.redhat.com/browse/DBZ-8717)
* Refactor JdbcChangeEventSink execute method for buffer resolution [DBZ-8726](https://issues.redhat.com/browse/DBZ-8726)
* Add support for Google Cloud Pub/Sub locational endpoints [DBZ-8735](https://issues.redhat.com/browse/DBZ-8735)
* Wasm SMT effective access to schema fields [DBZ-8737](https://issues.redhat.com/browse/DBZ-8737)
* Oracle Connector: Additional timestamp fields from LogMiner (V$LOGMNR_CONTENTS) [DBZ-8740](https://issues.redhat.com/browse/DBZ-8740)
* Trim extra spaces in property keys when transforming to config [DBZ-8748](https://issues.redhat.com/browse/DBZ-8748)
* Add support for event key routing in RabbitMQ sink [DBZ-8752](https://issues.redhat.com/browse/DBZ-8752)


### Breaking changes since 3.1.0.Alpha2

None


### Fixes since 3.1.0.Alpha2

* SQL Server Connector cannot be upgraded to 2.0 [DBZ-5845](https://issues.redhat.com/browse/DBZ-5845)
* JDBC sink connector doesn't delete rows from a postgres db table [DBZ-8287](https://issues.redhat.com/browse/DBZ-8287)
* MariaDB adapter fails on an ALTER USER statement [DBZ-8436](https://issues.redhat.com/browse/DBZ-8436)
* Expressions cause SQL parser exception in Percona SEQUENCE_TABLE function [DBZ-8559](https://issues.redhat.com/browse/DBZ-8559)
* Slow Debezium startup for large number of tables [DBZ-8595](https://issues.redhat.com/browse/DBZ-8595)
* Debezium doesn't shut down correctly when encountering message delivery timeout from pub/sub [DBZ-8672](https://issues.redhat.com/browse/DBZ-8672)
* Broken pipe on streaming connection after blocking snapshot (Postgres) [DBZ-8680](https://issues.redhat.com/browse/DBZ-8680)
* ts_ms in source may default to 0 instead of Instant.now()  [DBZ-8708](https://issues.redhat.com/browse/DBZ-8708)
* PDB database name default considering as UPPERCASE [DBZ-8710](https://issues.redhat.com/browse/DBZ-8710)
* Alter table modify column fails when using DEFAULT ON NULL clause [DBZ-8720](https://issues.redhat.com/browse/DBZ-8720)
* ExtractChangedRecordState SMT Now Working With Default Values [DBZ-8721](https://issues.redhat.com/browse/DBZ-8721)
* Restart of Oracle RAC node leads to redo thread being inconsistent indefinitely [DBZ-8724](https://issues.redhat.com/browse/DBZ-8724)
* Specifying archive.log.hours with non-zero value generates bad SQL [DBZ-8725](https://issues.redhat.com/browse/DBZ-8725)
* debezium/connect docker image is not available on arm64 [DBZ-8728](https://issues.redhat.com/browse/DBZ-8728)
* Debezium Server: Nats consumer crashes with binary serialization [DBZ-8734](https://issues.redhat.com/browse/DBZ-8734)
* Possibly  broken schema.history.internal.skip.unparseable.ddl for MariaDB [DBZ-8745](https://issues.redhat.com/browse/DBZ-8745)
* Oracle snapshot's source.ts does not account for database zone differences [DBZ-8749](https://issues.redhat.com/browse/DBZ-8749)


### Other changes since 3.1.0.Alpha2

* Support debezium platform in the release pipeline [DBZ-8682](https://issues.redhat.com/browse/DBZ-8682)
* Create pipeline for package helm charts and publish on quay.io [DBZ-8706](https://issues.redhat.com/browse/DBZ-8706)
* Add more unit tests for deciding if the row was handled or not [DBZ-8716](https://issues.redhat.com/browse/DBZ-8716)
* Test debezium scale down reads offsets [DBZ-8719](https://issues.redhat.com/browse/DBZ-8719)
* Create an orchestrator pipeline to run the release [DBZ-8731](https://issues.redhat.com/browse/DBZ-8731)
* MySqlConnectorConvertingFailureIT.shouldRecoverToSyncSchemaWhenFailedValueConvertByDdlWithSqlLogBinIsOff fails randomly [DBZ-8736](https://issues.redhat.com/browse/DBZ-8736)
* Update the way tests calculates the default zoned times for MariaDB driver 3.5 [DBZ-8742](https://issues.redhat.com/browse/DBZ-8742)
* Bump assertj-core to 3.27.3 [DBZ-8751](https://issues.redhat.com/browse/DBZ-8751)



## 3.1.0.Alpha2
February 20th 2025 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12443609)

### New features since 3.1.0.Alpha1

* Error handling mode in ReselectColumnsPostProcessors [DBZ-8336](https://issues.redhat.com/browse/DBZ-8336)
* Provide full data types coverage for TinyGo Wasm SMT [DBZ-8586](https://issues.redhat.com/browse/DBZ-8586)
* Add predicates support in Transformation UI [DBZ-8590](https://issues.redhat.com/browse/DBZ-8590)
* Mention write permission on signaling data collection for incremental snapshot [DBZ-8596](https://issues.redhat.com/browse/DBZ-8596)
* Publish Debezium platform snapshot artifact [DBZ-8603](https://issues.redhat.com/browse/DBZ-8603)
* Include Current Archive log been processed in JMX metrics [DBZ-8644](https://issues.redhat.com/browse/DBZ-8644)
* All text cols with binary collation should still be output as strings [DBZ-8679](https://issues.redhat.com/browse/DBZ-8679)
* SQL Server - Errors related to schema validation should provide more details [DBZ-8692](https://issues.redhat.com/browse/DBZ-8692)


### Breaking changes since 3.1.0.Alpha1

* schema.history.internal.store.only.captured.databases.ddl not working properly [DBZ-8558](https://issues.redhat.com/browse/DBZ-8558)
* Data loss when primary key update is last operation in a transaction [DBZ-8594](https://issues.redhat.com/browse/DBZ-8594)
* Remove deprecated Oracle JMX metrics [DBZ-8647](https://issues.redhat.com/browse/DBZ-8647)
* Emit LOB columns when available even if lob.enabled is off [DBZ-8653](https://issues.redhat.com/browse/DBZ-8653)


### Fixes since 3.1.0.Alpha1

* Reduced record buffer doesn't handle RECORD_VALUE with primary key fields [DBZ-8593](https://issues.redhat.com/browse/DBZ-8593)
* Events for tables with generated columns fail when using hybrid mining strategy [DBZ-8597](https://issues.redhat.com/browse/DBZ-8597)
* ANTLR DDL Parsing error [DBZ-8600](https://issues.redhat.com/browse/DBZ-8600)
* MySQL master and replica images fail to start [DBZ-8633](https://issues.redhat.com/browse/DBZ-8633)
* Remove misleading log entry about undo change failure [DBZ-8645](https://issues.redhat.com/browse/DBZ-8645)
* Oracle metric OldestScnAgeInMilliseconds does not account for database timezone [DBZ-8646](https://issues.redhat.com/browse/DBZ-8646)
* Using RECORD_VALUE with a DELETE event causes NullPointerException [DBZ-8648](https://issues.redhat.com/browse/DBZ-8648)
* Downstream JDBC system tests fails [DBZ-8651](https://issues.redhat.com/browse/DBZ-8651)
* Batch size calculation is incorrectly using min-batch-size [DBZ-8652](https://issues.redhat.com/browse/DBZ-8652)
* Mysql example images for replication don't work [DBZ-8655](https://issues.redhat.com/browse/DBZ-8655)
* Oracle performance drop when transaction contains many constraint violations [DBZ-8665](https://issues.redhat.com/browse/DBZ-8665)
* Upstream system tests fail [DBZ-8678](https://issues.redhat.com/browse/DBZ-8678)
* Skip empty transactions with commit with redo thread equal to 0 [DBZ-8681](https://issues.redhat.com/browse/DBZ-8681)
* DDL statement couldn't be parsed: GRANT SENSITIVE_VARIABLES_OBSERVER [DBZ-8685](https://issues.redhat.com/browse/DBZ-8685)


### Other changes since 3.1.0.Alpha1

* Link old jdbc connector to new home and mark as retired [DBZ-8225](https://issues.redhat.com/browse/DBZ-8225)
* Align MySQL and MariaDB grammars with upstream versions [DBZ-8270](https://issues.redhat.com/browse/DBZ-8270)
* Add transformations and predicates support in conductor [DBZ-8459](https://issues.redhat.com/browse/DBZ-8459)
* Documentation version-picker redirects to overview page [DBZ-8483](https://issues.redhat.com/browse/DBZ-8483)
* Add note to connector docs to inform users about future removal of Containerfile deployment instructions  [DBZ-8566](https://issues.redhat.com/browse/DBZ-8566)
* HIghlight that Debezium containers are not production ready [DBZ-8580](https://issues.redhat.com/browse/DBZ-8580)
* Change schema history producer configurations [DBZ-8598](https://issues.redhat.com/browse/DBZ-8598)
* Update Debezium Server and Operator to Quarkus 3.15.3 LTS [DBZ-8601](https://issues.redhat.com/browse/DBZ-8601)
* Allow optional removal of ehcache form Oracle package [DBZ-8602](https://issues.redhat.com/browse/DBZ-8602)
* Change snapshot pipeline to publish conductor artifacts [DBZ-8604](https://issues.redhat.com/browse/DBZ-8604)
* Build conductor snapshot image [DBZ-8605](https://issues.redhat.com/browse/DBZ-8605)
* Build stage snapshot image [DBZ-8606](https://issues.redhat.com/browse/DBZ-8606)
* Publish stage/conductor nightly images to image registry [DBZ-8607](https://issues.redhat.com/browse/DBZ-8607)
* Upgrade AssertJ-DB [DBZ-8609](https://issues.redhat.com/browse/DBZ-8609)
* Prepare Logical message decoder SMT docs for productization [DBZ-8641](https://issues.redhat.com/browse/DBZ-8641)
* Remove push from github workflow in container-images repo [DBZ-8649](https://issues.redhat.com/browse/DBZ-8649)
* Update QOSDK to 6.9.3 [DBZ-8654](https://issues.redhat.com/browse/DBZ-8654)
* Document PostgreSQL snapshot.isolation.mode property [DBZ-8659](https://issues.redhat.com/browse/DBZ-8659)
* Integration tests should verify truncation of all data types [DBZ-8663](https://issues.redhat.com/browse/DBZ-8663)
* Upgrade protoc from 1.4 to 1.5 for postgres container images [DBZ-8670](https://issues.redhat.com/browse/DBZ-8670)
* Snapshot tests fails with Kafka 3.8.0  [DBZ-8688](https://issues.redhat.com/browse/DBZ-8688)
* Enable formatting checks on all project durinig PRs [DBZ-8698](https://issues.redhat.com/browse/DBZ-8698)
* Disable ARM images for PostgreSQL [DBZ-8713](https://issues.redhat.com/browse/DBZ-8713)



## 3.1.0.Alpha1
January 20th 2025 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12441653)

### New features since 3.0.7.Final

* Support new non adaptive temporal values  [DBZ-6387](https://issues.redhat.com/browse/DBZ-6387)
* Add MariaDB SSL support [DBZ-8482](https://issues.redhat.com/browse/DBZ-8482)
* Update the pipeline designer flow UI to remove the multi moving circle to just have one instead. [DBZ-8532](https://issues.redhat.com/browse/DBZ-8532)
* Sqlserver connector requires unbounded memory to process big transactions [DBZ-8557](https://issues.redhat.com/browse/DBZ-8557)
* Use enum set strings flag available in Vitess v20 for decoding enums/sets  [DBZ-8561](https://issues.redhat.com/browse/DBZ-8561)
* Pulsar Producer Batcher Builder - Key Based Batching [DBZ-8563](https://issues.redhat.com/browse/DBZ-8563)
* Prototype  support of WASM in Debezium transformation [DBZ-8568](https://issues.redhat.com/browse/DBZ-8568)
* S3 storage can force path-style addressing [DBZ-8569](https://issues.redhat.com/browse/DBZ-8569)
* Support MySQL and PostgreSQL vector data types [DBZ-8571](https://issues.redhat.com/browse/DBZ-8571)


### Breaking changes since 3.0.7.Final

* Make source info schema versioned [DBZ-8499](https://issues.redhat.com/browse/DBZ-8499)
* Rename SparseVector and move to debezium-core [DBZ-8585](https://issues.redhat.com/browse/DBZ-8585)


### Fixes since 3.0.7.Final

* Exception during commit offsets won't trigger retry logic. [DBZ-2386](https://issues.redhat.com/browse/DBZ-2386)
* Fix invalid gtid error on startup when ordered tx metadata enabled [DBZ-8541](https://issues.redhat.com/browse/DBZ-8541)
* Debezium operator generate wrong offset and schema history properties [DBZ-8543](https://issues.redhat.com/browse/DBZ-8543)
* A recent log switch may be seen as consistent during log gathering [DBZ-8546](https://issues.redhat.com/browse/DBZ-8546)
* Content-based routing expression variable headers is singular in code [DBZ-8550](https://issues.redhat.com/browse/DBZ-8550)
* MongoDataConverter does not recognize nested empty array [DBZ-8572](https://issues.redhat.com/browse/DBZ-8572)
* Fix issues in Transformation UI sections  [DBZ-8575](https://issues.redhat.com/browse/DBZ-8575)
* ORA-65040 occurs on log switches when log.mining.restart.connection is enabled and connection defaults to PDB rather than CDB$ROOT [DBZ-8577](https://issues.redhat.com/browse/DBZ-8577)


### Other changes since 3.0.7.Final

* Remove mongo-initiator images [DBZ-8487](https://issues.redhat.com/browse/DBZ-8487)
* Support storages supported by Debezium operator for pipeline in Debezium platform [DBZ-8512](https://issues.redhat.com/browse/DBZ-8512)
* Setup minimum CI pipeline for debezium-platform-conductor [DBZ-8527](https://issues.redhat.com/browse/DBZ-8527)
* Missing quick profile in test containers module [DBZ-8545](https://issues.redhat.com/browse/DBZ-8545)
* Upgrade MongoDB driver to 5.2 [DBZ-8554](https://issues.redhat.com/browse/DBZ-8554)
* Move to Quarkus 3.17.7 for the Outbox Extension [DBZ-8583](https://issues.redhat.com/browse/DBZ-8583)
* Use latest tag instead of nightly for conductor image [DBZ-8589](https://issues.redhat.com/browse/DBZ-8589)



## 3.0.6.Final
December 19th 2024 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12441350)

### New features since 3.0.5.Final

None


### Breaking changes since 3.0.5.Final

None


### Fixes since 3.0.5.Final

* Revert MySQL grammar changes [DBZ-8539](https://issues.redhat.com/browse/DBZ-8539)


### Other changes since 3.0.5.Final

None



## 3.0.5.Final
December 18th 2024 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12439150)

### New features since 3.0.4.Final

* List all examples in root README.md of Debezium's Example Repo [DBZ-2535](https://issues.redhat.com/browse/DBZ-2535)
* Test the MS SQL Server Plugin with Transparent data encryption (TDE) [DBZ-4590](https://issues.redhat.com/browse/DBZ-4590)
* Allow adhoc snapshot on tables whose schemas have not been captured [DBZ-4903](https://issues.redhat.com/browse/DBZ-4903)
* Support Postgres 17 failover slots [DBZ-8412](https://issues.redhat.com/browse/DBZ-8412)
* Improve error handling in dispatchSnapshotEvent of EventDispatcher [DBZ-8433](https://issues.redhat.com/browse/DBZ-8433)
* Connector configuration logging improvement [DBZ-8472](https://issues.redhat.com/browse/DBZ-8472)
* Handle un-parseable DDLs gracefully [DBZ-8479](https://issues.redhat.com/browse/DBZ-8479)
* Track LogMiner partial rollback events in metrics [DBZ-8491](https://issues.redhat.com/browse/DBZ-8491)
* Support JDBC offset/history configuration in CRD [DBZ-8501](https://issues.redhat.com/browse/DBZ-8501)


### Breaking changes since 3.0.4.Final

* KafkaSignalChannel reprocess signal after restart even when the snapshot has finished [DBZ-7856](https://issues.redhat.com/browse/DBZ-7856)
* Connector schema snapshot field inconsists with SnapshotRecord Enum definition [DBZ-8496](https://issues.redhat.com/browse/DBZ-8496)


### Fixes since 3.0.4.Final

* Error with debezium.sink.pulsar.client.serviceUrl and debezium-server [DBZ-3720](https://issues.redhat.com/browse/DBZ-3720)
* MySQL regression - Defaults store.only.captured.tables.ddl to true [DBZ-6709](https://issues.redhat.com/browse/DBZ-6709)
* ExtractNewRecordState value of optional null field which has default value [DBZ-7094](https://issues.redhat.com/browse/DBZ-7094)
* DebeziumException: No column '' where ' found in table [DBZ-8034](https://issues.redhat.com/browse/DBZ-8034)
* MySQL Connector Does Not Act On `CREATE DATABASE` Records In The Binlog [DBZ-8291](https://issues.redhat.com/browse/DBZ-8291)
* Vgtid doesn't contain multiple shard GTIDs when multiple tasks are used [DBZ-8432](https://issues.redhat.com/browse/DBZ-8432)
* Object ID cache may fail with concurent modification expcetion [DBZ-8465](https://issues.redhat.com/browse/DBZ-8465)
* Oracle gathers and logs object attributes for views unnecessarily [DBZ-8492](https://issues.redhat.com/browse/DBZ-8492)
* ReselectColumnPostProcessor can throw ORA-01003 "no statement parsed" when using fallback non-flashback area query [DBZ-8493](https://issues.redhat.com/browse/DBZ-8493)
* Oracle DDL ALTER TABLE ADD CONSTRAINT fails to be parsed [DBZ-8494](https://issues.redhat.com/browse/DBZ-8494)
* Edit Source/Destination on adding new configuration properties its removing old once   [DBZ-8495](https://issues.redhat.com/browse/DBZ-8495)
* Invalid property name in JDBC Schema History [DBZ-8500](https://issues.redhat.com/browse/DBZ-8500)
* Fix the URL in Pipeline log page  [DBZ-8502](https://issues.redhat.com/browse/DBZ-8502)
* Failed to start LogMiner mining session due to "Required Start SCN" error message [DBZ-8503](https://issues.redhat.com/browse/DBZ-8503)
* Oracle data pump TEMPLATE_TABLE clause not supported [DBZ-8504](https://issues.redhat.com/browse/DBZ-8504)
* Postgres alpine images require lang/llvm 19 for build [DBZ-8505](https://issues.redhat.com/browse/DBZ-8505)
* TimezoneConverter include.list should be respected if set [DBZ-8514](https://issues.redhat.com/browse/DBZ-8514)
* Missing log classes debezium-platform-conductor [DBZ-8515](https://issues.redhat.com/browse/DBZ-8515)
* Debezium Server fails to start when using the sink Kinesis [DBZ-8517](https://issues.redhat.com/browse/DBZ-8517)
* Skip GoldenGate REPLICATION MARKER events [DBZ-8533](https://issues.redhat.com/browse/DBZ-8533)


### Other changes since 3.0.4.Final

* Add example for SSL-enabled Kafka [DBZ-1937](https://issues.redhat.com/browse/DBZ-1937)
* Create smoke test to make sure Debezium Server container image works [DBZ-3226](https://issues.redhat.com/browse/DBZ-3226)
* Align MySQL and MariaDB grammars with upstream versions [DBZ-8270](https://issues.redhat.com/browse/DBZ-8270)
* Support MongoDB 8.0 [DBZ-8451](https://issues.redhat.com/browse/DBZ-8451)
* Update description of `message.key.columns` and format admonitions in PG doc [DBZ-8455](https://issues.redhat.com/browse/DBZ-8455)
* Add Basic validation in UI to check for form completion before submitting. [DBZ-8474](https://issues.redhat.com/browse/DBZ-8474)
* Use schema evolution tool to manage the conductor database [DBZ-8486](https://issues.redhat.com/browse/DBZ-8486)
* Update Quarkus Outbox Extension to Quarkus 3.17.3 [DBZ-8506](https://issues.redhat.com/browse/DBZ-8506)
* Merge conductor and stage into single platform repository [DBZ-8508](https://issues.redhat.com/browse/DBZ-8508)
* Container Tests are executed with -DskipITs [DBZ-8509](https://issues.redhat.com/browse/DBZ-8509)
* Add github workflow for UI unit testing on PRs [DBZ-8526](https://issues.redhat.com/browse/DBZ-8526)



[[release-3.0.5-final]]
== *Release 3.0.5.Final* _(December 18th 2024)_

See the https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12439150[complete list of issues].

=== Kafka compatibility

This release has been built against Kafka Connect 3.9.0 and has been tested with version 3.9.0 of the Kafka brokers.
See the https://kafka.apache.org/documentation/#upgrade[Kafka documentation] for compatibility with other versions of Kafka brokers.


=== Upgrading

Before upgrading any connector, be sure to check the backward-incompatible changes that have been made since the release you were using.

When you decide to upgrade one of these connectors to 3.0.5.Final from any earlier versions,
first check the migration notes for the version you're using.
Gracefully stop the running connector, remove the old plugin files, install the 3.0.5.Final plugin files, and restart the connector using the same configuration.
Upon restart, the 3.0.5.Final connectors will continue where the previous connector left off.
As one might expect, all change events previously written to Kafka by the old connector will not be modified.

If you are using our container images, then please do not forget to pull them fresh from https://quay.io/organization/debezium[Quay.io].


=== Breaking changes

[Placeholder for Breaking changes text] (https://issues.redhat.com/browse/DBZ-7856[DBZ-7856]).

[Placeholder for Breaking changes text] (https://issues.redhat.com/browse/DBZ-8496[DBZ-8496]).



=== New features

* List all examples in root README.md of Debezium's Example Repo https://issues.redhat.com/browse/DBZ-2535[DBZ-2535]
* Test the MS SQL Server Plugin with Transparent data encryption (TDE) https://issues.redhat.com/browse/DBZ-4590[DBZ-4590]
* Allow adhoc snapshot on tables whose schemas have not been captured https://issues.redhat.com/browse/DBZ-4903[DBZ-4903]
* Support Postgres 17 failover slots https://issues.redhat.com/browse/DBZ-8412[DBZ-8412]
* Improve error handling in dispatchSnapshotEvent of EventDispatcher https://issues.redhat.com/browse/DBZ-8433[DBZ-8433]
* Connector configuration logging improvement https://issues.redhat.com/browse/DBZ-8472[DBZ-8472]
* Handle un-parseable DDLs gracefully https://issues.redhat.com/browse/DBZ-8479[DBZ-8479]
* Track LogMiner partial rollback events in metrics https://issues.redhat.com/browse/DBZ-8491[DBZ-8491]
* Support JDBC offset/history configuration in CRD https://issues.redhat.com/browse/DBZ-8501[DBZ-8501]


=== Fixes

* Error with debezium.sink.pulsar.client.serviceUrl and debezium-server https://issues.redhat.com/browse/DBZ-3720[DBZ-3720]
* MySQL regression - Defaults store.only.captured.tables.ddl to true https://issues.redhat.com/browse/DBZ-6709[DBZ-6709]
* ExtractNewRecordState value of optional null field which has default value https://issues.redhat.com/browse/DBZ-7094[DBZ-7094]
* DebeziumException: No column '' where ' found in table https://issues.redhat.com/browse/DBZ-8034[DBZ-8034]
* MySQL Connector Does Not Act On `CREATE DATABASE` Records In The Binlog https://issues.redhat.com/browse/DBZ-8291[DBZ-8291]
* Vgtid doesn't contain multiple shard GTIDs when multiple tasks are used https://issues.redhat.com/browse/DBZ-8432[DBZ-8432]
* Object ID cache may fail with concurent modification expcetion https://issues.redhat.com/browse/DBZ-8465[DBZ-8465]
* Oracle gathers and logs object attributes for views unnecessarily https://issues.redhat.com/browse/DBZ-8492[DBZ-8492]
* ReselectColumnPostProcessor can throw ORA-01003 "no statement parsed" when using fallback non-flashback area query https://issues.redhat.com/browse/DBZ-8493[DBZ-8493]
* Oracle DDL ALTER TABLE ADD CONSTRAINT fails to be parsed https://issues.redhat.com/browse/DBZ-8494[DBZ-8494]
* Edit Source/Destination on adding new configuration properties its removing old once   https://issues.redhat.com/browse/DBZ-8495[DBZ-8495]
* Invalid property name in JDBC Schema History https://issues.redhat.com/browse/DBZ-8500[DBZ-8500]
* Fix the URL in Pipeline log page  https://issues.redhat.com/browse/DBZ-8502[DBZ-8502]
* Failed to start LogMiner mining session due to "Required Start SCN" error message https://issues.redhat.com/browse/DBZ-8503[DBZ-8503]
* Oracle data pump TEMPLATE_TABLE clause not supported https://issues.redhat.com/browse/DBZ-8504[DBZ-8504]
* Postgres alpine images require lang/llvm 19 for build https://issues.redhat.com/browse/DBZ-8505[DBZ-8505]
* TimezoneConverter include.list should be respected if set https://issues.redhat.com/browse/DBZ-8514[DBZ-8514]
* Missing log classes debezium-platform-conductor https://issues.redhat.com/browse/DBZ-8515[DBZ-8515]
* Debezium Server fails to start when using the sink Kinesis https://issues.redhat.com/browse/DBZ-8517[DBZ-8517]
* Skip GoldenGate REPLICATION MARKER events https://issues.redhat.com/browse/DBZ-8533[DBZ-8533]


=== Other changes

* Add example for SSL-enabled Kafka https://issues.redhat.com/browse/DBZ-1937[DBZ-1937]
* Create smoke test to make sure Debezium Server container image works https://issues.redhat.com/browse/DBZ-3226[DBZ-3226]
* Align MySQL and MariaDB grammars with upstream versions https://issues.redhat.com/browse/DBZ-8270[DBZ-8270]
* Support MongoDB 8.0 https://issues.redhat.com/browse/DBZ-8451[DBZ-8451]
* Update description of `message.key.columns` and format admonitions in PG doc https://issues.redhat.com/browse/DBZ-8455[DBZ-8455]
* Add Basic validation in UI to check for form completion before submitting. https://issues.redhat.com/browse/DBZ-8474[DBZ-8474]
* Use schema evolution tool to manage the conductor database https://issues.redhat.com/browse/DBZ-8486[DBZ-8486]
* Update Quarkus Outbox Extension to Quarkus 3.17.3 https://issues.redhat.com/browse/DBZ-8506[DBZ-8506]
* Merge conductor and stage into single platform repository https://issues.redhat.com/browse/DBZ-8508[DBZ-8508]
* Container Tests are executed with -DskipITs https://issues.redhat.com/browse/DBZ-8509[DBZ-8509]
* Add github workflow for UI unit testing on PRs https://issues.redhat.com/browse/DBZ-8526[DBZ-8526]



## 3.0.4.Final
November 28th 2024 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12438823)

### New features since 3.0.3.Final

* Update the UI to pass on the backend URL at runtime from ENV Var while running the container image [DBZ-8424](https://issues.redhat.com/browse/DBZ-8424)
* Add support for mysql_clear_password in mysql-binlog-connector [DBZ-8445](https://issues.redhat.com/browse/DBZ-8445)


### Breaking changes since 3.0.3.Final

None


### Fixes since 3.0.3.Final

* Debezium db2i CDC source connector does not seem to pickup JOURNAL_ENTRY_TYPES => 'DR' records [DBZ-8453](https://issues.redhat.com/browse/DBZ-8453)
* Randomly failing tests after migration to async engine [DBZ-8461](https://issues.redhat.com/browse/DBZ-8461)
* Invalid label used for API service discriminator [DBZ-8464](https://issues.redhat.com/browse/DBZ-8464)


### Other changes since 3.0.3.Final

* Migrate rest of the testsuite to async engine [DBZ-7977](https://issues.redhat.com/browse/DBZ-7977)
* Update QOSDK to version 6.9.1 [DBZ-8452](https://issues.redhat.com/browse/DBZ-8452)
* Add JDBC storage module in Debezium Server [DBZ-8460](https://issues.redhat.com/browse/DBZ-8460)



## 3.0.3.Final
November 25th 2024 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12436708)

### New features since 3.0.2.Final

* Add support for bpchar datatype [DBZ-8416](https://issues.redhat.com/browse/DBZ-8416)
* Allow parts of DS resource to reference values from primary in configuration [DBZ-8431](https://issues.redhat.com/browse/DBZ-8431)


### Breaking changes since 3.0.2.Final

None


### Fixes since 3.0.2.Final

* Spanner tests fail randomly [DBZ-8410](https://issues.redhat.com/browse/DBZ-8410)
* Engine shutdown may get stuck when error is thrown during connector stop [DBZ-8414](https://issues.redhat.com/browse/DBZ-8414)
* JdbcOffsetBackingStore does not release lock of debezium_offset_storage gracefully [DBZ-8423](https://issues.redhat.com/browse/DBZ-8423)
* Installation documentation typo on download link [DBZ-8429](https://issues.redhat.com/browse/DBZ-8429)
* Asycn engine fails with NPE when transformation returns null [DBZ-8434](https://issues.redhat.com/browse/DBZ-8434)
* Snapshot completed flag not correctly saved on offsets [DBZ-8449](https://issues.redhat.com/browse/DBZ-8449)
* Formatting characters render in descriptions of Oracle `log.mining` properties [DBZ-8450](https://issues.redhat.com/browse/DBZ-8450)
* Prevent data corruption from netty version 4.1.111.Final  [DBZ-8438](https://issues.redhat.com/browse/DBZ-8438)


### Other changes since 3.0.2.Final

* Support config map offset store in the DS Operator [DBZ-8352](https://issues.redhat.com/browse/DBZ-8352)
* Migrate Vitess testsuite to async engine [DBZ-8377](https://issues.redhat.com/browse/DBZ-8377)
* Migrate Spanner testsuite to async engine [DBZ-8381](https://issues.redhat.com/browse/DBZ-8381)
* Do not build images for unsupported database versions [DBZ-8413](https://issues.redhat.com/browse/DBZ-8413)
* Update PatternFly version in UI from 6.beta to final 6.0 [DBZ-8415](https://issues.redhat.com/browse/DBZ-8415)
* Fix the UI build issue  [DBZ-8435](https://issues.redhat.com/browse/DBZ-8435)
* Make AbstractConnectorTest#createEngine method abstract [DBZ-8441](https://issues.redhat.com/browse/DBZ-8441)



## 3.0.2.Final
November 15th 2024 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12435057)

### New features since 3.0.1.Final

* Add file signal channel documentation to the signal channel chapter [DBZ-7245](https://issues.redhat.com/browse/DBZ-7245)
* Improve blocking snapshot reliability in case of restart [DBZ-7903](https://issues.redhat.com/browse/DBZ-7903)
* Allow skipping exceptions related to DML parser errors [DBZ-8208](https://issues.redhat.com/browse/DBZ-8208)
* Ability to enable DS REST API in Operator CR [DBZ-8234](https://issues.redhat.com/browse/DBZ-8234)
* Add feature to download and stream the Pipeline logs from UI [DBZ-8239](https://issues.redhat.com/browse/DBZ-8239)
* Add support for vitess-connector to send DDL events [DBZ-8325](https://issues.redhat.com/browse/DBZ-8325)
* Vstream table filter to match full table names [DBZ-8354](https://issues.redhat.com/browse/DBZ-8354)
* RowsScanned JMX metric for MongoDB differs from relational connectors [DBZ-8359](https://issues.redhat.com/browse/DBZ-8359)
* Refactor CassandraTypeProvider to not contain getClusterName method [DBZ-8373](https://issues.redhat.com/browse/DBZ-8373)
* Possibility for Debezium Oracle Connector to accept NLS Time Format (For Date and Timestamp Columns) [DBZ-8379](https://issues.redhat.com/browse/DBZ-8379)
* Provide config to allow for sending schema change events without historized schemas [DBZ-8392](https://issues.redhat.com/browse/DBZ-8392)
* Implement new config map offset store in DS [DBZ-8351](https://issues.redhat.com/browse/DBZ-8351)


### Breaking changes since 3.0.1.Final

None


### Fixes since 3.0.1.Final

* Race condition in stop-snapshot signal [DBZ-8303](https://issues.redhat.com/browse/DBZ-8303)
* Debezium shifts binlog offset despite RabbitMQ Timeout and unconfirmed messages [DBZ-8307](https://issues.redhat.com/browse/DBZ-8307)
* Debezium server with eventhubs sink type and eventhubs emulator connection string fails [DBZ-8357](https://issues.redhat.com/browse/DBZ-8357)
* Filter for snapshot using signal doesn't seem to work [DBZ-8358](https://issues.redhat.com/browse/DBZ-8358)
* JDBC storage module does not use quay.io images [DBZ-8362](https://issues.redhat.com/browse/DBZ-8362)
* Failure on offset store call to configure/start is logged at DEBUG level [DBZ-8364](https://issues.redhat.com/browse/DBZ-8364)
* Object name is not in the list of S3 schema history fields [DBZ-8366](https://issues.redhat.com/browse/DBZ-8366)
* Faulty "Failed to load mandatory config" error message [DBZ-8367](https://issues.redhat.com/browse/DBZ-8367)
* Upgrade protobuf dependencies to avoid potential vulnerability [DBZ-8371](https://issues.redhat.com/browse/DBZ-8371)
* Tests in IncrementalSnapshotIT may fail randomly [DBZ-8386](https://issues.redhat.com/browse/DBZ-8386)
* ExtractNewRecordState transform: NPE when processing non-envelope records  [DBZ-8393](https://issues.redhat.com/browse/DBZ-8393)
* Oracle LogMiner metric OldestScnAgeInMilliseconds can be negative [DBZ-8395](https://issues.redhat.com/browse/DBZ-8395)
* SqlServerConnectorIT.restartInTheMiddleOfTxAfterCompletedTx fails randomly [DBZ-8396](https://issues.redhat.com/browse/DBZ-8396)
* ExtractNewDocumentStateTestIT fails randomly [DBZ-8397](https://issues.redhat.com/browse/DBZ-8397)
* BlockingSnapshotIT fails on Oracle [DBZ-8398](https://issues.redhat.com/browse/DBZ-8398)
* Oracle OBJECT_ID lookup and cause high CPU and latency in Hybrid mining mode [DBZ-8399](https://issues.redhat.com/browse/DBZ-8399)
* Protobuf plugin does not compile for PostgreSQL 17 on Debian [DBZ-8403](https://issues.redhat.com/browse/DBZ-8403)


### Other changes since 3.0.1.Final

* Clarify signal data collection should be unique per connector [DBZ-6837](https://issues.redhat.com/browse/DBZ-6837)
* Use DebeziumSinkRecord instead of Kafka Connect's SinkRecord inside Debezium sink connectors [DBZ-8346](https://issues.redhat.com/browse/DBZ-8346)
* Migrate SQL server testsuite to async engine [DBZ-8353](https://issues.redhat.com/browse/DBZ-8353)
* Remove unnecessary converter code from parsers [DBZ-8360](https://issues.redhat.com/browse/DBZ-8360)
* Deduplicate Cassandra Debezium tests [DBZ-8363](https://issues.redhat.com/browse/DBZ-8363)
* Migrate MongoDB testsuite to async engine [DBZ-8369](https://issues.redhat.com/browse/DBZ-8369)
* Migrate Oracle testsuite to async engine [DBZ-8370](https://issues.redhat.com/browse/DBZ-8370)
* Add transform page to provide a single place to list the already configured transform plus UI to add a new transform [DBZ-8374](https://issues.redhat.com/browse/DBZ-8374)
* Migrate rest of Debezium testsuite to async engine [DBZ-8375](https://issues.redhat.com/browse/DBZ-8375)
* Migrate DB2 testsuite to async engine [DBZ-8380](https://issues.redhat.com/browse/DBZ-8380)
* Migrate IBM i testsuite to async engine [DBZ-8382](https://issues.redhat.com/browse/DBZ-8382)
* Upgrade Kafka to 3.8.1 [DBZ-8385](https://issues.redhat.com/browse/DBZ-8385)
* Add Transform Edit and delete support. [DBZ-8388](https://issues.redhat.com/browse/DBZ-8388)
* Log SCN existence check may throw ORA-01291 if a recent checkpoint occurred [DBZ-8389](https://issues.redhat.com/browse/DBZ-8389)
* Upgrade Kafka to 3.9.0 [DBZ-8400](https://issues.redhat.com/browse/DBZ-8400)
* Update Quarkus Outbox Extension to Quarkus 3.16.3 [DBZ-8409](https://issues.redhat.com/browse/DBZ-8409)



## 3.0.1.Final
October 25th 2024 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12433891)

### New features since 3.0.0.Final

* Support batch write to AWS Kinesis [DBZ-8193](https://issues.redhat.com/browse/DBZ-8193)
* Support for PostgreSQL 17 [DBZ-8275](https://issues.redhat.com/browse/DBZ-8275)
* Extend Debezium Server to include support for application.yaml [DBZ-8313](https://issues.redhat.com/browse/DBZ-8313)
* SQL Server Documentation for CDC on Server table [DBZ-8314](https://issues.redhat.com/browse/DBZ-8314)
* Add support for MySQL 9.1 [DBZ-8324](https://issues.redhat.com/browse/DBZ-8324)
* Support Cassandra 5.0 [DBZ-8347](https://issues.redhat.com/browse/DBZ-8347)


### Breaking changes since 3.0.0.Final

* Stop publishing of container images into Docker Hub [DBZ-8327](https://issues.redhat.com/browse/DBZ-8327)


### Fixes since 3.0.0.Final

* Oracle DDL parsing will fail if the DDL ends with a new line character [DBZ-7040](https://issues.redhat.com/browse/DBZ-7040)
* Missing documentation for MongoDb SSL configuration [DBZ-7927](https://issues.redhat.com/browse/DBZ-7927)
* Conditionalization implemented for single-sourcing MySQL/MariaDB content isn't working as expected [DBZ-8094](https://issues.redhat.com/browse/DBZ-8094)
* Debezium is replaying all events from an older offset [DBZ-8194](https://issues.redhat.com/browse/DBZ-8194)
* Embedded MySqlConnector "Unable to find minimal snapshot lock mode" since 2.5.4.Final [DBZ-8271](https://issues.redhat.com/browse/DBZ-8271)
* Reselect Post Processor not working when pkey of type uuid etc. [DBZ-8277](https://issues.redhat.com/browse/DBZ-8277)
* BinlogStreamingChangeEventSource totalRecordCounter is never updated [DBZ-8290](https://issues.redhat.com/browse/DBZ-8290)
* Restart Oracle connector when ORA-01001 invalid cursor exception is thrown [DBZ-8292](https://issues.redhat.com/browse/DBZ-8292)
* Connector uses incorrect partition names when creating offsets [DBZ-8298](https://issues.redhat.com/browse/DBZ-8298)
* ReselectPostProcessor fails when reselecting columns from Oracle [DBZ-8304](https://issues.redhat.com/browse/DBZ-8304)
* Debezium MySQL DDL parser: SECONDARY_ENGINE=RAPID does not support [DBZ-8305](https://issues.redhat.com/browse/DBZ-8305)
* Oracle DDL failure - subpartition list clause does not support in-memory clause [DBZ-8315](https://issues.redhat.com/browse/DBZ-8315)
* DDL statement couldn't be parsed [DBZ-8316](https://issues.redhat.com/browse/DBZ-8316)
* Binary Log Client doesn't process the TRANSACTION_ PAYLOAD header [DBZ-8340](https://issues.redhat.com/browse/DBZ-8340)
* Oracle connector: archive.log.only.mode stop working after reach SYSDATE SCN [DBZ-8345](https://issues.redhat.com/browse/DBZ-8345)


### Other changes since 3.0.0.Final

* Provide example for activity monitoring metrics [DBZ-8174](https://issues.redhat.com/browse/DBZ-8174)
* Write blog post on how detect data mutation patterns with Debezium [DBZ-8256](https://issues.redhat.com/browse/DBZ-8256)
* Formatting characters render literally in docs [DBZ-8293](https://issues.redhat.com/browse/DBZ-8293)
* REST tests fail due to unable to execute cp [DBZ-8294](https://issues.redhat.com/browse/DBZ-8294)
* Create MariaDB systemtests [DBZ-8306](https://issues.redhat.com/browse/DBZ-8306)
* Refactor MySqlTests and MariaDBTests to share the tests via parent base class [DBZ-8309](https://issues.redhat.com/browse/DBZ-8309)
* Document how to work with ServiceLoader and bundled jars [DBZ-8318](https://issues.redhat.com/browse/DBZ-8318)
* Broken system tests for upstream [DBZ-8326](https://issues.redhat.com/browse/DBZ-8326)
* Upstream system tests are stuck in Retrieving connector metrics [DBZ-8330](https://issues.redhat.com/browse/DBZ-8330)
* Fix upstream JDBC system tests [DBZ-8331](https://issues.redhat.com/browse/DBZ-8331)
* Add version for Cassandra 5 to debezium-build-parent [DBZ-8348](https://issues.redhat.com/browse/DBZ-8348)



## 3.0.0.Final
October 2nd 2024 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12431955)

### New features since 3.0.0.CR2

* Add documentation for custom converters in PG [DBZ-7820](https://issues.redhat.com/browse/DBZ-7820)
* Create REST bridge for DBZ signal channels [DBZ-8101](https://issues.redhat.com/browse/DBZ-8101)
* Support int/bigint arrays in reselect colums postprocessors [DBZ-8212](https://issues.redhat.com/browse/DBZ-8212)
* Log the record key when debezium fails to send the record to Kafka [DBZ-8282](https://issues.redhat.com/browse/DBZ-8282)


### Breaking changes since 3.0.0.CR2

* Remove deprecated additional condition property for incremental snapshot [DBZ-8278](https://issues.redhat.com/browse/DBZ-8278)


### Fixes since 3.0.0.CR2

* Custom convert (all to strings) and SQLServer default '0' type issue [DBZ-7045](https://issues.redhat.com/browse/DBZ-7045)
* UnsupportedClassVersionError while running debezium-connector docker Image [DBZ-7751](https://issues.redhat.com/browse/DBZ-7751)
* Error writing data to target database. (Caused by: java.lang.RuntimeException: org.postgresql.util.PSQLException: The column index is out of range: 140, number of columns: 139.) [DBZ-8221](https://issues.redhat.com/browse/DBZ-8221)
* Debezium Server messages not being sent to Pub/Sub after restart [DBZ-8236](https://issues.redhat.com/browse/DBZ-8236)
* An aborted ad-hoc blocking snapshot leaves the connector in a broken state   [DBZ-8244](https://issues.redhat.com/browse/DBZ-8244)
* JDBC Sink truncate event also add event to updateBufferByTable [DBZ-8247](https://issues.redhat.com/browse/DBZ-8247)
* mysql-binlog-connector-java doesn't compile with java 21 [DBZ-8253](https://issues.redhat.com/browse/DBZ-8253)
* DDL statement couldn't be parsed. 'mismatched input 'NOCACHE' expecting {'AS', 'USAGE', ';'} [DBZ-8262](https://issues.redhat.com/browse/DBZ-8262)
* journal processing loops after journal offset reset [DBZ-8265](https://issues.redhat.com/browse/DBZ-8265)


### Other changes since 3.0.0.CR2

* Add async engine config options to server documentation [DBZ-8133](https://issues.redhat.com/browse/DBZ-8133)
* Bump apicurio schema registry to 2.6.2.Final [DBZ-8145](https://issues.redhat.com/browse/DBZ-8145)
* Correct description of the `all_tables` option for the PG `publication.autocreate.mode` property [DBZ-8268](https://issues.redhat.com/browse/DBZ-8268)
* Test docs for productization and fix broken links and rendering errors  [DBZ-8284](https://issues.redhat.com/browse/DBZ-8284)



## 3.0.0.CR2
September 25th 2024 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12433150)

### New features since 3.0.0.CR1

* Snapshot isolation level options for postgres [DBZ-1252](https://issues.redhat.com/browse/DBZ-1252)
* Retry flush records if LockAcquisitionException occured in mysql [DBZ-7291](https://issues.redhat.com/browse/DBZ-7291)
* Add support for MAX_STRING_SIZE set to EXTENDED [DBZ-8039](https://issues.redhat.com/browse/DBZ-8039)
* Add invalid value logger for dates to Debezium Vitess Connector  [DBZ-8235](https://issues.redhat.com/browse/DBZ-8235)
* Support BLOB with EMPTY_BLOB() as default [DBZ-8248](https://issues.redhat.com/browse/DBZ-8248)


### Breaking changes since 3.0.0.CR1

None


### Fixes since 3.0.0.CR1

* Debezium does not restart automatically after throwing an ORA-00600 krvrdccs30 error [DBZ-8223](https://issues.redhat.com/browse/DBZ-8223)
* JDBC sink doesn't include fields as per documentation [DBZ-8224](https://issues.redhat.com/browse/DBZ-8224)
* Unbounded number of processing threads in async engine [DBZ-8237](https://issues.redhat.com/browse/DBZ-8237)
* Streaming metrics are stuck after an ad-hoc blocking snapshot [DBZ-8238](https://issues.redhat.com/browse/DBZ-8238)
* DDL statement couldn't be parsed with IF EXISTS [DBZ-8240](https://issues.redhat.com/browse/DBZ-8240)
* Random engine factory used by default [DBZ-8241](https://issues.redhat.com/browse/DBZ-8241)
* JDBC sink test suite should use the debezium/connect:nightly image for e2e tests [DBZ-8245](https://issues.redhat.com/browse/DBZ-8245)
* Performance Regression in Debezium Server Kafka after DBZ-7575 fix [DBZ-8251](https://issues.redhat.com/browse/DBZ-8251)
* Error Prone library included in MySQL connector [DBZ-8258](https://issues.redhat.com/browse/DBZ-8258)
* Debezium.text.ParsingException: DDL statement couldn't be parsed [DBZ-8259](https://issues.redhat.com/browse/DBZ-8259)


### Other changes since 3.0.0.CR1

* Test and check compatibility with ojdbc11 [DBZ-3658](https://issues.redhat.com/browse/DBZ-3658)
* Broken link to Streams doc about configuring logging  [DBZ-8231](https://issues.redhat.com/browse/DBZ-8231)
* Document passthrough hibernate.* properties for the JDBC connector [DBZ-8232](https://issues.redhat.com/browse/DBZ-8232)
* Bump Infinispan to 15.0.8.Final [DBZ-8246](https://issues.redhat.com/browse/DBZ-8246)
* AbstractConnectorTest consumeRecordsUntil may prematurely exit loop [DBZ-8250](https://issues.redhat.com/browse/DBZ-8250)
* Add a note to the docs about JDBC batch retry configs [DBZ-8252](https://issues.redhat.com/browse/DBZ-8252)
* Fix conditionalization in shared MariaDB/MySQL file [DBZ-8254](https://issues.redhat.com/browse/DBZ-8254)
* Add Oracle FUTC license [DBZ-8260](https://issues.redhat.com/browse/DBZ-8260)
* Remove Oracle libs from product assembly package [DBZ-8261](https://issues.redhat.com/browse/DBZ-8261)
* debezium-connector-binlog does not need MariaDB dependency [DBZ-8263](https://issues.redhat.com/browse/DBZ-8263)
* Provide subset package for Debezium Server [DBZ-8264](https://issues.redhat.com/browse/DBZ-8264)
* Bump container images to Fedora 40 [DBZ-8266](https://issues.redhat.com/browse/DBZ-8266)



## 3.0.0.CR1
September 13rd 2024 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12432262)

### New features since 3.0.0.Beta1

* Add support for MySQL 9 [DBZ-8030](https://issues.redhat.com/browse/DBZ-8030)
* Add support for MySQL vector datatype [DBZ-8157](https://issues.redhat.com/browse/DBZ-8157)
* Refactor engine signal support [DBZ-8160](https://issues.redhat.com/browse/DBZ-8160)
* Add feature to inherit shard epoch [DBZ-8163](https://issues.redhat.com/browse/DBZ-8163)
* Avoid 3 second delay in Oracle when one of the RAC nodes is offline [DBZ-8177](https://issues.redhat.com/browse/DBZ-8177)
* Truncate byte buffer should return a new array [DBZ-8189](https://issues.redhat.com/browse/DBZ-8189)
* Support for older MongoDb versions [DBZ-8202](https://issues.redhat.com/browse/DBZ-8202)
* Add VECTOR functions to MySQL grammar [DBZ-8210](https://issues.redhat.com/browse/DBZ-8210)
* Support MariaDB 11.4.3 [DBZ-8226](https://issues.redhat.com/browse/DBZ-8226)
* Add information about helm chart installation to operator readme [DBZ-8233](https://issues.redhat.com/browse/DBZ-8233)


### Breaking changes since 3.0.0.Beta1

* Error registering JMX signal and notification for multi task SQLServer  [DBZ-8137](https://issues.redhat.com/browse/DBZ-8137)
* Remove several deprecated Oracle configuration options [DBZ-8181](https://issues.redhat.com/browse/DBZ-8181)
* Unify vector datatypes between PostgreSQL and MySQL [DBZ-8183](https://issues.redhat.com/browse/DBZ-8183)


### Fixes since 3.0.0.Beta1

* Make ORA-00600 - krvrdccs10 automatically retriable [DBZ-5009](https://issues.redhat.com/browse/DBZ-5009)
* Incremental snapshot fails with NPE if surrogate key doesn't exist [DBZ-7797](https://issues.redhat.com/browse/DBZ-7797)
* MySQL 8.4 incompatibility due to removed SQL commands [DBZ-7838](https://issues.redhat.com/browse/DBZ-7838)
* Postgres connector - null value processing for "money" type column. [DBZ-8027](https://issues.redhat.com/browse/DBZ-8027)
* Using snapshot.include.collection.list with Oracle raises NullPointerException [DBZ-8032](https://issues.redhat.com/browse/DBZ-8032)
* Performance degradation when reconstructing (log.mining.stragtegy hybrid mode) [DBZ-8071](https://issues.redhat.com/browse/DBZ-8071)
* The source data type exceeds the debezium data type and cannot deserialize the object [DBZ-8142](https://issues.redhat.com/browse/DBZ-8142)
* Incorrect use of generic types in tests [DBZ-8166](https://issues.redhat.com/browse/DBZ-8166)
* Postgres JSONB Fields are not supported with Reselect Post Processor [DBZ-8168](https://issues.redhat.com/browse/DBZ-8168)
* NullPointerException (schemaUpdateCache is null) when restarting Oracle engine [DBZ-8187](https://issues.redhat.com/browse/DBZ-8187)
* XStream may fail to attach on retry if previous attempt failed [DBZ-8188](https://issues.redhat.com/browse/DBZ-8188)
* Exclude Oracle 23 VECSYS tablespace from capture [DBZ-8198](https://issues.redhat.com/browse/DBZ-8198)
* AbstractProcessorTest uses an incorrect database name when run against Oracle 23 Free edition [DBZ-8199](https://issues.redhat.com/browse/DBZ-8199)
* DDL statement couldn't be parsed: REVOKE IF EXISTS [DBZ-8209](https://issues.redhat.com/browse/DBZ-8209)
* System testsuite fails with route name being too long [DBZ-8213](https://issues.redhat.com/browse/DBZ-8213)
* Oracle TableSchemaBuilder provides wrong column name in error message [DBZ-8217](https://issues.redhat.com/browse/DBZ-8217)
* Using ehcache in Kafka connect throws an XMLConfiguration parse exception [DBZ-8219](https://issues.redhat.com/browse/DBZ-8219)
* OcpJdbcSinkConnectorIT fails [DBZ-8228](https://issues.redhat.com/browse/DBZ-8228)
* Container image does not install correct apicurio deps [DBZ-8230](https://issues.redhat.com/browse/DBZ-8230)


### Other changes since 3.0.0.Beta1

* Documentation for signals provides incorrect data-collection format for some connectors [DBZ-8090](https://issues.redhat.com/browse/DBZ-8090)
* Latest Informix JDBC Driver [DBZ-8167](https://issues.redhat.com/browse/DBZ-8167)
* upgrade Adobe s3mock to version 3.10.0 [DBZ-8169](https://issues.redhat.com/browse/DBZ-8169)
* Include Jackson libraries to JDBC connector Docker image distribution [DBZ-8175](https://issues.redhat.com/browse/DBZ-8175)
* Ehcache fails to start, throwing "Invaild XML Configuration" [DBZ-8178](https://issues.redhat.com/browse/DBZ-8178)
* Enable snapshot.database.errors.max.retriesEnable during Oracle tests [DBZ-8184](https://issues.redhat.com/browse/DBZ-8184)
* Change event for a logical decoding message doesn't contain `transaction` field [DBZ-8185](https://issues.redhat.com/browse/DBZ-8185)
* Add MariaDB connector server distribution [DBZ-8186](https://issues.redhat.com/browse/DBZ-8186)
* Update Vitess example to Debezium 2.7/Vitess 19 [DBZ-8196](https://issues.redhat.com/browse/DBZ-8196)
* OracleConnectorIT test shouldGracefullySkipObjectBasedTables can timeout prematurely [DBZ-8197](https://issues.redhat.com/browse/DBZ-8197)
* Reduce log verbosity of OpenLogReplicator SCN confirmation [DBZ-8201](https://issues.redhat.com/browse/DBZ-8201)
* Implement separate source and sink connector sections in documentation navigation [DBZ-8220](https://issues.redhat.com/browse/DBZ-8220)



## 3.0.0.Beta1
August 22nd 2024 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12431096)

### New features since 3.0.0.Alpha2

* Implement Ehcache event buffer [DBZ-7758](https://issues.redhat.com/browse/DBZ-7758)
* Expose a metric for number of create, update, delete events per table [DBZ-8035](https://issues.redhat.com/browse/DBZ-8035)
* Log additional details about abandoned transactions [DBZ-8044](https://issues.redhat.com/browse/DBZ-8044)
* Introduce timeout for replication slot creation [DBZ-8073](https://issues.redhat.com/browse/DBZ-8073)
* ConverterBuilder doesn't pass Headers to be manipulated [DBZ-8082](https://issues.redhat.com/browse/DBZ-8082)
* Add SMT to decode binary content of a logical decoding message [DBZ-8103](https://issues.redhat.com/browse/DBZ-8103)
* Support DECIMAL(p) Floating Point [DBZ-8114](https://issues.redhat.com/browse/DBZ-8114)
* Support for PgVector datatypes [DBZ-8121](https://issues.redhat.com/browse/DBZ-8121)
* Implement in process signal channel  [DBZ-8135](https://issues.redhat.com/browse/DBZ-8135)
* Validate log position method missing gtid info from SourceInfo [DBZ-8140](https://issues.redhat.com/browse/DBZ-8140)
* Vitess Connector Epoch should support parallelism & shard changes [DBZ-8154](https://issues.redhat.com/browse/DBZ-8154)
* Add an option for `publication.autocreate.mode` to create a publication with no tables [DBZ-8156](https://issues.redhat.com/browse/DBZ-8156)


### Breaking changes since 3.0.0.Alpha2

* Debezium Server Kafka BLOCKED forever when Kafka send failed [DBZ-7575](https://issues.redhat.com/browse/DBZ-7575)
* Rabbitmq native stream support individual stream for each table [DBZ-8118](https://issues.redhat.com/browse/DBZ-8118)


### Fixes since 3.0.0.Alpha2

* Incremental snapshots don't work with CloudEvent converter [DBZ-7601](https://issues.redhat.com/browse/DBZ-7601)
* Snapshot retrying logic falls into infinite retry loop [DBZ-7860](https://issues.redhat.com/browse/DBZ-7860)
* Primary Key Update/ Snapshot Race Condition [DBZ-8113](https://issues.redhat.com/browse/DBZ-8113)
* Docs: connect-log4j.properties instead log4j.properties [DBZ-8117](https://issues.redhat.com/browse/DBZ-8117)
* Recalculating mining range upper bounds causes getScnFromTimestamp to fail [DBZ-8119](https://issues.redhat.com/browse/DBZ-8119)
* ORA-00600: internal error code, arguments: [krvrdGetUID:2], [18446744073709551614], [], [], [], [], [], [], [], [], [], [] [DBZ-8125](https://issues.redhat.com/browse/DBZ-8125)
* ConvertingFailureIT#shouldFailConversionTimeTypeWithConnectModeWhenFailMode fails randomly [DBZ-8128](https://issues.redhat.com/browse/DBZ-8128)
* ibmi Connector does not take custom properties into account anymore [DBZ-8129](https://issues.redhat.com/browse/DBZ-8129)
* Unpredicatable ordering of table rows during insertion causing foreign key error [DBZ-8130](https://issues.redhat.com/browse/DBZ-8130)
* schema_only crashes ibmi Connector [DBZ-8131](https://issues.redhat.com/browse/DBZ-8131)
* Support larger database.server.id values [DBZ-8134](https://issues.redhat.com/browse/DBZ-8134)
* Open redo thread consistency check can lead to ORA-01291 - missing logfile [DBZ-8144](https://issues.redhat.com/browse/DBZ-8144)
* SchemaOnlyRecoverySnapshotter not registered as an SPI service implementation [DBZ-8147](https://issues.redhat.com/browse/DBZ-8147)
* When stopping the Oracle rac node the Debezium server throws an expections - ORA-12514: Cannot connect to database and retries  [DBZ-8149](https://issues.redhat.com/browse/DBZ-8149)
* Issue with Debezium Snapshot: DateTimeParseException with plugin pgoutput [DBZ-8150](https://issues.redhat.com/browse/DBZ-8150)
* JDBC connector validation fails when using record_value with no primary.key.fields [DBZ-8151](https://issues.redhat.com/browse/DBZ-8151)
* Taking RAC node offline and back online can lead to thread inconsistency [DBZ-8162](https://issues.redhat.com/browse/DBZ-8162)


### Other changes since 3.0.0.Alpha2

* MySQL has deprecated mysql_native_password usage [DBZ-7049](https://issues.redhat.com/browse/DBZ-7049)
* Upgrade to Apicurio 2.5.8 or higher [DBZ-7357](https://issues.redhat.com/browse/DBZ-7357)
* Write and publish Debezium Orchestra blog post [DBZ-7972](https://issues.redhat.com/browse/DBZ-7972)
* Move Debezium Conductor repository under Debezium Organisation [DBZ-7973](https://issues.redhat.com/browse/DBZ-7973)
* Decide on name, jira components, etc... for Debezium Orchestra platform [DBZ-7975](https://issues.redhat.com/browse/DBZ-7975)
* Migrate Postgres testsuite to async engine [DBZ-8077](https://issues.redhat.com/browse/DBZ-8077)
* Conditionalize reference to the MySQL default value in description of `schema.history.internal.store.only.captured.databases.ddl` [DBZ-8081](https://issues.redhat.com/browse/DBZ-8081)
* Bump Debezium Server to Quarkus 3.8.5 [DBZ-8095](https://issues.redhat.com/browse/DBZ-8095)
* Converters documentation uses incorrect examples [DBZ-8104](https://issues.redhat.com/browse/DBZ-8104)
* Remove reference to`additional condition` signal parameter from ad hoc snapshots doc [DBZ-8107](https://issues.redhat.com/browse/DBZ-8107)
* TimescaleDbDatabaseTest.shouldTransformCompressedChunks is failing [DBZ-8123](https://issues.redhat.com/browse/DBZ-8123)
* Update Oracle connector doc to describe options for restricting access permissions for the Debezium LogMiner user  [DBZ-8124](https://issues.redhat.com/browse/DBZ-8124)
* Use SQLSTATE to handle exceptions for replication slot creation command timeout [DBZ-8127](https://issues.redhat.com/browse/DBZ-8127)
* Re-add check to test for if assembly profile is active [DBZ-8138](https://issues.redhat.com/browse/DBZ-8138)
* Add LogMiner start mining session retry attempt counter to logs [DBZ-8143](https://issues.redhat.com/browse/DBZ-8143)
* Reduce logging verbosity of XStream DML event data [DBZ-8148](https://issues.redhat.com/browse/DBZ-8148)
* Upgrade Outbox Extension to Quarkus 3.14.0 [DBZ-8164](https://issues.redhat.com/browse/DBZ-8164)



## 3.0.0.Alpha2
August 2nd 2024 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12430393)

### New features since 3.0.0.Alpha1

* Add Status ObservedGeneration to Operator [DBZ-8025](https://issues.redhat.com/browse/DBZ-8025)
* Support Custom Converters in Debezium Server [DBZ-8040](https://issues.redhat.com/browse/DBZ-8040)
* Support FLOAT32 type in debezium-connector-spanner [DBZ-8043](https://issues.redhat.com/browse/DBZ-8043)
* Debezium should auto exclude empty shards (no tablets) and not crash on keyspaces with empty shards [DBZ-8053](https://issues.redhat.com/browse/DBZ-8053)
* Refactor LogMining implementation to allow alternative cache implementations [DBZ-8054](https://issues.redhat.com/browse/DBZ-8054)
* Standard Webhooks signatures for HTTP sink [DBZ-8063](https://issues.redhat.com/browse/DBZ-8063)
* Vitess-connector should provide a topic naming strategy that supports separate connectors per-table [DBZ-8069](https://issues.redhat.com/browse/DBZ-8069)
* Update third-party LICENSE with LGPL forMariaDB Connector/J [DBZ-8099](https://issues.redhat.com/browse/DBZ-8099)
* Rabbitmq native stream Failed [DBZ-8108](https://issues.redhat.com/browse/DBZ-8108)


### Breaking changes since 3.0.0.Alpha1

* Upgrade to Kafka 3.8.0 [DBZ-8105](https://issues.redhat.com/browse/DBZ-8105)


### Fixes since 3.0.0.Alpha1

* Embedded Infinispan tests fail to start with Java 23 [DBZ-7840](https://issues.redhat.com/browse/DBZ-7840)
* Clarify that Oracle connector does not read from physical standby [DBZ-7895](https://issues.redhat.com/browse/DBZ-7895)
* StackOverflow exception on incremental snapshot [DBZ-8011](https://issues.redhat.com/browse/DBZ-8011)
* JDBC primary.key.fields cannot be empty when i set insert.mode to upsert  and primary.key.mode record_value [DBZ-8018](https://issues.redhat.com/browse/DBZ-8018)
* Unable to acquire buffer lock, buffer queue is likely full [DBZ-8022](https://issues.redhat.com/browse/DBZ-8022)
* Release process sets incorrect images for k8s for the next development version  [DBZ-8041](https://issues.redhat.com/browse/DBZ-8041)
* Use recrate as (default) rollout strategy for deployments [DBZ-8047](https://issues.redhat.com/browse/DBZ-8047)
* "Unexpected input: ." when snapshot incremental empty Database [DBZ-8050](https://issues.redhat.com/browse/DBZ-8050)
* Debezium Operator Using RollingUpdate Strategy [DBZ-8051](https://issues.redhat.com/browse/DBZ-8051)
* Debezium Operator Using RollingUpdate Strategy [DBZ-8052](https://issues.redhat.com/browse/DBZ-8052)
* Oracle connector inconsistency in redo log switches [DBZ-8055](https://issues.redhat.com/browse/DBZ-8055)
* Blocking snapshot can fail due to CommunicationsException [DBZ-8058](https://issues.redhat.com/browse/DBZ-8058)
* FakeDNS not working with JDK version > 18 [DBZ-8059](https://issues.redhat.com/browse/DBZ-8059)
* Debezium Operator with a provided Service Account doesn't spin up deployment [DBZ-8061](https://issues.redhat.com/browse/DBZ-8061)
* ParsingException (MySQL/MariaDB): rename table syntax [DBZ-8066](https://issues.redhat.com/browse/DBZ-8066)
* Oracle histogram metrics are no longer printed in logs correctly [DBZ-8068](https://issues.redhat.com/browse/DBZ-8068)
* In hybrid  log.mining.strategy reconstruction logs should be set to DEBUG [DBZ-8070](https://issues.redhat.com/browse/DBZ-8070)
* Support capturing BLOB column types during snapshot for MySQL/MariaDB [DBZ-8076](https://issues.redhat.com/browse/DBZ-8076)
* Standard Webhooks auth secret config value is not marked as PASSWORD_PATTERN  [DBZ-8078](https://issues.redhat.com/browse/DBZ-8078)
* Vitess transaction Epoch should not reset to zero when tx ID is missing [DBZ-8087](https://issues.redhat.com/browse/DBZ-8087)
* After changing the column datatype from int to float the Debezium fails to round it and i get a null value for this field in the stream [DBZ-8089](https://issues.redhat.com/browse/DBZ-8089)
* MySQL and MariaDB keyword YES cannot be parsed as a column name [DBZ-8092](https://issues.redhat.com/browse/DBZ-8092)
* NotificationIT tests seemingly seem to fail due to stepping on one another [DBZ-8100](https://issues.redhat.com/browse/DBZ-8100)
* ORA-26928 - Unable to communicate with XStream apply coordinator process should be retriable [DBZ-8102](https://issues.redhat.com/browse/DBZ-8102)
* Transformations are not closed in emebdded engine [DBZ-8106](https://issues.redhat.com/browse/DBZ-8106)
* Don't close connection after loading timescale metadata in TimescaleDb SMT [DBZ-8109](https://issues.redhat.com/browse/DBZ-8109)


### Other changes since 3.0.0.Alpha1

* Bump Infinispan to 14.0.29.Final [DBZ-8010](https://issues.redhat.com/browse/DBZ-8010)
* Write a blog post about async engine [DBZ-8013](https://issues.redhat.com/browse/DBZ-8013)
* Test offset/history store configurations [DBZ-8015](https://issues.redhat.com/browse/DBZ-8015)
* Upgrade postgres server version to 15 [DBZ-8062](https://issues.redhat.com/browse/DBZ-8062)
* Disable DebeziumResourceNoTopicCreationIT - no longer compatible with Java 21 [DBZ-8067](https://issues.redhat.com/browse/DBZ-8067)
* Speed-up PostgresShutdownIT [DBZ-8075](https://issues.redhat.com/browse/DBZ-8075)
* Add MariaDB to debezium/connect image [DBZ-8088](https://issues.redhat.com/browse/DBZ-8088)



## 3.0.0.Alpha1
July 11st 2024 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12413693)

### New features since 2.7.0.Final

* Provide MongoDB sink connector [DBZ-7223](https://issues.redhat.com/browse/DBZ-7223)
* Extends process of finding Bundle path [DBZ-7992](https://issues.redhat.com/browse/DBZ-7992)
* Support FLOAT32 type in debezium-connector-spanner [DBZ-8043](https://issues.redhat.com/browse/DBZ-8043)


### Breaking changes since 2.7.0.Final

* Use Java 17 in container images [DBZ-6795](https://issues.redhat.com/browse/DBZ-6795)


### Fixes since 2.7.0.Final

* Debezium postgres jdbc sink not handling infinity values [DBZ-7920](https://issues.redhat.com/browse/DBZ-7920)
* JdbcSinkTask doesn't clear offsets on stop [DBZ-7946](https://issues.redhat.com/browse/DBZ-7946)
* ibmi as400 connector config isn't prefixed with "database." [DBZ-7955](https://issues.redhat.com/browse/DBZ-7955)
* Duplicate downstream annotation comments incorrectly refer to Db2 connector [DBZ-7968](https://issues.redhat.com/browse/DBZ-7968)
* Issue with Hybrid mode and DDL change [DBZ-7991](https://issues.redhat.com/browse/DBZ-7991)
* Incorrect offset/history property mapping generatated  [DBZ-8007](https://issues.redhat.com/browse/DBZ-8007)
* Debezium Server Operator on minikube with java.lang.NullPointerException': java.lang.NullPointerException [DBZ-8019](https://issues.redhat.com/browse/DBZ-8019)
* ORA-65090: operation only allowed in a container database when connecting to a non-CDB database [DBZ-8023](https://issues.redhat.com/browse/DBZ-8023)
* Added type to Prometheus JMX exporter [DBZ-8036](https://issues.redhat.com/browse/DBZ-8036)
* Add `kafka.producer` metrics to debezium-server jmx exporter config [DBZ-8037](https://issues.redhat.com/browse/DBZ-8037)


### Other changes since 2.7.0.Final

* Use Java 17 as baseline [DBZ-7224](https://issues.redhat.com/browse/DBZ-7224)
* Document new MariaDB connector [DBZ-7786](https://issues.redhat.com/browse/DBZ-7786)
* Move to Maven 3.9.8 as build requirement [DBZ-7965](https://issues.redhat.com/browse/DBZ-7965)
* Add disclaimer that PostProcessors and CustomConverters are Debezium source connectors only [DBZ-8031](https://issues.redhat.com/browse/DBZ-8031)
* Typos in Bug report template [DBZ-8038](https://issues.redhat.com/browse/DBZ-8038)
* Find an alternative way to manually deploy the connector with local changes that is compatible with Debezium 3 [DBZ-8046](https://issues.redhat.com/browse/DBZ-8046)



## 2.7.0.Final
June 28th 2024 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12429396)

### New features since 2.7.0.Beta2

* Suport collection scoped streaming [DBZ-7760](https://issues.redhat.com/browse/DBZ-7760)
* Allow stoping DS instance by scaling to zero via annotation [DBZ-7953](https://issues.redhat.com/browse/DBZ-7953)
* Support heartbeat events in vitess-connector [DBZ-7962](https://issues.redhat.com/browse/DBZ-7962)


### Breaking changes since 2.7.0.Beta2

None


### Fixes since 2.7.0.Beta2

* Unable to use resume token of some documents with composite IDs [DBZ-6522](https://issues.redhat.com/browse/DBZ-6522)
* Quarkus generates VSC kubernetes annotations pointing to a fork [DBZ-7415](https://issues.redhat.com/browse/DBZ-7415)
* MongoDB documentation still mentions replica_set connection mode  [DBZ-7862](https://issues.redhat.com/browse/DBZ-7862)
* Clarify documentation for log.mining.archive.destination.name Oracle configuration property [DBZ-7939](https://issues.redhat.com/browse/DBZ-7939)
* Ad-hoc snapshot raises ORA-00911 when table name uses non-standard characters requiring quotations [DBZ-7942](https://issues.redhat.com/browse/DBZ-7942)
* Exclude signaling data collection from the snapshot process [DBZ-7944](https://issues.redhat.com/browse/DBZ-7944)
* JDBC sink time tests fail due to increased precision with SQL Server [DBZ-7949](https://issues.redhat.com/browse/DBZ-7949)
* Commit is not called after DDLs in JDBC stores [DBZ-7951](https://issues.redhat.com/browse/DBZ-7951)
* Database case sensitivity can lead to NullPointerException on column lookups [DBZ-7956](https://issues.redhat.com/browse/DBZ-7956)
* Debezium ibmi connector drops journal entries [DBZ-7957](https://issues.redhat.com/browse/DBZ-7957)
* Error counter reset in poll() can cause infinite retries [DBZ-7964](https://issues.redhat.com/browse/DBZ-7964)
* Oracle DDL parser fails using NOMONITORING clause [DBZ-7967](https://issues.redhat.com/browse/DBZ-7967)
* Invalid default DSimage used for nighly/snapshot operator version [DBZ-7970](https://issues.redhat.com/browse/DBZ-7970)
* Mongo Oversized Document FAQ documentation issue [DBZ-7987](https://issues.redhat.com/browse/DBZ-7987)
* Cassandra connector does not work with 2.6.1 Server [DBZ-7988](https://issues.redhat.com/browse/DBZ-7988)
* Testcontainers tests fails on newer versions of Docker [DBZ-7986](https://issues.redhat.com/browse/DBZ-7986)


### Other changes since 2.7.0.Beta2

* Document the use of the "source" prefix usage for table name formats [DBZ-6618](https://issues.redhat.com/browse/DBZ-6618)
* Remove dependency on MySQL driver, add custom CharacterSet Mapper [DBZ-7783](https://issues.redhat.com/browse/DBZ-7783)
* Rebase website-builder image on Ruby 3.2 [DBZ-7916](https://issues.redhat.com/browse/DBZ-7916)
* Warn about incompatible usage of read.only property for PostgreSQL  [DBZ-7947](https://issues.redhat.com/browse/DBZ-7947)
* Run JDBC sink tests for any relational connector pull requests [DBZ-7948](https://issues.redhat.com/browse/DBZ-7948)
* Bump Quarkus to 3.12.0 for Quarkus Outbox Extension [DBZ-7961](https://issues.redhat.com/browse/DBZ-7961)
* Bump Hibernate dependency to 6.4.8.Final [DBZ-7969](https://issues.redhat.com/browse/DBZ-7969)
* Deprecated EmbeddedEngine [DBZ-7976](https://issues.redhat.com/browse/DBZ-7976)



## 2.7.0.Beta2
June 13rd 2024 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12429023)

### New features since 2.7.0.Beta1

* Allow skipping of table row count in snapshot phase [DBZ-7640](https://issues.redhat.com/browse/DBZ-7640)
* Add heartbeat action query to SQL Server [DBZ-7801](https://issues.redhat.com/browse/DBZ-7801)
* Read-only incremental snapshots for PostgreSQL [DBZ-7917](https://issues.redhat.com/browse/DBZ-7917)
* Support truncation of byte arrays [DBZ-7925](https://issues.redhat.com/browse/DBZ-7925)


### Breaking changes since 2.7.0.Beta1

* Drop support for PostgreSQL 10 and 11 [DBZ-7128](https://issues.redhat.com/browse/DBZ-7128)


### Fixes since 2.7.0.Beta1

* Oracle property column.truncate.to.length.chars does not support length zero [DBZ-7079](https://issues.redhat.com/browse/DBZ-7079)
* Debezium Server cannot pass empty string to Kafka config [DBZ-7767](https://issues.redhat.com/browse/DBZ-7767)
* Unable To Exclude Column Using Configuration [DBZ-7813](https://issues.redhat.com/browse/DBZ-7813)
* Oracle connector failed to work when the table name contains single quote [DBZ-7831](https://issues.redhat.com/browse/DBZ-7831)
* Incorrect documentation for CE type  [DBZ-7926](https://issues.redhat.com/browse/DBZ-7926)
* DDL statement couldn't be parsed [DBZ-7931](https://issues.redhat.com/browse/DBZ-7931)
* SQL Server default value resolution for TIME data types causes precision loss [DBZ-7933](https://issues.redhat.com/browse/DBZ-7933)
* Incorrect name of JMX Exporter k8s service [DBZ-7934](https://issues.redhat.com/browse/DBZ-7934)
* OlrNetworkClient does not disconnect when error occurs [DBZ-7935](https://issues.redhat.com/browse/DBZ-7935)
* Multiple ARRAY types in single table causing error [DBZ-7938](https://issues.redhat.com/browse/DBZ-7938)


### Other changes since 2.7.0.Beta1

* Create REST extension tests and infrastructure [DBZ-7785](https://issues.redhat.com/browse/DBZ-7785)
* Introduce ROW_ID for OpenLogReplicator changes [DBZ-7823](https://issues.redhat.com/browse/DBZ-7823)
* Test SqlServerConnectorIT#shouldStopRetriableRestartsAtConfiguredMaximumDuringStreaming is failing [DBZ-7936](https://issues.redhat.com/browse/DBZ-7936)
* Add exception details when engine fails to commit offset [DBZ-7937](https://issues.redhat.com/browse/DBZ-7937)



## 2.7.0.Beta1
June 6th 2024 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12428104)

### New features since 2.7.0.Alpha2

* debezium-connector-db2: z/OS integration [DBZ-4812](https://issues.redhat.com/browse/DBZ-4812)
* Ensure vgtid remains local to shards streamed by task [DBZ-6721](https://issues.redhat.com/browse/DBZ-6721)
* Decompose provide.transaction.metadata into components [DBZ-6722](https://issues.redhat.com/browse/DBZ-6722)
* Handle Enum as String or Int [DBZ-7792](https://issues.redhat.com/browse/DBZ-7792)
* MariaDB target should support 'upsert' for insert.mode [DBZ-7874](https://issues.redhat.com/browse/DBZ-7874)
* Add support for user/password authentication in Nats Jetstream sink adapter [DBZ-7876](https://issues.redhat.com/browse/DBZ-7876)
* Allow customizing ObjectMapper in JsonSerde [DBZ-7887](https://issues.redhat.com/browse/DBZ-7887)
* Add configurable delay after successful snapshot before starting streaming [DBZ-7902](https://issues.redhat.com/browse/DBZ-7902)
* Enhancing the threads utility class for broader use [DBZ-7906](https://issues.redhat.com/browse/DBZ-7906)
* Include Prometheus JMX exporter in Debezium Server distribution [DBZ-7913](https://issues.redhat.com/browse/DBZ-7913)
* Add support for TLS auth for NATS JetStream sink [DBZ-7922](https://issues.redhat.com/browse/DBZ-7922)


### Breaking changes since 2.7.0.Alpha2

* Debezium snapshots are being deployed to old Sonatype infrastucture [DBZ-7641](https://issues.redhat.com/browse/DBZ-7641)
* Oracle connector decimal.handling.mode improvement [DBZ-7882](https://issues.redhat.com/browse/DBZ-7882)


### Fixes since 2.7.0.Alpha2

* Debezium 1.9.2 cannot capture field that is date type of postgres [DBZ-5182](https://issues.redhat.com/browse/DBZ-5182)
* Rewrite batch statement not supported for jdbc debezium sink [DBZ-7845](https://issues.redhat.com/browse/DBZ-7845)
* Debezium MySQL Snapshot Connector Fails [DBZ-7858](https://issues.redhat.com/browse/DBZ-7858)
* Reduce enum array allocation [DBZ-7859](https://issues.redhat.com/browse/DBZ-7859)
* Snapshot retrying logic falls into infinite retry loop [DBZ-7860](https://issues.redhat.com/browse/DBZ-7860)
* Bump Java in Debezium Server images [DBZ-7861](https://issues.redhat.com/browse/DBZ-7861)
* Default value of error retries not interpreted correctly [DBZ-7870](https://issues.redhat.com/browse/DBZ-7870)
* Avro schema compatibility issues when upgrading from Oracle Debezium 2.5.3.Final to 2.6.1.Final [DBZ-7880](https://issues.redhat.com/browse/DBZ-7880)
* Improve offset and history storage configuration [DBZ-7884](https://issues.redhat.com/browse/DBZ-7884)
* Oracle Debezium Connector cannot startup due to failing incremental snapshot [DBZ-7886](https://issues.redhat.com/browse/DBZ-7886)
* Multiple completed reading from a capture instance notifications [DBZ-7889](https://issues.redhat.com/browse/DBZ-7889)
* Debezium can't handle columns with # in its name [DBZ-7893](https://issues.redhat.com/browse/DBZ-7893)
* Oracle interval default values are not properly parsed [DBZ-7898](https://issues.redhat.com/browse/DBZ-7898)
* Debezium server unable to shutdown on pubsub error  [DBZ-7904](https://issues.redhat.com/browse/DBZ-7904)
* Handle gtid without range only single position [DBZ-7905](https://issues.redhat.com/browse/DBZ-7905)
* Oracle connector cannot parse SUBPARTITION when altering table [DBZ-7908](https://issues.redhat.com/browse/DBZ-7908)
* Make column exclude use keyspace not shard [DBZ-7910](https://issues.redhat.com/browse/DBZ-7910)
* The explanation in the documentation is insufficient - metric [DBZ-7912](https://issues.redhat.com/browse/DBZ-7912)


### Other changes since 2.7.0.Alpha2

* Too much logs after Debezium update [DBZ-7871](https://issues.redhat.com/browse/DBZ-7871)
* Test Geometry and Geography columns during Initial Snapshot  [DBZ-7878](https://issues.redhat.com/browse/DBZ-7878)
* Remove incubating note from post-processors index.adoc file [DBZ-7890](https://issues.redhat.com/browse/DBZ-7890)



## 2.7.0.Alpha2
May 10th 2024 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12427305)

### New features since 2.7.0.Alpha1

* Add ROW_ID as part of source information block for LogMiner sources [DBZ-4332](https://issues.redhat.com/browse/DBZ-4332)
* Support for ARRAY data types for postgres [DBZ-7752](https://issues.redhat.com/browse/DBZ-7752)
* Enhance documentation about using tags to customize connector snapshot/streaming MBean names [DBZ-7800](https://issues.redhat.com/browse/DBZ-7800)
* Allow specifying the log mining flush table with an optional schema [DBZ-7819](https://issues.redhat.com/browse/DBZ-7819)
* Added nats JWT/seed authentication config options [DBZ-7829](https://issues.redhat.com/browse/DBZ-7829)
* Update Debezium container images to use Fedora 38 [DBZ-7832](https://issues.redhat.com/browse/DBZ-7832)
* Debezium oracle connectors needs to support IN clause for log miner query for more than 1000 tables as it creates performance issue [DBZ-7847](https://issues.redhat.com/browse/DBZ-7847)


### Breaking changes since 2.7.0.Alpha1

None


### Fixes since 2.7.0.Alpha1

* Debezium User Guide 2.5.4: Grammatical error [DBZ-7803](https://issues.redhat.com/browse/DBZ-7803)
* > io.debezium.text.ParsingException : SQL Contains Partition [DBZ-7805](https://issues.redhat.com/browse/DBZ-7805)
* Ad-hoc blocking snapshot not working through file channeling without inserting a row in the database. [DBZ-7806](https://issues.redhat.com/browse/DBZ-7806)
* Postgres: Potential data loss on connector restart [DBZ-7816](https://issues.redhat.com/browse/DBZ-7816)
* Abnormal Behavior in Debezium Monitoring Example - mysql connector [DBZ-7826](https://issues.redhat.com/browse/DBZ-7826)
* DEBEZIUM_VERSION is wrongly set to 2.6.0.Alpha1  [DBZ-7827](https://issues.redhat.com/browse/DBZ-7827)
* Sql Server incorrectly applying quoted snapshot statement overrides [DBZ-7828](https://issues.redhat.com/browse/DBZ-7828)
* Debezium JDBC Sink not handle order correctly [DBZ-7830](https://issues.redhat.com/browse/DBZ-7830)
* Fix typo in documentation/modules doc [DBZ-7844](https://issues.redhat.com/browse/DBZ-7844)
* Support Oracle DDL Alter Audit Policy [DBZ-7864](https://issues.redhat.com/browse/DBZ-7864)
* Support Oracle DDL Create Audit Policy [DBZ-7865](https://issues.redhat.com/browse/DBZ-7865)


### Other changes since 2.7.0.Alpha1

* Log exception details early in case MySQL keep-alive causes deadlock on shutdown [DBZ-7570](https://issues.redhat.com/browse/DBZ-7570)
* Extend mongodb system tests with ssl option [DBZ-7605](https://issues.redhat.com/browse/DBZ-7605)
* Refactor oracle connector test job [DBZ-7807](https://issues.redhat.com/browse/DBZ-7807)
* Fix anchor ID collisions that prevent downstream documentation from building [DBZ-7815](https://issues.redhat.com/browse/DBZ-7815)
* Add c3p0 timeout configuration example to JDBC sink [DBZ-7822](https://issues.redhat.com/browse/DBZ-7822)
* Move undocumented option to internal [DBZ-7833](https://issues.redhat.com/browse/DBZ-7833)
* Increase wait for shouldGracefullySkipObjectBasedTables on XStream [DBZ-7839](https://issues.redhat.com/browse/DBZ-7839)
* Bump Outbox Extension to Quarkus 3.10.0 [DBZ-7842](https://issues.redhat.com/browse/DBZ-7842)
* in the Cassandra documentation, there is a typo which should have been disable not Dusable. [DBZ-7851](https://issues.redhat.com/browse/DBZ-7851)



## 2.7.0.Alpha1
April 25th 2024 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12425451)

### New features since 2.6.0.Final

* Support helm chart installation of debezium-operator [DBZ-7116](https://issues.redhat.com/browse/DBZ-7116)
* Adding additional-conditions into Incremental Snapshot to MongoDB [DBZ-7138](https://issues.redhat.com/browse/DBZ-7138)
* Document MongoDB connector inactivity pause and it's performance implications [DBZ-7147](https://issues.redhat.com/browse/DBZ-7147)
* Move MariaDB connector from MySQL to its own separate connector [DBZ-7693](https://issues.redhat.com/browse/DBZ-7693)
* Mongodb Delete events should have `_id` in the payload [DBZ-7695](https://issues.redhat.com/browse/DBZ-7695)
* Provide option to encode ordering metadata in each record [DBZ-7698](https://issues.redhat.com/browse/DBZ-7698)
* Manage escaping when captured table are determined for snapshot [DBZ-7718](https://issues.redhat.com/browse/DBZ-7718)
* Performance improve in KafkaRecordEmitter class [DBZ-7722](https://issues.redhat.com/browse/DBZ-7722)
* Introduce `RawToString` transform for converting GUIDs stored in Oracle `RAW(16)` columns to Guid string [DBZ-7753](https://issues.redhat.com/browse/DBZ-7753)
* Improve NLS character set support by including orai18n dependency [DBZ-7761](https://issues.redhat.com/browse/DBZ-7761)
* Vitess Connector should have parity with MySQL's time.precision.mode [DBZ-7773](https://issues.redhat.com/browse/DBZ-7773)
* Document potential null values in the after field for lookup full update type [DBZ-7789](https://issues.redhat.com/browse/DBZ-7789)
* Fix invalid date/timestamp check & logging level [DBZ-7811](https://issues.redhat.com/browse/DBZ-7811)


### Breaking changes since 2.6.0.Final

* Provide query timeout property to avoid indefinitely hangs during queries [DBZ-7616](https://issues.redhat.com/browse/DBZ-7616)
* Update max.iteration.transactions to a sensible default [DBZ-7750](https://issues.redhat.com/browse/DBZ-7750)


### Fixes since 2.6.0.Final

* Builtin database name filter is incorrectly applied only to collections instead of databases  in snapshot [DBZ-7485](https://issues.redhat.com/browse/DBZ-7485)
* After the initial deployment of Debezium, if a new table is added to MSSQL, its schema is was captured [DBZ-7697](https://issues.redhat.com/browse/DBZ-7697)
* The test is failing because wrong topics are used [DBZ-7715](https://issues.redhat.com/browse/DBZ-7715)
* Incremental Snapshot: read duplicate data when database has 1000 tables [DBZ-7716](https://issues.redhat.com/browse/DBZ-7716)
* Handle instability in JDBC connector system tests [DBZ-7726](https://issues.redhat.com/browse/DBZ-7726)
* SQLServerConnectorIT.shouldNotStreamWhenUsingSnapshotModeInitialOnly check an old log message [DBZ-7729](https://issues.redhat.com/browse/DBZ-7729)
* Fix MongoDB unwrap SMT test [DBZ-7731](https://issues.redhat.com/browse/DBZ-7731)
* Snapshot fails with an error of invalid lock [DBZ-7732](https://issues.redhat.com/browse/DBZ-7732)
* Column CON_ID queried on V$THREAD is not available in Oracle 11 [DBZ-7737](https://issues.redhat.com/browse/DBZ-7737)
* Redis NOAUTH Authentication Error when DB index is specified [DBZ-7740](https://issues.redhat.com/browse/DBZ-7740)
* Getting oldest transaction in Oracle buffer can cause NoSuchElementException with Infinispan [DBZ-7741](https://issues.redhat.com/browse/DBZ-7741)
* The MySQL Debezium connector is not doing the snapshot after the reset. [DBZ-7743](https://issues.redhat.com/browse/DBZ-7743)
* MongoDb connector doesn't work with Load Balanced cluster [DBZ-7744](https://issues.redhat.com/browse/DBZ-7744)
* Align unwrap tests to respect AT LEAST ONCE delivery [DBZ-7746](https://issues.redhat.com/browse/DBZ-7746)
* Exclude reload4j from Kafka connect dependencies in system testsuite [DBZ-7748](https://issues.redhat.com/browse/DBZ-7748)
* Pod Security Context not set from template [DBZ-7749](https://issues.redhat.com/browse/DBZ-7749)
* Apply MySQL binlog client version 0.29.1 - bugfix: read long value when deserializing gtid transaction's length [DBZ-7757](https://issues.redhat.com/browse/DBZ-7757)
* Change streaming exceptions are swallowed by BufferedChangeStreamCursor [DBZ-7759](https://issues.redhat.com/browse/DBZ-7759)
* Sql-Server connector fails after initial start / processed record on subsequent starts [DBZ-7765](https://issues.redhat.com/browse/DBZ-7765)
* Valid resume token is considered invalid which leads to new snapshot with some snapshot modes [DBZ-7770](https://issues.redhat.com/browse/DBZ-7770)
* NO_DATA snapshot mode validation throw DebeziumException on restarts if snapshot is not completed [DBZ-7780](https://issues.redhat.com/browse/DBZ-7780)
* DDL statement couldn't be parsed [DBZ-7788](https://issues.redhat.com/browse/DBZ-7788)
* old class reference in ibmi-connector services [DBZ-7795](https://issues.redhat.com/browse/DBZ-7795)
* Documentation for Debezium Scripting mentions wrong property [DBZ-7798](https://issues.redhat.com/browse/DBZ-7798)


### Other changes since 2.6.0.Final

* Update documenation for embedded engine [DBZ-7632](https://issues.redhat.com/browse/DBZ-7632)
* Implement basic JHM perf. tests for async engine [DBZ-7633](https://issues.redhat.com/browse/DBZ-7633)
* Upgrade Debezium Quarkus Outbox to Quarkus 3.9.2 [DBZ-7663](https://issues.redhat.com/browse/DBZ-7663)
* Move LogPositionValidator outside the JdbcConnection [DBZ-7717](https://issues.redhat.com/browse/DBZ-7717)
* Fix mongodb image in system tests [DBZ-7739](https://issues.redhat.com/browse/DBZ-7739)
* Refactor exporting to CloudEvents [DBZ-7755](https://issues.redhat.com/browse/DBZ-7755)
* Use thread cap only for deault value [DBZ-7763](https://issues.redhat.com/browse/DBZ-7763)
* Evaluate cached thread pool as the default option for async embedded engine [DBZ-7764](https://issues.redhat.com/browse/DBZ-7764)
* Create JMH benchmark for engine record processing [DBZ-7776](https://issues.redhat.com/browse/DBZ-7776)
* Improve processing speed of async engine processors which use List#get() [DBZ-7777](https://issues.redhat.com/browse/DBZ-7777)
* Disable renovate in debezium-ui [DBZ-7814](https://issues.redhat.com/browse/DBZ-7814)



## 2.6.0.Final
April 2nd 2024 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12425282)

### New features since 2.6.0.CR1

* Add documentation for Cassandra conncetor event.order.guarantee.mode property [DBZ-7720](https://issues.redhat.com/browse/DBZ-7720)


### Breaking changes since 2.6.0.CR1

None


### Fixes since 2.6.0.CR1

* JDBC Storage does not support connection recovery [DBZ-7258](https://issues.redhat.com/browse/DBZ-7258)
* Full incremental snapshot on SQL Server Table skipping block of 36 records [DBZ-7359](https://issues.redhat.com/browse/DBZ-7359)
* Snapshot skipping records [DBZ-7585](https://issues.redhat.com/browse/DBZ-7585)
* AsyncEmbeddedEngine doesn't shut down threads properly [DBZ-7661](https://issues.redhat.com/browse/DBZ-7661)
* RedisSchemaHistoryIT fails randomly [DBZ-7692](https://issues.redhat.com/browse/DBZ-7692)
* RedisSchemaHistoryIT#testRedisConnectionRetry can run into infinite retry loop [DBZ-7701](https://issues.redhat.com/browse/DBZ-7701)
* Fix system tests error when using Kafka 3.6.0 or less. [DBZ-7708](https://issues.redhat.com/browse/DBZ-7708)
* Adjust fakeDNS starting to work both on Docker Desktop and Podman Desktop [DBZ-7711](https://issues.redhat.com/browse/DBZ-7711)
* Fix mysql and postgresql system test assert failures [DBZ-7713](https://issues.redhat.com/browse/DBZ-7713)
* Fix errors when runing system testsuite with mysql and jdbc tests together [DBZ-7714](https://issues.redhat.com/browse/DBZ-7714)
* whitespace in filename of debezium-connector-ibmi [DBZ-7721](https://issues.redhat.com/browse/DBZ-7721)


### Other changes since 2.6.0.CR1

* Create Debezium design document for new implementation of DebeziumEngine [DBZ-7073](https://issues.redhat.com/browse/DBZ-7073)
* Provide a generic snapshot mode configurable via connector properties [DBZ-7497](https://issues.redhat.com/browse/DBZ-7497)
* Create JDBC sink connector system tests [DBZ-7592](https://issues.redhat.com/browse/DBZ-7592)
* Bump up versions of dependencies in system testsuite [DBZ-7630](https://issues.redhat.com/browse/DBZ-7630)
* Align snapshot modes for Informix [DBZ-7699](https://issues.redhat.com/browse/DBZ-7699)
* Add tag for system test mongodb sharded replica_set mode [DBZ-7706](https://issues.redhat.com/browse/DBZ-7706)
* Remove unneeded records copying from RecordProcessors [DBZ-7710](https://issues.redhat.com/browse/DBZ-7710)
* Example-mongodb image - fix init script for images with base mongo:6.0 [DBZ-7712](https://issues.redhat.com/browse/DBZ-7712)
* Remove dependency for mysql-connector test-jar in Redis tests [DBZ-7723](https://issues.redhat.com/browse/DBZ-7723)



## 2.6.0.CR1
March 25th 2024 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12423730)

### New features since 2.6.0.Beta1

* Add XML support for OpenLogReplicator [DBZ-6896](https://issues.redhat.com/browse/DBZ-6896)
* Use TRACE level log for Debezium Server in build time [DBZ-7369](https://issues.redhat.com/browse/DBZ-7369)
* Implement Versioned interfaces in Transformation and Converter plugins [DBZ-7618](https://issues.redhat.com/browse/DBZ-7618)
* Performance Issue in Cassandra Connector [DBZ-7622](https://issues.redhat.com/browse/DBZ-7622)
* Provide partition mode to guarantee order of events in same partition [DBZ-7631](https://issues.redhat.com/browse/DBZ-7631)
* Support empty debezium.sink.redis.user and debezium.sink.redis.password [DBZ-7646](https://issues.redhat.com/browse/DBZ-7646)


### Breaking changes since 2.6.0.Beta1

* Deploying Debezium for the first time, it not captures the schema of all tables in the database. [DBZ-7593](https://issues.redhat.com/browse/DBZ-7593)
* Switch ts_ms from BEGIN timestamp to COMMIT timestamp for row events [DBZ-7628](https://issues.redhat.com/browse/DBZ-7628)
* Bump MySQL driver from 8.0.33 to 8.3.0 [DBZ-7652](https://issues.redhat.com/browse/DBZ-7652)


### Fixes since 2.6.0.Beta1

* Log Mining Processor advances SCN incorrectly if LogMiner query returns no rows [DBZ-6679](https://issues.redhat.com/browse/DBZ-6679)
* Oracle connector unable to find SCN after Exadata maintenance updates [DBZ-7389](https://issues.redhat.com/browse/DBZ-7389)
* Oracle LOB requery on Primary Key change does not work for all column types [DBZ-7458](https://issues.redhat.com/browse/DBZ-7458)
* Incorrect value of TIME(n) replicate from MySQL if the original value is negative [DBZ-7594](https://issues.redhat.com/browse/DBZ-7594)
* Re-select Post Processor not working for complex types [DBZ-7596](https://issues.redhat.com/browse/DBZ-7596)
* Null instead of toast placeholder written for binary types when "hex" mode configured [DBZ-7599](https://issues.redhat.com/browse/DBZ-7599)
* Poor snapshot performance during schema snapshot DDL processing [DBZ-7608](https://issues.redhat.com/browse/DBZ-7608)
* Re-select post processor performance [DBZ-7611](https://issues.redhat.com/browse/DBZ-7611)
* Uncaught exception during config validation in Engine [DBZ-7614](https://issues.redhat.com/browse/DBZ-7614)
* Enhanced event timestamp precision combined with ExtractNewRecordState not working [DBZ-7615](https://issues.redhat.com/browse/DBZ-7615)
* Incremental snapshot query doesn't honor message.key.columns order [DBZ-7617](https://issues.redhat.com/browse/DBZ-7617)
* Metric ScnFreezeCount never increases [DBZ-7619](https://issues.redhat.com/browse/DBZ-7619)
* JDBC connector does not process ByteBuffer field value [DBZ-7620](https://issues.redhat.com/browse/DBZ-7620)
* Cassandra can have misaligned Jackson dependencies [DBZ-7629](https://issues.redhat.com/browse/DBZ-7629)
* Numerci value without mantissa cannot be parsed [DBZ-7643](https://issues.redhat.com/browse/DBZ-7643)
* Missing test annotation in PostgresConnectorIT [DBZ-7649](https://issues.redhat.com/browse/DBZ-7649)
* Update QOSDK and Quarkus to fix vcs-url annotation  CVE [DBZ-7664](https://issues.redhat.com/browse/DBZ-7664)
* MySQL connector fails to parse DDL with RETURNING keyword [DBZ-7666](https://issues.redhat.com/browse/DBZ-7666)
* Schema history comparator doesn't handle SERVER_ID_KEY and TIMESTAMP_KEY properly [DBZ-7690](https://issues.redhat.com/browse/DBZ-7690)
* Duplicate envar generated in operator bundle [DBZ-7703](https://issues.redhat.com/browse/DBZ-7703)


### Other changes since 2.6.0.Beta1

* debezium-connector-jdbc occurred  java.sql.SQLException: ORA-01461: can bind a LONG value only [DBZ-6900](https://issues.redhat.com/browse/DBZ-6900)
* Align snapshot modes for MongoDB [DBZ-7304](https://issues.redhat.com/browse/DBZ-7304)
* Align snapshot modes for DB2 [DBZ-7305](https://issues.redhat.com/browse/DBZ-7305)
* Align all snapshot mode on all connectors [DBZ-7308](https://issues.redhat.com/browse/DBZ-7308)
* Remove LogMiner continuous mining configuration option [DBZ-7610](https://issues.redhat.com/browse/DBZ-7610)
* Update Quarkus Outbox to Quarkus 3.8.2 [DBZ-7623](https://issues.redhat.com/browse/DBZ-7623)
* Upgrade Debezium Server to Quarkus 3.2.10 [DBZ-7624](https://issues.redhat.com/browse/DBZ-7624)
* MongoDbReplicaSet and MongoDbShardedCluster should not create a new network for each builder instance by default [DBZ-7626](https://issues.redhat.com/browse/DBZ-7626)
* Remove forgotten lombok code from system tests [DBZ-7634](https://issues.redhat.com/browse/DBZ-7634)
* Add JDBC connector to artifact server image preparation [DBZ-7644](https://issues.redhat.com/browse/DBZ-7644)
* Revert removal of Oracle LogMiner continuous mining [DBZ-7645](https://issues.redhat.com/browse/DBZ-7645)
* Add documentation for MongoDB capture.mode.full.update.type property [DBZ-7647](https://issues.redhat.com/browse/DBZ-7647)
* Fix MySQL image fetch for tests [DBZ-7651](https://issues.redhat.com/browse/DBZ-7651)
* RedisSchemaHistoryIT continually fails [DBZ-7654](https://issues.redhat.com/browse/DBZ-7654)
* Upgrade Quarkus Outbox Extension to Quarkus 3.8.3 [DBZ-7656](https://issues.redhat.com/browse/DBZ-7656)
* Bump SQL Server test image to SQL Server 2022 [DBZ-7657](https://issues.redhat.com/browse/DBZ-7657)
* Upgrade Debezium Server to Quarkus 3.2.11.Final [DBZ-7662](https://issues.redhat.com/browse/DBZ-7662)
* Exclude jcl-over-slf4j dependency [DBZ-7665](https://issues.redhat.com/browse/DBZ-7665)



## 2.6.0.Beta1
March 6th 2024 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12423016)

### New features since 2.6.0.Alpha2

* DB2/AS400 CDC using free jt400 library [DBZ-2002](https://issues.redhat.com/browse/DBZ-2002)
* Use row value constructors to speed up multi-column queries for incremental snapshots [DBZ-5071](https://issues.redhat.com/browse/DBZ-5071)
* Add metadata to watermarking signals [DBZ-6858](https://issues.redhat.com/browse/DBZ-6858)
* Provide the Redo SQL as part of the change event [DBZ-6960](https://issues.redhat.com/browse/DBZ-6960)
* Introduce a new microsecond/nanosecond precision timestamp in envelope [DBZ-7107](https://issues.redhat.com/browse/DBZ-7107)
* Append LSN to txID [DBZ-7454](https://issues.redhat.com/browse/DBZ-7454)
* Defer transaction capture until the first DML event occurs [DBZ-7473](https://issues.redhat.com/browse/DBZ-7473)
* Support arbitrary payloads with outbox event router on debezium server [DBZ-7512](https://issues.redhat.com/browse/DBZ-7512)
* Allow XStream error ORA-23656 to be retried [DBZ-7559](https://issues.redhat.com/browse/DBZ-7559)
* Upgrade PostgreSQL driver to 42.6.1 [DBZ-7571](https://issues.redhat.com/browse/DBZ-7571)
* Improved logging in case of PostgreSQL failure [DBZ-7581](https://issues.redhat.com/browse/DBZ-7581)


### Breaking changes since 2.6.0.Alpha2

* Include ojdbc8 driver with Oracle connector distribution [DBZ-7364](https://issues.redhat.com/browse/DBZ-7364)
* Avoid overriding MAVEN_DEP_DESTINATION in connect image  [DBZ-7551](https://issues.redhat.com/browse/DBZ-7551)


### Fixes since 2.6.0.Alpha2

* PostgreSQL connector doesn't restart properly if database if not reachable [DBZ-6236](https://issues.redhat.com/browse/DBZ-6236)
* NullPointerException in MongoDB connector [DBZ-6434](https://issues.redhat.com/browse/DBZ-6434)
* Cassandra-4: Debezium connector stops producing events after a schema change [DBZ-7363](https://issues.redhat.com/browse/DBZ-7363)
* Callout annotations rendered multiple times in downstream User Guide [DBZ-7418](https://issues.redhat.com/browse/DBZ-7418)
* PreparedStatement leak in Oracle ReselectColumnsProcessor [DBZ-7479](https://issues.redhat.com/browse/DBZ-7479)
* Allow special characters in signal table name [DBZ-7480](https://issues.redhat.com/browse/DBZ-7480)
* Poor snapshot performance with new reselect SMT [DBZ-7488](https://issues.redhat.com/browse/DBZ-7488)
* Debezium Oracle Connector ParsingException on XMLTYPE with lob.enabled=true [DBZ-7489](https://issues.redhat.com/browse/DBZ-7489)
* Db2ReselectColumnsProcessorIT does not clean-up after test failures [DBZ-7491](https://issues.redhat.com/browse/DBZ-7491)
* Completion callback called before connector stop [DBZ-7496](https://issues.redhat.com/browse/DBZ-7496)
* Fix MySQL 8 event timestamp resolution logic error where fallback to seconds occurs erroneously for non-GTID events [DBZ-7500](https://issues.redhat.com/browse/DBZ-7500)
* Remove incubating from Debezium documentation [DBZ-7501](https://issues.redhat.com/browse/DBZ-7501)
* LogMinerHelperIT test shouldAddCorrectLogFiles randomly fails [DBZ-7504](https://issues.redhat.com/browse/DBZ-7504)
* MySQl ReadOnlyIncrementalSnapshotIT testStopSnapshotKafkaSignal fails randomly [DBZ-7508](https://issues.redhat.com/browse/DBZ-7508)
* Multi-threaded snapshot can enqueue changes out of order [DBZ-7534](https://issues.redhat.com/browse/DBZ-7534)
* AsyncEmbeddedEngineTest#testTasksAreStoppedIfSomeFailsToStart fails randomly [DBZ-7535](https://issues.redhat.com/browse/DBZ-7535)
* MongoDbReplicaSetAuthTest fails randomly [DBZ-7537](https://issues.redhat.com/browse/DBZ-7537)
* ReadOnlyIncrementalSnapshotIT#testStopSnapshotKafkaSignal fails randomly [DBZ-7553](https://issues.redhat.com/browse/DBZ-7553)
* Wait for Redis server to start [DBZ-7564](https://issues.redhat.com/browse/DBZ-7564)
* Fix null event timestamp possible from FORMAT_DESCRIPTION and PREVIOUS_GTIDS events in MySqlStreamingChangeEventSource::setEventTimestamp [DBZ-7567](https://issues.redhat.com/browse/DBZ-7567)
* AsyncEmbeddedEngineTest.testExecuteSmt fails randomly [DBZ-7568](https://issues.redhat.com/browse/DBZ-7568)
* Debezium fails to compile with JDK 21 [DBZ-7569](https://issues.redhat.com/browse/DBZ-7569)
* Redis tests fail randomly with JedisConnectionException: Unexpected end of stream [DBZ-7576](https://issues.redhat.com/browse/DBZ-7576)
* RedisOffsetIT.testRedisConnectionRetry fails randomly [DBZ-7578](https://issues.redhat.com/browse/DBZ-7578)
* Unavailable Toasted HSTORE Json Storage Mode column causes serialization failure [DBZ-7582](https://issues.redhat.com/browse/DBZ-7582)
* Oracle Connector REST Extension Tests Fail [DBZ-7597](https://issues.redhat.com/browse/DBZ-7597)
* Serialization of XML columns with NULL values fails using Infinispan Buffer [DBZ-7598](https://issues.redhat.com/browse/DBZ-7598)


### Other changes since 2.6.0.Alpha2

* MySQL config values validated twice [DBZ-2015](https://issues.redhat.com/browse/DBZ-2015)
* Implement Hybrid Mining Strategy for Oracle, seamless DDL tracking with online catalog performance [DBZ-3401](https://issues.redhat.com/browse/DBZ-3401)
* Tests in RHEL system testsuite throw errors without ocp cluster [DBZ-7002](https://issues.redhat.com/browse/DBZ-7002)
* Move timeout configuration of MongoDbReplicaSet into Builder class [DBZ-7054](https://issues.redhat.com/browse/DBZ-7054)
* Several Oracle tests fail regularly on Testing Farm infrastructure [DBZ-7072](https://issues.redhat.com/browse/DBZ-7072)
* Remove obsolete MySQL version from TF [DBZ-7173](https://issues.redhat.com/browse/DBZ-7173)
* Add Oracle 23 to CI test matrix [DBZ-7195](https://issues.redhat.com/browse/DBZ-7195)
* Refactor sharded mongo ocp test [DBZ-7221](https://issues.redhat.com/browse/DBZ-7221)
* Implement Snapshotter SPI Oracle [DBZ-7302](https://issues.redhat.com/browse/DBZ-7302)
* Align snapshot modes for SQLServer [DBZ-7303](https://issues.redhat.com/browse/DBZ-7303)
* Update snapshot mode documentation [DBZ-7309](https://issues.redhat.com/browse/DBZ-7309)
* Upgrade ojdbc8 to 21.11.0.0 [DBZ-7365](https://issues.redhat.com/browse/DBZ-7365)
* Document relation between column type and serializers for outbox [DBZ-7368](https://issues.redhat.com/browse/DBZ-7368)
* Test testEmptyChangesProducesHeartbeat tends to fail randomly [DBZ-7453](https://issues.redhat.com/browse/DBZ-7453)
* Align snapshot modes for PostgreSQL, MySQL, Oracle [DBZ-7461](https://issues.redhat.com/browse/DBZ-7461)
* Document toggling MariaDB mode  [DBZ-7487](https://issues.redhat.com/browse/DBZ-7487)
* Add informix to main repository CI workflow [DBZ-7490](https://issues.redhat.com/browse/DBZ-7490)
* Disable Oracle Integration Tests on GitHub [DBZ-7494](https://issues.redhat.com/browse/DBZ-7494)
* Unify and adjust thread time outs [DBZ-7495](https://issues.redhat.com/browse/DBZ-7495)
* Add "IF [NOT] EXISTS" DDL support for Oracle 23 [DBZ-7498](https://issues.redhat.com/browse/DBZ-7498)
* Deployment examples show attribute name instead of its value [DBZ-7499](https://issues.redhat.com/browse/DBZ-7499)
* Add ability to parse Map<String, Object> into ConfigProperties [DBZ-7503](https://issues.redhat.com/browse/DBZ-7503)
* Support Oracle 23 SELECT without FROM [DBZ-7505](https://issues.redhat.com/browse/DBZ-7505)
* Add Oracle 23 Annotation support for CREATE/ALTER TABLE statements [DBZ-7506](https://issues.redhat.com/browse/DBZ-7506)
* TestContainers MongoDbReplicaSetAuthTest randomly fails [DBZ-7507](https://issues.redhat.com/browse/DBZ-7507)
* Add Informix to Java Outreach [DBZ-7510](https://issues.redhat.com/browse/DBZ-7510)
* Disable parallel record processing in DBZ server tests against Apicurio [DBZ-7515](https://issues.redhat.com/browse/DBZ-7515)
* Add Start CDC hook in Reselect Columns PostProcessor Tests [DBZ-7516](https://issues.redhat.com/browse/DBZ-7516)
* Remove the unused 'connector' parameter in the createSourceTask method in EmbeddedEngine.java [DBZ-7517](https://issues.redhat.com/browse/DBZ-7517)
* Update commons-compress to 1.26.0 [DBZ-7520](https://issues.redhat.com/browse/DBZ-7520)
* Promote JDBC sink from Incubating [DBZ-7521](https://issues.redhat.com/browse/DBZ-7521)
* Allow to download containers also from Docker Hub [DBZ-7524](https://issues.redhat.com/browse/DBZ-7524)
* Update rocketmq version [DBZ-7525](https://issues.redhat.com/browse/DBZ-7525)
* signalLogWithEscapedCharacter fails with pgoutput-decoder [DBZ-7526](https://issues.redhat.com/browse/DBZ-7526)
* Move RocketMQ dependency to debezium server [DBZ-7527](https://issues.redhat.com/browse/DBZ-7527)
* Rework shouldGenerateSnapshotAndContinueStreaming assertions to deal with parallelization [DBZ-7530](https://issues.redhat.com/browse/DBZ-7530)
* SQLServer tests taking long time due to database bad state [DBZ-7541](https://issues.redhat.com/browse/DBZ-7541)
* Explicitly import jakarta dependencies that are excluded via glassfish filter [DBZ-7545](https://issues.redhat.com/browse/DBZ-7545)
* Include RocketMQ and Redis container output into test log [DBZ-7557](https://issues.redhat.com/browse/DBZ-7557)
* Numeric default value decimal scale mismatch [DBZ-7562](https://issues.redhat.com/browse/DBZ-7562)
* Documentation conflict [DBZ-7565](https://issues.redhat.com/browse/DBZ-7565)
* Upgrade Kafka to 3.7.0 [DBZ-7574](https://issues.redhat.com/browse/DBZ-7574)
* Oracle connector always brings OLR dependencies [DBZ-7579](https://issues.redhat.com/browse/DBZ-7579)
* Correct JDBC connector dependencies [DBZ-7580](https://issues.redhat.com/browse/DBZ-7580)
* Reduce debug logs on tests  [DBZ-7588](https://issues.redhat.com/browse/DBZ-7588)
* Server SQS sink doesn't support quick profile [DBZ-7590](https://issues.redhat.com/browse/DBZ-7590)



## 2.6.0.Alpha2
February 13rd 2024 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12419774)

### New features since 2.6.0.Alpha1

* Add  Number of records captured and processed as metrics for Debezium MongoDB Connector [DBZ-6432](https://issues.redhat.com/browse/DBZ-6432)
* Add timezone conversion to metadata in Timezone Converter SMT [DBZ-7022](https://issues.redhat.com/browse/DBZ-7022)
* Create new implementation of DebeziumEngine [DBZ-7024](https://issues.redhat.com/browse/DBZ-7024)
* Error when fail converting value with internal schema [DBZ-7143](https://issues.redhat.com/browse/DBZ-7143)
* Provide alternative direct query for faster execution [DBZ-7273](https://issues.redhat.com/browse/DBZ-7273)
* MongoDb connector doesn't use post-images [DBZ-7299](https://issues.redhat.com/browse/DBZ-7299)
* Support DECFLOAT in Db2 connector [DBZ-7362](https://issues.redhat.com/browse/DBZ-7362)
* Create PubSub example for DS deployed via operator [DBZ-7370](https://issues.redhat.com/browse/DBZ-7370)
* Support connector scoped trustore/keystore for MongoDB [DBZ-7379](https://issues.redhat.com/browse/DBZ-7379)
* Put transaction id in offsets only when it's present [DBZ-7380](https://issues.redhat.com/browse/DBZ-7380)
* Replace additional rolebinding definition in kubernetes.yml with @RBACRule [DBZ-7381](https://issues.redhat.com/browse/DBZ-7381)
* Reduce size of docker image for Debezium 2.6 and up [DBZ-7385](https://issues.redhat.com/browse/DBZ-7385)
* Allow the C3P0ConnectionProvider to be customized via configuration [DBZ-7431](https://issues.redhat.com/browse/DBZ-7431)
* Need to be able to set an ordering key value [DBZ-7435](https://issues.redhat.com/browse/DBZ-7435)
* Evaluate container image size for Debezium UI served by nginx [DBZ-7447](https://issues.redhat.com/browse/DBZ-7447)
* Support UUID as document key for incremental snapshotting [DBZ-7451](https://issues.redhat.com/browse/DBZ-7451)
* Consolidate version management  [DBZ-7455](https://issues.redhat.com/browse/DBZ-7455)


### Breaking changes since 2.6.0.Alpha1

* Vitess-connector should not store GTID set in status topic [DBZ-7250](https://issues.redhat.com/browse/DBZ-7250)


### Fixes since 2.6.0.Alpha1

* Connector is getting stopped while processing bulk update(50k) records in debezium server 2.0.1.Final [DBZ-6955](https://issues.redhat.com/browse/DBZ-6955)
* Debezium fails after table split operation [DBZ-7360](https://issues.redhat.com/browse/DBZ-7360)
* Informix-Connector breaks on table with numerical default value [DBZ-7372](https://issues.redhat.com/browse/DBZ-7372)
* MSSQL wrong default values in db schema for varchar, nvarchar, char columns [DBZ-7374](https://issues.redhat.com/browse/DBZ-7374)
* Fix mysql version in mysql-replication container images [DBZ-7384](https://issues.redhat.com/browse/DBZ-7384)
* Duplicate Debezium SMT transform [DBZ-7416](https://issues.redhat.com/browse/DBZ-7416)
* Kinesis Sink Exception on PutRecord [DBZ-7417](https://issues.redhat.com/browse/DBZ-7417)
* ParsingException (MariaDB Only): alterSpec drop foreign key with 'tablename.' prefix [DBZ-7420](https://issues.redhat.com/browse/DBZ-7420)
* Poor performance with incremental snapshot with long list of tables [DBZ-7421](https://issues.redhat.com/browse/DBZ-7421)
* Oracle Snapshot mistakenly uses LogMiner Offset Loader by default [DBZ-7425](https://issues.redhat.com/browse/DBZ-7425)
* Reselect columns should source key values from after Struct when not using event-key sources [DBZ-7429](https://issues.redhat.com/browse/DBZ-7429)
* Stopwatch throw NPE when toString is called without having statistics [DBZ-7436](https://issues.redhat.com/browse/DBZ-7436)
* ReselectColumnsPostProcessor filter not use exclude predicate [DBZ-7437](https://issues.redhat.com/browse/DBZ-7437)
* Adhoc snapshots are not triggered via File channel signal when submitted before the start of the application [DBZ-7441](https://issues.redhat.com/browse/DBZ-7441)
* LogMiner batch size does not increase automatically [DBZ-7445](https://issues.redhat.com/browse/DBZ-7445)
* Reduce string creation during SQL_REDO column read [DBZ-7446](https://issues.redhat.com/browse/DBZ-7446)
* Oracle connector does not ignore reselection for excluded clob/blob columns [DBZ-7456](https://issues.redhat.com/browse/DBZ-7456)
* The expected value pattern for table.include.list does not align with the documentation [DBZ-7460](https://issues.redhat.com/browse/DBZ-7460)
* SQL Server queries with special characters fail after applying DBZ-7273 [DBZ-7463](https://issues.redhat.com/browse/DBZ-7463)
* Signals actions are not loaded for SQLServer [DBZ-7467](https://issues.redhat.com/browse/DBZ-7467)
* MySQL connector cannot parse table with WITH SYSTEM VERSIONING PARTITION BY SYSTEM_TIME [DBZ-7468](https://issues.redhat.com/browse/DBZ-7468)
* Postgres images require clang-11 [DBZ-7475](https://issues.redhat.com/browse/DBZ-7475)
* Make readiness and liveness proble timouts configurable [DBZ-7476](https://issues.redhat.com/browse/DBZ-7476)
* Snapshotter SPI wrongly loaded on Debezium Server [DBZ-7481](https://issues.redhat.com/browse/DBZ-7481)


### Other changes since 2.6.0.Alpha1

* Remove obsolete MySQL version from TF [DBZ-7173](https://issues.redhat.com/browse/DBZ-7173)
* Correctly handle METADATA records [DBZ-7176](https://issues.redhat.com/browse/DBZ-7176)
* Move Snapshotter interface to core module as SPI [DBZ-7300](https://issues.redhat.com/browse/DBZ-7300)
* Implement Snapshotter SPI MySQL/MariaDB [DBZ-7301](https://issues.redhat.com/browse/DBZ-7301)
* Update the Debezium UI repo with local development infra and readme file. [DBZ-7353](https://issues.redhat.com/browse/DBZ-7353)
* Update QOSDK to the latest version [DBZ-7361](https://issues.redhat.com/browse/DBZ-7361)
* Upstream artefact server image preparation job failing [DBZ-7371](https://issues.redhat.com/browse/DBZ-7371)
* Tests in RHEL system testsuite fail to initialize Kafka containers [DBZ-7373](https://issues.redhat.com/browse/DBZ-7373)
* Fix logging for schema only recovery mode in mysql connector [DBZ-7376](https://issues.redhat.com/browse/DBZ-7376)
* Records from snapshot delivered out of order [DBZ-7382](https://issues.redhat.com/browse/DBZ-7382)
* Upgrade json-path to 2.9.0 [DBZ-7383](https://issues.redhat.com/browse/DBZ-7383)
* Remove the use of Lombok in Debezium testsuite [DBZ-7386](https://issues.redhat.com/browse/DBZ-7386)
* Use Java 17 as compile-time dependency [DBZ-7387](https://issues.redhat.com/browse/DBZ-7387)
* Upgrade Outbox Extension to Quarkus 3.7.0 [DBZ-7388](https://issues.redhat.com/browse/DBZ-7388)
* Add dependancy update bot to the UI Repo [DBZ-7392](https://issues.redhat.com/browse/DBZ-7392)
* Fix the unit test cases [DBZ-7423](https://issues.redhat.com/browse/DBZ-7423)
* Adopt Oracle 23 to Testing Farm [DBZ-7439](https://issues.redhat.com/browse/DBZ-7439)
* Upgrade protobuf to 3.25.2 [DBZ-7442](https://issues.redhat.com/browse/DBZ-7442)
* Correct debezium.sink.pubsub.flowcontrol.* variable names in Debezium Server docs site [DBZ-7443](https://issues.redhat.com/browse/DBZ-7443)
* Upgrade Quarkus for Debezium Server to 3.2.9.Final [DBZ-7449](https://issues.redhat.com/browse/DBZ-7449)
* Fix TimescaleDbDatabaseTest to run into test container [DBZ-7452](https://issues.redhat.com/browse/DBZ-7452)
* Upgrade example-mongo image version to 6.0 [DBZ-7457](https://issues.redhat.com/browse/DBZ-7457)
* Test Db2ReselectColumnsProcessorIT randomly fails [DBZ-7471](https://issues.redhat.com/browse/DBZ-7471)



## 2.6.0.Alpha1
January 18th 2024 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12416463)

### New features since 2.5.0.Final

* Provide a public API from the connector implementations to retrieve the list of matching collections or tables based on the different include-/exclude lists [DBZ-7167](https://issues.redhat.com/browse/DBZ-7167)
* Notifications are Missing the ID field in log channel [DBZ-7249](https://issues.redhat.com/browse/DBZ-7249)
* Provide config option to customize CloudEvents.data schema name [DBZ-7284](https://issues.redhat.com/browse/DBZ-7284)
* Clarify comment on serialization of document ids [DBZ-7287](https://issues.redhat.com/browse/DBZ-7287)
* Unittest for hasCommitAlreadyBeenHandled in CommitScn Class [DBZ-7288](https://issues.redhat.com/browse/DBZ-7288)
* Oracle Infinispan abandoned trasactions minor enhancements [DBZ-7313](https://issues.redhat.com/browse/DBZ-7313)
* Add support for NEW_ROW_AND_OLD_VALUES value capture type. [DBZ-7348](https://issues.redhat.com/browse/DBZ-7348)


### Breaking changes since 2.5.0.Final

* Remove replica_set connection mode [DBZ-7260](https://issues.redhat.com/browse/DBZ-7260)
* Re-select columns should use the table's primary instead of the event's key [DBZ-7358](https://issues.redhat.com/browse/DBZ-7358)


### Fixes since 2.5.0.Final

* Empty object sent to GCP Pub/Sub after DELETE event [DBZ-7098](https://issues.redhat.com/browse/DBZ-7098)
* Debezium-ddl-parser crashes on parsing MySQL DDL statement (subquery with UNION) [DBZ-7259](https://issues.redhat.com/browse/DBZ-7259)
* Oracle DDL parsing error in PARTITION REFERENCE [DBZ-7266](https://issues.redhat.com/browse/DBZ-7266)
* Enhance Oracle's CREATE TABLE for Multiple Table Specifications [DBZ-7286](https://issues.redhat.com/browse/DBZ-7286)
* PostgreSQL ad-hoc blocking snapshots fail when snapshot mode is "never" [DBZ-7311](https://issues.redhat.com/browse/DBZ-7311)
* Ad-hoc blocking snapshot dies with "invalid snapshot identifier" immediately after connector creation [DBZ-7312](https://issues.redhat.com/browse/DBZ-7312)
* Specifying a table include list with spaces between elements cause LogMiner queries to miss matches [DBZ-7315](https://issues.redhat.com/browse/DBZ-7315)
* Debezium heartbeat.action.query does not start before writing to WAL: part 2 [DBZ-7316](https://issues.redhat.com/browse/DBZ-7316)
* errors.max.retries is not used to stop retrying [DBZ-7342](https://issues.redhat.com/browse/DBZ-7342)
* Oracle connector is ocasionally unable to find SCN [DBZ-7345](https://issues.redhat.com/browse/DBZ-7345)
* Initial snapshot notifications should use full identifier. [DBZ-7347](https://issues.redhat.com/browse/DBZ-7347)
* MySqlJdbcSinkDataTypeConverterIT#testBooleanDataTypeMapping fails [DBZ-7355](https://issues.redhat.com/browse/DBZ-7355)


### Other changes since 2.5.0.Final

* Add service loader manifests for all Connect plugins [DBZ-7298](https://issues.redhat.com/browse/DBZ-7298)
* Update Groovy version to 4.x [DBZ-7340](https://issues.redhat.com/browse/DBZ-7340)
* Upgrade Antora to 3.1.7 [DBZ-7344](https://issues.redhat.com/browse/DBZ-7344)
* Upgrade Outbox Extension to Quarkus 3.6.5 [DBZ-7352](https://issues.redhat.com/browse/DBZ-7352)



## 2.5.0.Final
December 21st 2023 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12416251)

### New features since 2.5.0.CR1

* Support persistent history for snapshot requests for the kafka signal topic. [DBZ-7164](https://issues.redhat.com/browse/DBZ-7164)
* Change metrics endpoint of Connect REST Extensions to use the MBeanServerv directly instead of HTTP calls to the Jolokia endpoint [DBZ-7177](https://issues.redhat.com/browse/DBZ-7177)
* Metrics endpoint must handle connectors with multiple tasks (SQL Server) [DBZ-7178](https://issues.redhat.com/browse/DBZ-7178)
* Add configuration option to CloudEventsConverter to customize schema type name [DBZ-7235](https://issues.redhat.com/browse/DBZ-7235)


### Breaking changes since 2.5.0.CR1

* Guard against implicit offset invalidation caused by switch of default connection mode [DBZ-7272](https://issues.redhat.com/browse/DBZ-7272)


### Fixes since 2.5.0.CR1

* DDL GRANT statement couldn't be parsed [DBZ-7213](https://issues.redhat.com/browse/DBZ-7213)
* Debezium Oracle plugin 2.5.0 Beta does not support Oracle 11g [DBZ-7257](https://issues.redhat.com/browse/DBZ-7257)
* Error during snapshot with multiple snapshot threads will not properly abort snasphostting [DBZ-7264](https://issues.redhat.com/browse/DBZ-7264)
* MySQL RDS UPDATE queries not ignored [DBZ-7271](https://issues.redhat.com/browse/DBZ-7271)
* Leaking JDBC connections [DBZ-7275](https://issues.redhat.com/browse/DBZ-7275)
* IncrementalSnapshotCaseSensitiveIT#insertDeleteWatermarkingStrategy fails [DBZ-7276](https://issues.redhat.com/browse/DBZ-7276)
* Debezium MySQL could not parse certain grant privileges. [DBZ-7277](https://issues.redhat.com/browse/DBZ-7277)
* Add PL/SQL Parser for Create Table Memoptimize [DBZ-7279](https://issues.redhat.com/browse/DBZ-7279)
* Support for Creating EDITIONABLE or NONEDITIONABLE Packages [DBZ-7283](https://issues.redhat.com/browse/DBZ-7283)


### Other changes since 2.5.0.CR1

* Move metrics endpoint from UI backend to the Debezium Connect REST extension/s [DBZ-6764](https://issues.redhat.com/browse/DBZ-6764)
* Add PL/SQL Parser for Alter Table Memoptimize [DBZ-7268](https://issues.redhat.com/browse/DBZ-7268)
* website-builder image fails with newer bundler [DBZ-7269](https://issues.redhat.com/browse/DBZ-7269)
* Vitess connector build fails due to invalid GPG key [DBZ-7280](https://issues.redhat.com/browse/DBZ-7280)



## 2.5.0.CR1
December 14th 2023 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12416252)

### New features since 2.5.0.Beta1

* Explore BLOB support via re-selection [DBZ-4321](https://issues.redhat.com/browse/DBZ-4321)
* Use the StreamNameMapper in debezium-server-kafka [DBZ-6071](https://issues.redhat.com/browse/DBZ-6071)
* Provide INSERT/DELETE semantics for incremental snapshot watermarking [DBZ-6834](https://issues.redhat.com/browse/DBZ-6834)
* AWS SQS as sink type in Debezium standalone server [DBZ-7214](https://issues.redhat.com/browse/DBZ-7214)
* Oracle LOB to be properly ignored if lob.enabled=false [DBZ-7237](https://issues.redhat.com/browse/DBZ-7237)
* Upgrade  Kafka to 3.6.1 and ZooKeeper to 3.8.3 [DBZ-7238](https://issues.redhat.com/browse/DBZ-7238)


### Breaking changes since 2.5.0.Beta1

* When metadata is in headers, a schema name of a structure in CE `data` field is incorrect [DBZ-7216](https://issues.redhat.com/browse/DBZ-7216)
* MySQL BIT Type should have a default length 1 [DBZ-7230](https://issues.redhat.com/browse/DBZ-7230)


### Fixes since 2.5.0.Beta1

* Oracle abandoned transaction implementation bug causes OoM [DBZ-7236](https://issues.redhat.com/browse/DBZ-7236)
* Add Grammar Oracle Truncate Cluster [DBZ-7242](https://issues.redhat.com/browse/DBZ-7242)
* Length value is not removed when changing a column's type [DBZ-7251](https://issues.redhat.com/browse/DBZ-7251)
* MongoDB table/colelction snapshot notification contain incorrect offsets [DBZ-7252](https://issues.redhat.com/browse/DBZ-7252)
* Broken support for multi-namespace watching  [DBZ-7254](https://issues.redhat.com/browse/DBZ-7254)


### Other changes since 2.5.0.Beta1

* Add tracing logs to track execution time for Debezium JDBC connector  [DBZ-7217](https://issues.redhat.com/browse/DBZ-7217)
* Validate & clarify multiple archive log destination requirements for Oracle [DBZ-7218](https://issues.redhat.com/browse/DBZ-7218)
* Upgrade logback to 1.2.13 [DBZ-7232](https://issues.redhat.com/browse/DBZ-7232)



## 2.5.0.Beta1
December 4th 2023 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12416250)

### New features since 2.5.0.Alpha2

* Support for mariadb GTID [DBZ-1482](https://issues.redhat.com/browse/DBZ-1482)
* Include only certain columns in JDBC sink connector [DBZ-6636](https://issues.redhat.com/browse/DBZ-6636)
* Support native RabbitMQ Streams [DBZ-6703](https://issues.redhat.com/browse/DBZ-6703)
* Add support for partitioning with Azure EventHubs  [DBZ-6723](https://issues.redhat.com/browse/DBZ-6723)
* Enhance Notification information and more notifications for Initial Snapshots [DBZ-6878](https://issues.redhat.com/browse/DBZ-6878)
* Add handling for CDB and non-CDB / PDB in Oracle REST Extension tests [DBZ-7091](https://issues.redhat.com/browse/DBZ-7091)
* Check schema length when create value to find missed DDL by SQL_BIN_LOG=OFF [DBZ-7093](https://issues.redhat.com/browse/DBZ-7093)
* Add service account parameter to DebeziumServer CRD [DBZ-7111](https://issues.redhat.com/browse/DBZ-7111)
* Inactivity pause in MongoDB connector should be configurable [DBZ-7146](https://issues.redhat.com/browse/DBZ-7146)
* Oracle Infinispan event processor speed-up using in memory cache [DBZ-7153](https://issues.redhat.com/browse/DBZ-7153)
* Add last event process time, number of events, number of heartbeat events metrics to MongoDb connector [DBZ-7162](https://issues.redhat.com/browse/DBZ-7162)
* LogMiner ISPN event buffer recent transaction optimization [DBZ-7169](https://issues.redhat.com/browse/DBZ-7169)
* Support logical decoding from Postgres 16 stand-bys [DBZ-7181](https://issues.redhat.com/browse/DBZ-7181)
* Support MySQL 8 high resolution replication timestamps from GTID events [DBZ-7183](https://issues.redhat.com/browse/DBZ-7183)
* Use buffer queue when reading MongoDB change stream events [DBZ-7184](https://issues.redhat.com/browse/DBZ-7184)
* Cleanup event processing loop in streaming event source of MongoDB connector [DBZ-7186](https://issues.redhat.com/browse/DBZ-7186)
* Oracle Infinispan - implement support for abandoned transactions [DBZ-7192](https://issues.redhat.com/browse/DBZ-7192)
* Add ability to avoid throwing an exception for missing additional fields  [DBZ-7197](https://issues.redhat.com/browse/DBZ-7197)
* XStream attach should be retriable [DBZ-7207](https://issues.redhat.com/browse/DBZ-7207)


### Breaking changes since 2.5.0.Alpha2

* MongoDB data collection filter requires replica set specification on blocking/initial snapshot execution [DBZ-7139](https://issues.redhat.com/browse/DBZ-7139)
* Remove deprecated ComputePartition SMT [DBZ-7141](https://issues.redhat.com/browse/DBZ-7141)
* JDBC connector wrongly uses default value when value is NULL on optional fields [DBZ-7191](https://issues.redhat.com/browse/DBZ-7191)


### Fixes since 2.5.0.Alpha2

* Test Avro adjustment for MongoDb connector and ExtractNewDocumentState SMT [DBZ-6809](https://issues.redhat.com/browse/DBZ-6809)
* The DefaultDeleteHandlingStrategy couldn't add the rewrite "__deleted" field to a non-struct value  [DBZ-7066](https://issues.redhat.com/browse/DBZ-7066)
* Debezium server has no default for offset.flush.interval.ms  [DBZ-7099](https://issues.redhat.com/browse/DBZ-7099)
*  Failed to authenticate to the MySQL database after snapshot [DBZ-7132](https://issues.redhat.com/browse/DBZ-7132)
* Failure reading CURRENT_TIMESTAMP on Informix 12.10 [DBZ-7137](https://issues.redhat.com/browse/DBZ-7137)
* Debezium-ddl-parser crashes on parsing MySQL DDL statement (specific UNION) [DBZ-7140](https://issues.redhat.com/browse/DBZ-7140)
* outbox.EventRouter SMT throws NullPointerException when there is a whitespace in fields.additional.placement value [DBZ-7142](https://issues.redhat.com/browse/DBZ-7142)
* Debezium-ddl-parser crashes on parsing MySQL DDL statement (specific UPDATE) [DBZ-7152](https://issues.redhat.com/browse/DBZ-7152)
* JsonSerialisation is unable to process changes from sharded collections with composite sharding key [DBZ-7157](https://issues.redhat.com/browse/DBZ-7157)
* Log sequence check should treat each redo thread independently [DBZ-7158](https://issues.redhat.com/browse/DBZ-7158)
* Fix DebeziumMySqlConnectorResource not using the new MySQL adatper structure to support different MySQL flavors [DBZ-7179](https://issues.redhat.com/browse/DBZ-7179)
* Parsing MySQL indexes for JSON field fails, when casting is used with types double and float [DBZ-7189](https://issues.redhat.com/browse/DBZ-7189)
* Unchanged toasted array columns  are substituted with unavailable.value.placeholder, even when REPLICA IDENTITY FULL is configured. [DBZ-7193](https://issues.redhat.com/browse/DBZ-7193)
* MongoDB streaming pauses for Blocking Snapshot only when there is no event [DBZ-7206](https://issues.redhat.com/browse/DBZ-7206)
* NPE on AbstractInfinispanLogMinerEventProcessor.logCacheStats [DBZ-7211](https://issues.redhat.com/browse/DBZ-7211)


### Other changes since 2.5.0.Alpha2

* Generate sundrio fluent builders for operator model [DBZ-6550](https://issues.redhat.com/browse/DBZ-6550)
* Convert operator source into multi module project [DBZ-6551](https://issues.redhat.com/browse/DBZ-6551)
* Implement "validate filters" endpoint in connector-specific Connect REST extensions [DBZ-6762](https://issues.redhat.com/browse/DBZ-6762)
* Implement IT tests against Cloud Spanner emulator in main repo. [DBZ-6906](https://issues.redhat.com/browse/DBZ-6906)
* Implement strategy pattern for MariaDB and MySQL differences [DBZ-7083](https://issues.redhat.com/browse/DBZ-7083)
* Run MySQL CI builds in parallel [DBZ-7135](https://issues.redhat.com/browse/DBZ-7135)
* Add matrix strategy to workflows [DBZ-7154](https://issues.redhat.com/browse/DBZ-7154)
* Add Unit Tests for ServiceAccountDependent Class in Debezium Operator Repository [DBZ-7155](https://issues.redhat.com/browse/DBZ-7155)
* Fail fast during deserialization if a value is not a CloudEvent [DBZ-7159](https://issues.redhat.com/browse/DBZ-7159)
* Correctly calculate Max LSN [DBZ-7175](https://issues.redhat.com/browse/DBZ-7175)
* Upgrade to Infinispan 14.0.20 [DBZ-7187](https://issues.redhat.com/browse/DBZ-7187)
* Upgrade Outbox Extension to Quarkus 3.5.3 [DBZ-7188](https://issues.redhat.com/browse/DBZ-7188)
* Enable ability to stream changes against Oracle 23c for LogMiner [DBZ-7194](https://issues.redhat.com/browse/DBZ-7194)
* Add modify range_partitions to modify_table_partition rule in parsing PL/SQL [DBZ-7196](https://issues.redhat.com/browse/DBZ-7196)
* Handle Drop Tablespace in PL/SQL [DBZ-7208](https://issues.redhat.com/browse/DBZ-7208)
* Upgrade logback to 1.2.12 [DBZ-7209](https://issues.redhat.com/browse/DBZ-7209)



## 2.5.0.Alpha2
November 10th 2023 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12415492)

### New features since 2.5.0.Alpha1

* JDBC Sink Connector - Support batch operations [DBZ-6317](https://issues.redhat.com/browse/DBZ-6317)
* Utilize $changeStreamSplitLargeEvent to handle large change events with post and pre images [DBZ-6726](https://issues.redhat.com/browse/DBZ-6726)
* Add support for MySQL 8.2 [DBZ-6873](https://issues.redhat.com/browse/DBZ-6873)
* Kinesis Sink Reliability [DBZ-7032](https://issues.redhat.com/browse/DBZ-7032)
* Upgrade MSSQL JDBC driver to support sensitivity classification [DBZ-7109](https://issues.redhat.com/browse/DBZ-7109)
* Add maximum retry limit to Redis Schema History [DBZ-7120](https://issues.redhat.com/browse/DBZ-7120)
* Emit a notification when completed reading from a capture instance [DBZ-7043](https://issues.redhat.com/browse/DBZ-7043)

### Breaking changes since 2.5.0.Alpha1

* Drop support for MySQL 5.7 [DBZ-6874](https://issues.redhat.com/browse/DBZ-6874)
* Rename metadata.location ->metadata.source  [DBZ-7060](https://issues.redhat.com/browse/DBZ-7060)
* Switch default connection mode to shared for sharded clusters  [DBZ-7108](https://issues.redhat.com/browse/DBZ-7108)
* Remove deprecated EmbeddedEngine interface [DBZ-7110](https://issues.redhat.com/browse/DBZ-7110)


### Fixes since 2.5.0.Alpha1

* Oracle RAC throws ORA-00310: archive log sequence required [DBZ-5350](https://issues.redhat.com/browse/DBZ-5350)
* oracle missing CDC data [DBZ-5656](https://issues.redhat.com/browse/DBZ-5656)
* Missing oracle cdc records [DBZ-5750](https://issues.redhat.com/browse/DBZ-5750)
* Connector frequently misses commit operations [DBZ-6942](https://issues.redhat.com/browse/DBZ-6942)
* Missing events from Oracle 19c [DBZ-6963](https://issues.redhat.com/browse/DBZ-6963)
* Debezium Embedded Infinispan Performs Slowly [DBZ-7047](https://issues.redhat.com/browse/DBZ-7047)
* Field exclusion does not work with events of removed fields [DBZ-7058](https://issues.redhat.com/browse/DBZ-7058)
* JDBC sink connector not working with CloudEvent [DBZ-7065](https://issues.redhat.com/browse/DBZ-7065)
* JDBC connection leak when error occurs during processing [DBZ-7069](https://issues.redhat.com/browse/DBZ-7069)
* Some server tests fail due to @com.google.inject.Inject annotation [DBZ-7077](https://issues.redhat.com/browse/DBZ-7077)
* HttpIT fails with "Unrecognized field subEvents"  [DBZ-7092](https://issues.redhat.com/browse/DBZ-7092)
* MySQL parser does not conform to arithmetical operation priorities [DBZ-7095](https://issues.redhat.com/browse/DBZ-7095)
* When RelationalBaseSourceConnector#validateConnection is called with invalid config [inside Connector#validate()] can lead to exceptions [DBZ-7105](https://issues.redhat.com/browse/DBZ-7105)
* Debezium crashes on parsing MySQL DDL statement (specific INSERT) [DBZ-7119](https://issues.redhat.com/browse/DBZ-7119)


### Other changes since 2.5.0.Alpha1

* Add (integration) tests for Oracle connector-specific Debezium Connect REST extension [DBZ-6763](https://issues.redhat.com/browse/DBZ-6763)
* Intermittent failure of MongoDbReplicaSetAuthTest [DBZ-6875](https://issues.redhat.com/browse/DBZ-6875)
* Mongodb tests in RHEL system testsuite are failing with DBZ 2.3.4 [DBZ-6996](https://issues.redhat.com/browse/DBZ-6996)
* Use DebeziumEngine instead of EmbeddedEngine in the testsuite [DBZ-7007](https://issues.redhat.com/browse/DBZ-7007)
* Update transformation property "delete.tombstone.handling.mode" to debezium doc [DBZ-7062](https://issues.redhat.com/browse/DBZ-7062)
* Add MariaDB driver for testing and distribution [DBZ-7085](https://issues.redhat.com/browse/DBZ-7085)
* Allow DS JMX to use username-password authentication on k8 [DBZ-7087](https://issues.redhat.com/browse/DBZ-7087)
* VitessConnectorIT.shouldTaskFailIfColumnNameInvalid fails [DBZ-7104](https://issues.redhat.com/browse/DBZ-7104)



## 2.5.0.Alpha1
October 26th 2023 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12410510)

### New features since 2.4.0.Final

* Provide first class support for MariaDB [DBZ-2913](https://issues.redhat.com/browse/DBZ-2913)
* Support for IBM Informix [DBZ-4999](https://issues.redhat.com/browse/DBZ-4999)
* Add support for honouring MongoDB read preference in change stream after promotion [DBZ-5953](https://issues.redhat.com/browse/DBZ-5953)
* Enable Spanner Connector against Cloud Spanner Emulator [DBZ-6845](https://issues.redhat.com/browse/DBZ-6845)
* Refactor Oracle streaming metrics beans [DBZ-6899](https://issues.redhat.com/browse/DBZ-6899)
* Provide capability to set image pull secrets in DS k8s CRD [DBZ-6962](https://issues.redhat.com/browse/DBZ-6962)
* Upgrade to Vitess 17 for integration tests [DBZ-6981](https://issues.redhat.com/browse/DBZ-6981)
* Add the ability to sanitize field name when inferencing json schema [DBZ-6983](https://issues.redhat.com/browse/DBZ-6983)
* Allow OLM Bundle scripts to download from maven central by default [DBZ-6995](https://issues.redhat.com/browse/DBZ-6995)
* Enhance README.md with Instructions for Creating a Kubernetes Namespace [DBZ-7004](https://issues.redhat.com/browse/DBZ-7004)
* Support OKD/Openshift catalog in OH release script [DBZ-7010](https://issues.redhat.com/browse/DBZ-7010)
* Add displayName and description metadata to DebeziumServer CRD in OLM Bundle [DBZ-7011](https://issues.redhat.com/browse/DBZ-7011)
* Upgrade  Kafka to 3.6.0 [DBZ-7033](https://issues.redhat.com/browse/DBZ-7033)
* DebeziumConnector always attempts to contact Quay.io to determine latest stable version [DBZ-7044](https://issues.redhat.com/browse/DBZ-7044)
* Support snapshot with automatic retry [DBZ-7050](https://issues.redhat.com/browse/DBZ-7050)
* Provide resources to set pod requests and limits in DS k8s CRD [DBZ-7052](https://issues.redhat.com/browse/DBZ-7052)
* Provide svc to better collects dbz-server metrics  in DS k8s [DBZ-7053](https://issues.redhat.com/browse/DBZ-7053)
* Improve logging at DEBUG level for Commit events [DBZ-7067](https://issues.redhat.com/browse/DBZ-7067)
* Replace schema tracking restriction for SYS/SYSTEM users with configurable option [DBZ-7071](https://issues.redhat.com/browse/DBZ-7071)


### Breaking changes since 2.4.0.Final

* Setting "none" to "delete.handle.mode" is recommended [DBZ-6907](https://issues.redhat.com/browse/DBZ-6881)
* Deprecate MongoDB 4.4 [DBZ-6907](https://issues.redhat.com/browse/DBZ-6881)


### Fixes since 2.4.0.Final

* Multiple debezium:offsets Redis clients [DBZ-6952](https://issues.redhat.com/browse/DBZ-6952)
* Wrong case-behavior for non-avro column name in sink connector [DBZ-6958](https://issues.redhat.com/browse/DBZ-6958)
* Handle properly bytea field for jdbc sink to postgresql [DBZ-6967](https://issues.redhat.com/browse/DBZ-6967)
* Debezium jdbc sink process truncate event failure [DBZ-6970](https://issues.redhat.com/browse/DBZ-6970)
* Single quote replication includes escaped quotes for N(CHAR/VARCHAR) columns [DBZ-6975](https://issues.redhat.com/browse/DBZ-6975)
* Debezium jdbc sink should throw not supporting schema change topic exception [DBZ-6990](https://issues.redhat.com/browse/DBZ-6990)
* Debezium doesn't compile with JDK 21 [DBZ-6992](https://issues.redhat.com/browse/DBZ-6992)
* OLM bundle version for GA releases is invalid [DBZ-6994](https://issues.redhat.com/browse/DBZ-6994)
* ALTER TABLE fails when adding multiple columns to JDBC sink target [DBZ-6999](https://issues.redhat.com/browse/DBZ-6999)
* Invalid Link to zulip chat in CSV metadata [DBZ-7000](https://issues.redhat.com/browse/DBZ-7000)
* Make sure to terminate the task once connectivity is lost to either the rebalance or sync topic [DBZ-7001](https://issues.redhat.com/browse/DBZ-7001)
* Missing .metadata.annotations.repository field in CSV metadata [DBZ-7003](https://issues.redhat.com/browse/DBZ-7003)
* Single quote replication and loss of data [DBZ-7006](https://issues.redhat.com/browse/DBZ-7006)
* Oracle connector: Payload size over 76020 bytes are getting truncated [DBZ-7018](https://issues.redhat.com/browse/DBZ-7018)
* DDL statement couldn't be parsed [DBZ-7030](https://issues.redhat.com/browse/DBZ-7030)
* Blocking ad-hoc snapshot is not really blocking for MySQL [DBZ-7035](https://issues.redhat.com/browse/DBZ-7035)
* Fake ROTATE event on connection restart cleans metadata [DBZ-7037](https://issues.redhat.com/browse/DBZ-7037)


### Other changes since 2.4.0.Final

* Adding Debezium Server example using MySQL and GCP PubSub [DBZ-4471](https://issues.redhat.com/browse/DBZ-4471)
* Test Debezium against MSSQL 2016 [DBZ-6693](https://issues.redhat.com/browse/DBZ-6693)
* Test Debezium against DB2 1.5.8.0 [DBZ-6694](https://issues.redhat.com/browse/DBZ-6694)
* Add MSSQL 2022 to test matrix [DBZ-6695](https://issues.redhat.com/browse/DBZ-6695)
* Edit test matrix after team evaluation [DBZ-6696](https://issues.redhat.com/browse/DBZ-6696)
* Edit test automation to run both DB2 1.5.8.0 and 1.5.0.0a [DBZ-6697](https://issues.redhat.com/browse/DBZ-6697)
* Refactor ElapsedTimeStrategy [DBZ-6778](https://issues.redhat.com/browse/DBZ-6778)
* Provide configuration option to exclude extension attributes from a CloudEvent [DBZ-6982](https://issues.redhat.com/browse/DBZ-6982)
* Further refactoring to correct downstream rendering of incremental snapshots topics [DBZ-6997](https://issues.redhat.com/browse/DBZ-6997)
* Remove deprecated embedded engine code [DBZ-7013](https://issues.redhat.com/browse/DBZ-7013)
* Enable replication slot advance check [DBZ-7015](https://issues.redhat.com/browse/DBZ-7015)
* Add configuration option to CloudEventsConverter to retrieve id and type from headers [DBZ-7016](https://issues.redhat.com/browse/DBZ-7016)
* Use optional schema for Timezone Converter tests [DBZ-7020](https://issues.redhat.com/browse/DBZ-7020)
* Debezium Operator blogpost  [DBZ-7025](https://issues.redhat.com/browse/DBZ-7025)
* Apply 2.3.4 updates to main branch [DBZ-7039](https://issues.redhat.com/browse/DBZ-7039)
* Update documentation with Postgres's pgoutput limitation [DBZ-7041](https://issues.redhat.com/browse/DBZ-7041)
* Use oracle container registry for MySQL images [DBZ-7042](https://issues.redhat.com/browse/DBZ-7042)
* Updates to fix build of downstream doc [DBZ-7046](https://issues.redhat.com/browse/DBZ-7046)
* Update operator dependencies and add qosdk platform bom [DBZ-7048](https://issues.redhat.com/browse/DBZ-7048)
* Upgrade maven-surefire-plugin to 3.1.2 [DBZ-7055](https://issues.redhat.com/browse/DBZ-7055)
* Consolidate resource labels and annotations [DBZ-7064](https://issues.redhat.com/browse/DBZ-7064)
* Disable time sync in Testing farm test runs [DBZ-7074](https://issues.redhat.com/browse/DBZ-7074)



## 2.4.0.Final
October 3rd 2023 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12411356)

### New features since 2.4.0.CR1

* Add timestamp to Notification [DBZ-6793](https://issues.redhat.com/browse/DBZ-6793)
* Add MongoDB Connector support for `filtering.match.mode=regex|literal` property [DBZ-6973](https://issues.redhat.com/browse/DBZ-6973)


### Breaking changes since 2.4.0.CR1

None


### Fixes since 2.4.0.CR1

* Debezium Outbox not working with CloudEventsConverter [DBZ-3642](https://issues.redhat.com/browse/DBZ-3642)
* Incremental snapshot data-collections are not deduplicated [DBZ-6787](https://issues.redhat.com/browse/DBZ-6787)
* MongoDB connector no longer requires cluster-wide privileges [DBZ-6888](https://issues.redhat.com/browse/DBZ-6888)
* Timezone Transformation can't work [DBZ-6940](https://issues.redhat.com/browse/DBZ-6940)
* MySQL Kafka Signalling documentation is incorrect [DBZ-6941](https://issues.redhat.com/browse/DBZ-6941)
* Infinite loop when using OR condition in additional-condition [DBZ-6956](https://issues.redhat.com/browse/DBZ-6956)
* Filter out specified DDL events logic is reverted [DBZ-6966](https://issues.redhat.com/browse/DBZ-6966)
* DDL parser does not support NOCOPY keyword [DBZ-6971](https://issues.redhat.com/browse/DBZ-6971)
* Decrease time spent in handling rebalance events [DBZ-6974](https://issues.redhat.com/browse/DBZ-6974)
* ParsingException (MySQL/MariaDB): User specification with whitespace [DBZ-6978](https://issues.redhat.com/browse/DBZ-6978)
* RecordsStreamProducerIT#shouldReceiveChangesForInfinityNumericWithInfinity fails on Postgres < 14 [DBZ-6986](https://issues.redhat.com/browse/DBZ-6986)
* PostgresConnectorIT#shouldAddNewFieldToSourceInfo may fail as the schema may not exists [DBZ-6987](https://issues.redhat.com/browse/DBZ-6987)


### Other changes since 2.4.0.CR1

* Add option to use apicurio with TLS to system level testsuite [DBZ-6954](https://issues.redhat.com/browse/DBZ-6954)
* Documentation for cursor.oversize.skip.threshold is missing units [DBZ-6968](https://issues.redhat.com/browse/DBZ-6968)



## 2.4.0.CR1
September 22nd 2023 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12413673)

### New features since 2.4.0.Beta2

* Update mongodb incremental snapshot to allow multiple threads reading chunks [DBZ-6518](https://issues.redhat.com/browse/DBZ-6518)
* Support for GKE workload identities [DBZ-6885](https://issues.redhat.com/browse/DBZ-6885)
* Support for PostgreSQL 16 [DBZ-6911](https://issues.redhat.com/browse/DBZ-6911)
* Vitess connector should retry on not found errors [DBZ-6939](https://issues.redhat.com/browse/DBZ-6939)


### Breaking changes since 2.4.0.Beta2

* Retry all status runtime exceptions by default [DBZ-6944](https://issues.redhat.com/browse/DBZ-6944)


### Fixes since 2.4.0.Beta2

* Ad-hoc blocking snaps trigger emits schema changes of all tables [DBZ-6828](https://issues.redhat.com/browse/DBZ-6828)
* When the start_scn corresponding to the existence of a transaction in V$TRANSACTION is 0, log mining starts from the oldest scn when the oracle connector is started for the first time [DBZ-6869](https://issues.redhat.com/browse/DBZ-6869)
* Ensure that the connector can handle rebalance events robustly [DBZ-6870](https://issues.redhat.com/browse/DBZ-6870)
* OpenLogReplicator confirmation can resend or omit events on restarts [DBZ-6895](https://issues.redhat.com/browse/DBZ-6895)
* ExtractNewRecordState's schema cache is not updated with arrival of the ddl change event [DBZ-6901](https://issues.redhat.com/browse/DBZ-6901)
* Misleading Debezium error message when RDI port is not specified in application.properties [DBZ-6902](https://issues.redhat.com/browse/DBZ-6902)
* Generting protobuf files to target/generated-sources breaks build [DBZ-6903](https://issues.redhat.com/browse/DBZ-6903)
* Clean log printout in Redis Debezium Sink [DBZ-6908](https://issues.redhat.com/browse/DBZ-6908)
* Values being omitted from list of JSON object [DBZ-6910](https://issues.redhat.com/browse/DBZ-6910)
* fix logger named [DBZ-6935](https://issues.redhat.com/browse/DBZ-6935)
* MySql connector get NPE when snapshot.mode is set to never and signal data collection configured [DBZ-6937](https://issues.redhat.com/browse/DBZ-6937)
* Sanity check / retry for redo logs does not work per Oracle RAC thread [DBZ-6938](https://issues.redhat.com/browse/DBZ-6938)
* Drop events has wrong table changes information [DBZ-6945](https://issues.redhat.com/browse/DBZ-6945)
* Remove spaces from Signal and Notification MBean's ObjectName [DBZ-6957](https://issues.redhat.com/browse/DBZ-6957)


### Other changes since 2.4.0.Beta2

* Migrate all examples from mongodb.hosts to mongodb.connection.string [DBZ-6893](https://issues.redhat.com/browse/DBZ-6893)



## 2.4.0.Beta2
September 13rd 2023 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12412109)

### New features since 2.4.0.Beta1

* Ingest changes via OpenLogReplicator [DBZ-2543](https://issues.redhat.com/browse/DBZ-2543)
* Only publish deltas instead of full snapshots to reduce size of sync event messages [DBZ-6458](https://issues.redhat.com/browse/DBZ-6458)
* SMT for handling timezone conversions [DBZ-6567](https://issues.redhat.com/browse/DBZ-6567)
* Support custom authentication on MongoDB connector [DBZ-6741](https://issues.redhat.com/browse/DBZ-6741)
* Document `mongodb.authentication.class` [DBZ-6788](https://issues.redhat.com/browse/DBZ-6788)
* Support truncating large columns [DBZ-6844](https://issues.redhat.com/browse/DBZ-6844)
* Always reset VStream grpc channel when max size is exceeded [DBZ-6852](https://issues.redhat.com/browse/DBZ-6852)
* Add an overview page for Connector detail [DBZ-6856](https://issues.redhat.com/browse/DBZ-6856)
* Avoid getting NPE when executing the arrived method in ExecuteSnapshot [DBZ-6865](https://issues.redhat.com/browse/DBZ-6865)
* Configurable order of user defined and internal aggregation pipeline  [DBZ-6872](https://issues.redhat.com/browse/DBZ-6872)
* Add support for MongoDB 7 [DBZ-6882](https://issues.redhat.com/browse/DBZ-6882)


### Breaking changes since 2.4.0.Beta1

* Deprecate support for MongoDB 4.0 and 4.2 [DBZ-6151](https://issues.redhat.com/browse/DBZ-6151)
* Remove deprecated mongodb.hosts and  mongodb.members.autodiscover properties [DBZ-6892](https://issues.redhat.com/browse/DBZ-6892)


### Fixes since 2.4.0.Beta1

* Documentation content section in the debezium.io scroll over to the top header. [DBZ-5942](https://issues.redhat.com/browse/DBZ-5942)
* Postgres - Incremental snapshot fails on tables with an enum type in the primary key [DBZ-6481](https://issues.redhat.com/browse/DBZ-6481)
* schema.history.internal.store.only.captured.databases.ddl flag not considered while snapshot schema to history topic [DBZ-6712](https://issues.redhat.com/browse/DBZ-6712)
* ExtractNewDocumentState for MongoDB ignore previous document state when handling delete event's with REWRITE [DBZ-6725](https://issues.redhat.com/browse/DBZ-6725)
* MongoDB New Document State Extraction: original name overriding does not work [DBZ-6773](https://issues.redhat.com/browse/DBZ-6773)
* Error with propagation source column name [DBZ-6831](https://issues.redhat.com/browse/DBZ-6831)
* Kafka offset store fails with NPE [DBZ-6853](https://issues.redhat.com/browse/DBZ-6853)
* JDBC Offset storage - configuration of table name does not work [DBZ-6855](https://issues.redhat.com/browse/DBZ-6855)
* JDBC sink insert fails with Oracle target database due to semicolon [DBZ-6857](https://issues.redhat.com/browse/DBZ-6857)
* Oracle test shouldContinueToUpdateOffsetsEvenWhenTableIsNotChanged fails with NPE [DBZ-6860](https://issues.redhat.com/browse/DBZ-6860)
* Tombstone events causes NPE on JDBC connector [DBZ-6862](https://issues.redhat.com/browse/DBZ-6862)
* Debezium-MySQL not filtering AWS RDS internal events [DBZ-6864](https://issues.redhat.com/browse/DBZ-6864)
* errors.max.retries = 0 Causes retrievable error to be ignored [DBZ-6866](https://issues.redhat.com/browse/DBZ-6866)
* Streaming aggregation pipeline broken for combination of database filter and signal collection [DBZ-6867](https://issues.redhat.com/browse/DBZ-6867)
* ChangeStream aggregation pipeline fails on large documents which should be excluded [DBZ-6871](https://issues.redhat.com/browse/DBZ-6871)
* Oracle alter table drop constraint fails when cascading index [DBZ-6876](https://issues.redhat.com/browse/DBZ-6876)


### Other changes since 2.4.0.Beta1

* Docs for Timezone SMT [DBZ-6835](https://issues.redhat.com/browse/DBZ-6835)
* Write a blog post for custom properties step in DBZ UI [DBZ-6838](https://issues.redhat.com/browse/DBZ-6838)
* Improve website/documentation artifact links [DBZ-6850](https://issues.redhat.com/browse/DBZ-6850)
* Add possibility to add on-demand adjusted testing farm execution [DBZ-6854](https://issues.redhat.com/browse/DBZ-6854)
* Oracle connector test suite logging no longer works [DBZ-6859](https://issues.redhat.com/browse/DBZ-6859)
* Increase Oracle log level to DEBUG for several key important log messages [DBZ-6880](https://issues.redhat.com/browse/DBZ-6880)
* Document cursor pipeline ordering and oversize document handling mode [DBZ-6883](https://issues.redhat.com/browse/DBZ-6883)



## 2.4.0.Beta1
August 29th 2023 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12411390)

### New features since 2.4.0.Alpha2

* Provide by DDL type schema event filtering [DBZ-6240](https://issues.redhat.com/browse/DBZ-6240)
* Add support for TimescaleDB [DBZ-6482](https://issues.redhat.com/browse/DBZ-6482)
* Max transaction duration for Oracle connector [DBZ-6615](https://issues.redhat.com/browse/DBZ-6615)
* Debezium 2.3.0.Final Missing Kafka Channel Documentation [DBZ-6688](https://issues.redhat.com/browse/DBZ-6688)
* Make the Kafka channel consumer group ID configurable for the PostgreSQL connector [DBZ-6689](https://issues.redhat.com/browse/DBZ-6689)
* Use JSON format for JMX Notification userData [DBZ-6742](https://issues.redhat.com/browse/DBZ-6742)
* Use custom RowDeserializers in case of binlog compression [DBZ-6786](https://issues.redhat.com/browse/DBZ-6786)
* Create a shardless topic naming strategy for vitess connector [DBZ-6800](https://issues.redhat.com/browse/DBZ-6800)
* JDBC sink does not support SQL Server identity inserts  [DBZ-6801](https://issues.redhat.com/browse/DBZ-6801)
* Allow the embedded infinispan global configuration to be configurable [DBZ-6808](https://issues.redhat.com/browse/DBZ-6808)
* SqlServer connector send heartbeats when there is no change in the DB [DBZ-6811](https://issues.redhat.com/browse/DBZ-6811)
* Make finished partition deletion delay configurable. [DBZ-6814](https://issues.redhat.com/browse/DBZ-6814)
* Add vcs.xml for idea [DBZ-6825](https://issues.redhat.com/browse/DBZ-6825)
* Make partial and multi-response transactions debug level logs [DBZ-6830](https://issues.redhat.com/browse/DBZ-6830)


### Breaking changes since 2.4.0.Alpha2

* Expose Oracle SCN-based metrics as Numeric rather than String values [DBZ-6798](https://issues.redhat.com/browse/DBZ-6798)


### Fixes since 2.4.0.Alpha2

* Debezium heartbeat.action.query does not start before writing to WAL. [DBZ-6635](https://issues.redhat.com/browse/DBZ-6635)
* Schema name changed with Custom topic naming strategy [DBZ-6641](https://issues.redhat.com/browse/DBZ-6641)
* Wrong behavior of quote.identifiers in JdbcSinkConnector [DBZ-6682](https://issues.redhat.com/browse/DBZ-6682)
* Toasted UUID array is not properly processed [DBZ-6720](https://issues.redhat.com/browse/DBZ-6720)
* Debezium crashes on parsing MySQL DDL statement (specific JOIN) [DBZ-6724](https://issues.redhat.com/browse/DBZ-6724)
* When using pgoutput in postgres connector, (+/-)Infinity is not supported in decimal values [DBZ-6758](https://issues.redhat.com/browse/DBZ-6758)
* Outbox transformation can cause connector to crash [DBZ-6760](https://issues.redhat.com/browse/DBZ-6760)
* MongoDB New Document State Extraction: nonexistent field for add.headers [DBZ-6774](https://issues.redhat.com/browse/DBZ-6774)
* Mongodb connector tests are massively failing when executed on 7.0-rc version [DBZ-6779](https://issues.redhat.com/browse/DBZ-6779)
* Dbz crashes on parsing MySQL DDL statement (SELECT 1.;) [DBZ-6780](https://issues.redhat.com/browse/DBZ-6780)
* Mysql connector tests are failing when executed without any profile [DBZ-6791](https://issues.redhat.com/browse/DBZ-6791)
* Dbz crashed on parsing MySQL DDL statement (SELECT 1 + @sum:=1 AS ss;) [DBZ-6794](https://issues.redhat.com/browse/DBZ-6794)
* MySQL DDL parser - REPEAT function not accepted [DBZ-6803](https://issues.redhat.com/browse/DBZ-6803)
* Fix bug with getsnapshottingtask [DBZ-6820](https://issues.redhat.com/browse/DBZ-6820)
* Dbz crashes on DDL statement (non Latin chars in variables) [DBZ-6821](https://issues.redhat.com/browse/DBZ-6821)
* Not trim the default value for the BIGINT and SMALLINT types when parsing MySQL DDL [DBZ-6824](https://issues.redhat.com/browse/DBZ-6824)
* PostgresConnectorIT#shouldAddNewFieldToSourceInfo fails randomly [DBZ-6839](https://issues.redhat.com/browse/DBZ-6839)
* Wrong filtered comments [DBZ-6840](https://issues.redhat.com/browse/DBZ-6840)
* Intermittend test failure: BaseSourceTaskTest.verifyTaskRestartsSuccessfully [DBZ-6841](https://issues.redhat.com/browse/DBZ-6841)


### Other changes since 2.4.0.Alpha2

* Upstream documentation connector config is not unified [DBZ-6704](https://issues.redhat.com/browse/DBZ-6704)
* Blocking snapshot must take snapshot configurations from signal [DBZ-6731](https://issues.redhat.com/browse/DBZ-6731)
* Documentation Request - Property File Configuration - Off-Heap Event Buffering with Embedded Infinispan [DBZ-6813](https://issues.redhat.com/browse/DBZ-6813)
* Onboard testing farm [DBZ-6827](https://issues.redhat.com/browse/DBZ-6827)
* When using `skip.messages.without.change=true` a WARN log message is reported for each record [DBZ-6843](https://issues.redhat.com/browse/DBZ-6843)



## 2.4.0.Alpha2
August 9th 2023 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12410665)

### New features since 2.4.0.Alpha1

* Switch tracing to OpenTelemetry [DBZ-2862](https://issues.redhat.com/browse/DBZ-2862)
* Update the Edit connector UI to incorporate the feedback received from team in demo [DBZ-6514](https://issues.redhat.com/browse/DBZ-6514)
* Support blocking ad-hoc snapshots [DBZ-6566](https://issues.redhat.com/browse/DBZ-6566)
* Add new parameters to RabbitMQ consumer [DBZ-6581](https://issues.redhat.com/browse/DBZ-6581)
* Document read preference changes in 2.4 [DBZ-6591](https://issues.redhat.com/browse/DBZ-6591)
* Log appropriate error when JDBC connector receive SchemaChange record  [DBZ-6655](https://issues.redhat.com/browse/DBZ-6655)
* Send tombstone events when partition queries are finished [DBZ-6658](https://issues.redhat.com/browse/DBZ-6658)
* Propagate source column name and allow sink to use it [DBZ-6684](https://issues.redhat.com/browse/DBZ-6684)
* Disable jdk-outreach-workflow.yml in forked personal repo [DBZ-6702](https://issues.redhat.com/browse/DBZ-6702)
* Support alternative JDBC drivers in MySQL connector [DBZ-6727](https://issues.redhat.com/browse/DBZ-6727)
* Add STOPPED and RESTARTING connector states to testing library [DBZ-6734](https://issues.redhat.com/browse/DBZ-6734)
* Add a new parameter for selecting the db index when using Redis Storage [DBZ-6759](https://issues.redhat.com/browse/DBZ-6759)


### Breaking changes since 2.4.0.Alpha1

* Allow packaging of multiple Cassandra distributions [DBZ-6638](https://issues.redhat.com/browse/DBZ-6638)
* Specify decimal precision in schema for MySQL unsigned bigints in precise mode [DBZ-6714](https://issues.redhat.com/browse/DBZ-6714)
* Increase Oracle default query fetch size from 2000 to 10000 [DBZ-6729](https://issues.redhat.com/browse/DBZ-6729)
* Debezium should convert _bin collate varchar columns to strings not byte arrays [DBZ-6748](https://issues.redhat.com/browse/DBZ-6748)
* Table schemas should be updated for each shard individually [DBZ-6775](https://issues.redhat.com/browse/DBZ-6775)


### Fixes since 2.4.0.Alpha1

* Connector drop down causes a scroll bar [DBZ-5421](https://issues.redhat.com/browse/DBZ-5421)
* Provide outline for drawer component showing connector details [DBZ-5831](https://issues.redhat.com/browse/DBZ-5831)
* Modify scroll for the running connector component [DBZ-5832](https://issues.redhat.com/browse/DBZ-5832)
* Connector restart regression [DBZ-6213](https://issues.redhat.com/browse/DBZ-6213)
* Document Optimal MongoDB Oplog Config for Resiliency  [DBZ-6455](https://issues.redhat.com/browse/DBZ-6455)
* JDBC Schema History: When the table name is passed as dbName.tableName, the connector does not start [DBZ-6484](https://issues.redhat.com/browse/DBZ-6484)
* Oracle DDL parser does not properly detect end of statement when comments obfuscate the semicolon [DBZ-6599](https://issues.redhat.com/browse/DBZ-6599)
* Received an unexpected message type that does not have an 'after' Debezium block [DBZ-6637](https://issues.redhat.com/browse/DBZ-6637)
* When Debezium Mongodb connector encounter authentication or under privilege errors, the connection between debezium and mongodb keeps going up. [DBZ-6643](https://issues.redhat.com/browse/DBZ-6643)
* Snapshot will not capture data when signal.data.collection is present without table.include.list [DBZ-6669](https://issues.redhat.com/browse/DBZ-6669)
* Retriable operations are retried infinitely since error handlers are not reused [DBZ-6670](https://issues.redhat.com/browse/DBZ-6670)
* Oracle DDL parser does not support column visibility on ALTER TABLE [DBZ-6677](https://issues.redhat.com/browse/DBZ-6677)
* Partition duplication after rebalances with single leader task [DBZ-6685](https://issues.redhat.com/browse/DBZ-6685)
* JDBC Sink Connector Fails on Loading Flat Data Containing Struct Type Fields from Kafka [DBZ-6686](https://issues.redhat.com/browse/DBZ-6686)
* SQLSyntaxErrorException using Debezium JDBC Sink connector [DBZ-6687](https://issues.redhat.com/browse/DBZ-6687)
* Should use topic.prefix rather than connector.server.name in MBean namings [DBZ-6690](https://issues.redhat.com/browse/DBZ-6690)
* CDC - Debezium x RabbitMQ - Debezium Server crashes when an UPDATE/DELETE on source database (PostgreSQL) [DBZ-6691](https://issues.redhat.com/browse/DBZ-6691)
* Missing operationTime field on ping command when executed against Atlas  [DBZ-6700](https://issues.redhat.com/browse/DBZ-6700)
* MongoDB SRV protocol not working in Debezium Server [DBZ-6701](https://issues.redhat.com/browse/DBZ-6701)
* Custom properties step not working correctly in validation of the properties added by user [DBZ-6711](https://issues.redhat.com/browse/DBZ-6711)
* Add tzdata-java to UI installation Dockerfile [DBZ-6713](https://issues.redhat.com/browse/DBZ-6713)
* Refactor EmbeddedEngine::run method [DBZ-6715](https://issues.redhat.com/browse/DBZ-6715)
* Oracle fails to process a DROP USER [DBZ-6716](https://issues.redhat.com/browse/DBZ-6716)
* Oracle LogMiner mining distance calculation should be skipped when upper bounds is not within distance [DBZ-6733](https://issues.redhat.com/browse/DBZ-6733)
* MariaDB: Unparseable DDL statement (ALTER TABLE IF EXISTS) [DBZ-6736](https://issues.redhat.com/browse/DBZ-6736)
* MySQL dialect does not properly recognize non-default value longblob types due to typo [DBZ-6753](https://issues.redhat.com/browse/DBZ-6753)
* Postgres tests for toasted byte array and toasted date array fail with decoderbufs plugin [DBZ-6767](https://issues.redhat.com/browse/DBZ-6767)
* Notifications and signals leaks between MBean instances when using JMX channels [DBZ-6777](https://issues.redhat.com/browse/DBZ-6777)
* Oracle XML column types are not properly resolved when adding XMLTYPE column during streaming [DBZ-6782](https://issues.redhat.com/browse/DBZ-6782)


### Other changes since 2.4.0.Alpha1

* Highlight information about how to configure the schema history topic to store data only for intended tables [DBZ-6219](https://issues.redhat.com/browse/DBZ-6219)
* Blogpost about custom signalling/notification channels [DBZ-6478](https://issues.redhat.com/browse/DBZ-6478)
* NotificationIT with Oracle xstream fails randomly [DBZ-6672](https://issues.redhat.com/browse/DBZ-6672)
* Flaky Oracle test: shouldCaptureChangesForTransactionsAcrossSnapshotBoundaryWithoutReemittingDDLChanges [DBZ-6673](https://issues.redhat.com/browse/DBZ-6673)
* Update documentation on XML and RAW data types [DBZ-6676](https://issues.redhat.com/browse/DBZ-6676)
* Use descriptive text instead of ‘-1’ in ‘Time since last event’ for no event case  [DBZ-6681](https://issues.redhat.com/browse/DBZ-6681)
* MongoDB upstream documentation duplication [DBZ-6705](https://issues.redhat.com/browse/DBZ-6705)
* Upstream documentation missing types for configurations [DBZ-6707](https://issues.redhat.com/browse/DBZ-6707)
* Exit test suite consumption loop when connector has stopped [DBZ-6730](https://issues.redhat.com/browse/DBZ-6730)
* Update Quarkus to 3.2.3.Final [DBZ-6740](https://issues.redhat.com/browse/DBZ-6740)
* Decouple Debezium Server and Extension Quarkus versions [DBZ-6744](https://issues.redhat.com/browse/DBZ-6744)
* SingleProcessor remove redundant filter logic [DBZ-6745](https://issues.redhat.com/browse/DBZ-6745)
* OracheSchemaMigrationIT fails after adding RAW data type support [DBZ-6751](https://issues.redhat.com/browse/DBZ-6751)
* Missing or misspelled IDs result in downstream build errors [DBZ-6754](https://issues.redhat.com/browse/DBZ-6754)
* Bump the MySQL binlog client version to 0.28.1 which includes significant GTID event performance improvements [DBZ-6783](https://issues.redhat.com/browse/DBZ-6783)
* Add new Redis Sink connector parameter description to the documentation [DBZ-6784](https://issues.redhat.com/browse/DBZ-6784)
* Upgrade Kafka to 3.5.1 [DBZ-6785](https://issues.redhat.com/browse/DBZ-6785)



## 2.4.0.Alpha1
July 14th 2023 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12409716)

### New features since 2.3.0.Final

* Capture & display critical connector metrics for Debezium UI [DBZ-5321](https://issues.redhat.com/browse/DBZ-5321)
* Don't require cluster-wide privileges when watching a single database/collection [DBZ-6182](https://issues.redhat.com/browse/DBZ-6182)
* Debezium Offset-Editor example application [DBZ-6338](https://issues.redhat.com/browse/DBZ-6338)
* Notify about initial snapshot progress [DBZ-6416](https://issues.redhat.com/browse/DBZ-6416)
* Set Readpreference tags in the MongoDB client  [DBZ-6468](https://issues.redhat.com/browse/DBZ-6468)
* MySqlSnapshotChangeEventSource parallel execute createSchemaEventsForTables  [DBZ-6472](https://issues.redhat.com/browse/DBZ-6472)
* Refactor errors.max.retries to common connector framework [DBZ-6573](https://issues.redhat.com/browse/DBZ-6573)
* Explain failure on existing publication update when switching to `filtered` from `all_tables`  [DBZ-6577](https://issues.redhat.com/browse/DBZ-6577)
* Debezium should honor read preference from connection string [DBZ-6578](https://issues.redhat.com/browse/DBZ-6578)
* Document support for database restricted privileges for 2.4 [DBZ-6592](https://issues.redhat.com/browse/DBZ-6592)
* Use source field in topic in table.format.name [DBZ-6595](https://issues.redhat.com/browse/DBZ-6595)
* Support authentication with TC MongoDB deployments [DBZ-6596](https://issues.redhat.com/browse/DBZ-6596)
* Support for getting primary key from header [DBZ-6602](https://issues.redhat.com/browse/DBZ-6602)
* Support for custom tags in the connector metrics [DBZ-6603](https://issues.redhat.com/browse/DBZ-6603)
* Update docs for new shard field [DBZ-6627](https://issues.redhat.com/browse/DBZ-6627)
* Improve JDBC connector documentation [DBZ-6632](https://issues.redhat.com/browse/DBZ-6632)
* Add configurable timeout to initialization procedure [DBZ-6653](https://issues.redhat.com/browse/DBZ-6653)
* Introduce internal config option to control how close to CURRENT_SCN Oracle may mine [DBZ-6660](https://issues.redhat.com/browse/DBZ-6660)
* Add support for XML_TYPE column type to Debezium connector for Oracle [DBZ-3605](https://issues.redhat.com/browse/DBZ-3605)

### Breaking changes since 2.3.0.Final

* MongoDB change stream pipeline not respecting hard coded `readPreference=secondaryPreferred` [DBZ-6521](https://issues.redhat.com/browse/DBZ-6521)
* Add shard field to events [DBZ-6617](https://issues.redhat.com/browse/DBZ-6617)


### Fixes since 2.3.0.Final

* Mysql connector fails to parse statement FLUSH FIREWALL_RULES [DBZ-3925](https://issues.redhat.com/browse/DBZ-3925)
* Snapshot result not saved if LAST record is filtered out [DBZ-5464](https://issues.redhat.com/browse/DBZ-5464)
* CloudEventsConverter throws static error on Kafka Connect 3.5+ [DBZ-6517](https://issues.redhat.com/browse/DBZ-6517)
* Dependency io.debezium:debezium-testing-testcontainers affects logback in tests [DBZ-6525](https://issues.redhat.com/browse/DBZ-6525)
* Batches with DELETE statement first will skip everything else [DBZ-6576](https://issues.redhat.com/browse/DBZ-6576)
* Oracle unsupported DDL statement - drop multiple partitions [DBZ-6585](https://issues.redhat.com/browse/DBZ-6585)
* Only Struct objects supported for [Header field insertion], found: null [DBZ-6588](https://issues.redhat.com/browse/DBZ-6588)
* Support PostgreSQL coercion for UUID, JSON, and JSONB data types [DBZ-6589](https://issues.redhat.com/browse/DBZ-6589)
* MySQL parser cannot parse CAST AS dec [DBZ-6590](https://issues.redhat.com/browse/DBZ-6590)
* Excessive Log Message 'Marking Processed Record for Topic' [DBZ-6597](https://issues.redhat.com/browse/DBZ-6597)
* Fixed DataCollections for table scan completion notificaiton [DBZ-6605](https://issues.redhat.com/browse/DBZ-6605)
* Oracle connector is not recoverable if ORA-01327 is wrapped by another JDBC or Oracle exception [DBZ-6610](https://issues.redhat.com/browse/DBZ-6610)
* Fatal error when parsing Mysql (Percona 5.7.39-42) procedure [DBZ-6613](https://issues.redhat.com/browse/DBZ-6613)
* Build of Potgres connector fails when building against Kafka 2.X [DBZ-6614](https://issues.redhat.com/browse/DBZ-6614)
* Upgrade postgresql driver to v42.6.0 [DBZ-6619](https://issues.redhat.com/browse/DBZ-6619)
* MySQL ALTER USER with RETAIN CURRENT PASSWORD fails with parsing exception [DBZ-6622](https://issues.redhat.com/browse/DBZ-6622)
* Inaccurate documentation regarding additional-condition [DBZ-6628](https://issues.redhat.com/browse/DBZ-6628)
* Oracle connection SQLRecoverableExceptions are not retried by default [DBZ-6633](https://issues.redhat.com/browse/DBZ-6633)
* Cannot delete non-null interval value [DBZ-6648](https://issues.redhat.com/browse/DBZ-6648)
* ConcurrentModificationException thrown in Debezium 2.3 [DBZ-6650](https://issues.redhat.com/browse/DBZ-6650)
* Dbz crashes on parsing Mysql Procedure Code (Statement Labels) [DBZ-6651](https://issues.redhat.com/browse/DBZ-6651)
* CloudEvents converter is broken for JSON message deserialization [DBZ-6654](https://issues.redhat.com/browse/DBZ-6654)
* Vitess: Connector fails if table name is a mysql reserved word [DBZ-6656](https://issues.redhat.com/browse/DBZ-6656)
* Junit conflicts cause by test-containers module using transitive Junit5 from quarkus [DBZ-6659](https://issues.redhat.com/browse/DBZ-6659)


### Other changes since 2.3.0.Final

* Add the API endpoint to expose running connector metrics [DBZ-5359](https://issues.redhat.com/browse/DBZ-5359)
* Display critical connector metrics [DBZ-5360](https://issues.redhat.com/browse/DBZ-5360)
* Define and document schema history topic messages schema [DBZ-5518](https://issues.redhat.com/browse/DBZ-5518)
* Align query.fetch.size across connectors [DBZ-5676](https://issues.redhat.com/browse/DBZ-5676)
* Upgrade to Apache Kafka 3.5.0 [DBZ-6047](https://issues.redhat.com/browse/DBZ-6047)
* Remove downstream related code from UI Frontend code [DBZ-6394](https://issues.redhat.com/browse/DBZ-6394)
* Make Signal actions extensible [DBZ-6417](https://issues.redhat.com/browse/DBZ-6417)
* Cleanup duplicit jobs from jenkins [DBZ-6535](https://issues.redhat.com/browse/DBZ-6535)
* Implement sharded mongo ocp deployment and integration tests  [DBZ-6538](https://issues.redhat.com/browse/DBZ-6538)
* Refactor retry handling in Redis schema history [DBZ-6594](https://issues.redhat.com/browse/DBZ-6594)
* Upgrade Quarkus to 3.2.0.Final [DBZ-6626](https://issues.redhat.com/browse/DBZ-6626)
* Upgrade kcctl to 1.0.0.Beta3 [DBZ-6642](https://issues.redhat.com/browse/DBZ-6642)
* Upgrade gRPC to 1.56.1 [DBZ-6649](https://issues.redhat.com/browse/DBZ-6649)
* Disable Kafka 2.x CRON trigger [DBZ-6667](https://issues.redhat.com/browse/DBZ-6667)



## 2.3.0.Final
June 20th 2023 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12409293)

### New features since 2.3.0.CR1

* Add support for custom SourceInfoStructMaker for adding new fields to source field [DBZ-6076](https://issues.redhat.com/browse/DBZ-6076)
* Connector can potentially read a lot of sync topic messages on startup [DBZ-6308](https://issues.redhat.com/browse/DBZ-6308)
* Allow to specify separate SID for rac.nodes settings [DBZ-6359](https://issues.redhat.com/browse/DBZ-6359)
* Periodically clean up SGA using new LogMiner connection [DBZ-6499](https://issues.redhat.com/browse/DBZ-6499)
* Upgrade debezium-connector-mysql tests to use MySQL 8 [DBZ-6534](https://issues.redhat.com/browse/DBZ-6534)
* Remove duplicate partitions in TaskSyncContext. [DBZ-6544](https://issues.redhat.com/browse/DBZ-6544)
* Support exactly-once semantic for streaming phase from Postgres connector [DBZ-6547](https://issues.redhat.com/browse/DBZ-6547)
* Monitoring failed Incremental Snapshots [DBZ-6552](https://issues.redhat.com/browse/DBZ-6552)


### Breaking changes since 2.3.0.CR1

None


### Fixes since 2.3.0.CR1

* Upgrade to Infinispan 14.0.11.Final to fix CVE-2022-45047 [DBZ-6193](https://issues.redhat.com/browse/DBZ-6193)
* Date and Time values without timezones are not persisted correctly based on database.time_zone [DBZ-6399](https://issues.redhat.com/browse/DBZ-6399)
* "Ignoring invalid task provided offset" [DBZ-6463](https://issues.redhat.com/browse/DBZ-6463)
* Oracle snapshot.include.collection.list should be prefixed with databaseName in documentation. [DBZ-6474](https://issues.redhat.com/browse/DBZ-6474)
* Allow schema to be specified in the Debezium Sink Connector configuration [DBZ-6491](https://issues.redhat.com/browse/DBZ-6491)
* Error value of negative seconds in convertOracleIntervalDaySecond [DBZ-6513](https://issues.redhat.com/browse/DBZ-6513)
* Parse mysql table name failed which ending with backslash [DBZ-6519](https://issues.redhat.com/browse/DBZ-6519)
* Oracle Connector: Snapshot fails with specific combination [DBZ-6528](https://issues.redhat.com/browse/DBZ-6528)
* Table order is incorrect on snapshots [DBZ-6533](https://issues.redhat.com/browse/DBZ-6533)
* Unhandled NullPointerException in PartitionRouting will crash the whole connect plugin [DBZ-6543](https://issues.redhat.com/browse/DBZ-6543)
* Incorrect image name in postgres example of the operator repo [DBZ-6548](https://issues.redhat.com/browse/DBZ-6548)
* Examples are not updated with correct image tags for released  [DBZ-6549](https://issues.redhat.com/browse/DBZ-6549)
* SQL grammar exception on MySQL ALTER statements with multiple columns [DBZ-6554](https://issues.redhat.com/browse/DBZ-6554)
* debezium/connect image for 2.2.1.Final is not available on dockerhub or quay.io [DBZ-6558](https://issues.redhat.com/browse/DBZ-6558)
* Bug in field.name.adjustment.mode Property [DBZ-6559](https://issues.redhat.com/browse/DBZ-6559)
* Operator sets incorrect value of transformation.predicate when no predicate is specified [DBZ-6560](https://issues.redhat.com/browse/DBZ-6560)
* Kubernetes-Config extension interferes with SSL tests due to k8 devservice starting up [DBZ-6574](https://issues.redhat.com/browse/DBZ-6574)
* MySQL read-only connector with Kafka signals enabled fails on start up [DBZ-6579](https://issues.redhat.com/browse/DBZ-6579)
* Redis schema history can fail upon startup [DBZ-6580](https://issues.redhat.com/browse/DBZ-6580)


### Other changes since 2.3.0.CR1

* Use "debezium/kafka" container for Debezium UI tests instead of "confluentinc/cp-kafka" [DBZ-6449](https://issues.redhat.com/browse/DBZ-6449)
* Include debezium operator in image build pipeline [DBZ-6546](https://issues.redhat.com/browse/DBZ-6546)
* Update repository list in contributor list and missing commit workflows [DBZ-6556](https://issues.redhat.com/browse/DBZ-6556)
* Upgrade MySQL JDBC driver to 8.0.33 [DBZ-6563](https://issues.redhat.com/browse/DBZ-6563)
* Upgrade Google Cloud BOM to 26.17.0 [DBZ-6570](https://issues.redhat.com/browse/DBZ-6570)



## 2.3.0.CR1
June 9th 2023 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12408706)

### New features since 2.3.0.Beta1

* Code Improvements for skip.messages.without.change [DBZ-6366](https://issues.redhat.com/browse/DBZ-6366)
* Allow sending signals and receiving notifications via JMX [DBZ-6424](https://issues.redhat.com/browse/DBZ-6424)
* MySql in debezium-parser-ddl does not support TABLE statement parsing [DBZ-6435](https://issues.redhat.com/browse/DBZ-6435)
* Utilize event.processing.failure.handling.mode in Vitess replication connection [DBZ-6510](https://issues.redhat.com/browse/DBZ-6510)
* Only use error processing mode on certain errors [DBZ-6523](https://issues.redhat.com/browse/DBZ-6523)
* Use better hashing function for PartitionRouting [DBZ-6529](https://issues.redhat.com/browse/DBZ-6529)
* Create PoC of Debezium Server Operator [DBZ-6493](https://issues.redhat.com/browse/DBZ-6493)


### Breaking changes since 2.3.0.Beta1

None


### Fixes since 2.3.0.Beta1

* Create OCP cluster provisioning jobs [DBZ-3129](https://issues.redhat.com/browse/DBZ-3129)
*  io.debezium.text.ParsingException: DDL statement couldn't be parsed. Please open a Jira issue with the statement [DBZ-6507](https://issues.redhat.com/browse/DBZ-6507)
* Oracle Connector failed parsing DDL Statement [DBZ-6508](https://issues.redhat.com/browse/DBZ-6508)
* FileSignalChannel is not loaded [DBZ-6509](https://issues.redhat.com/browse/DBZ-6509)
* MySqlReadOnlyIncrementalSnapshotChangeEventSource enforces Kafka dependency during initialization [DBZ-6511](https://issues.redhat.com/browse/DBZ-6511)
* Debezium incremental snapshot chunk size documentation unclear or incorrect [DBZ-6512](https://issues.redhat.com/browse/DBZ-6512)
* Debezium incremental snapshot chunk size documentation unclear or incorrect [DBZ-6515](https://issues.redhat.com/browse/DBZ-6515)
* [PostgreSQL] LTree data is not being captured by streaming [DBZ-6524](https://issues.redhat.com/browse/DBZ-6524)
* MySQL "national" keyword is not accepted as column name [DBZ-6537](https://issues.redhat.com/browse/DBZ-6537)


### Other changes since 2.3.0.Beta1

* Test Debezium on RED HAT OPENSHIFT DATABASE ACCESS - MongoDB Atlas [DBZ-5231](https://issues.redhat.com/browse/DBZ-5231)
* Add docs on how to extend channels and notification [DBZ-6408](https://issues.redhat.com/browse/DBZ-6408)
* Create Cron trigger for system tests [DBZ-6423](https://issues.redhat.com/browse/DBZ-6423)
* Debezium UI Repo dependency update  [DBZ-6473](https://issues.redhat.com/browse/DBZ-6473)
* Add Debezium Server nightly images [DBZ-6536](https://issues.redhat.com/browse/DBZ-6536)
* Include debezium operator in release scripts [DBZ-6539](https://issues.redhat.com/browse/DBZ-6539)
* Start publishing nightly images for Debezium Operator [DBZ-6541](https://issues.redhat.com/browse/DBZ-6541)
* Start releasing images for Debezium Operator [DBZ-6542](https://issues.redhat.com/browse/DBZ-6542)



## 2.3.0.Beta1
May 26th 2023 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12407588)

### New features since 2.3.0.Alpha1

* Testsuite should deploy PostgreSQL with Primary-Secondary streaming replication [DBZ-3202](https://issues.redhat.com/browse/DBZ-3202)
* PostgreSQL: Set Replica Identity when the connector starts [DBZ-6112](https://issues.redhat.com/browse/DBZ-6112)
* Correlate incremental snapshot notifications ids with execute signal [DBZ-6447](https://issues.redhat.com/browse/DBZ-6447)
* [MariaDB] Add support for userstat plugin keywords [DBZ-6459](https://issues.redhat.com/browse/DBZ-6459)
* Add a header provider string [DBZ-6489](https://issues.redhat.com/browse/DBZ-6489)


### Breaking changes since 2.3.0.Alpha1

* JDBC offset storage: Change encoding from UTF-16 to UTF-8 [DBZ-6476](https://issues.redhat.com/browse/DBZ-6476)


### Fixes since 2.3.0.Alpha1

* Debezium Server stops sending events to Google Cloud Pub/Sub [DBZ-5175](https://issues.redhat.com/browse/DBZ-5175)
* Snapshot step 5 - Reading structure of captured tables time too long  [DBZ-6439](https://issues.redhat.com/browse/DBZ-6439)
* Oracle parallel snapshots do not properly set PDB context when using multitenancy [DBZ-6457](https://issues.redhat.com/browse/DBZ-6457)
* Debezium Server cannot recover from Google Pub/Sub errors [DBZ-6461](https://issues.redhat.com/browse/DBZ-6461)
* DDL statement couldn't be parsed: AUTHENTICATION_POLICY_ADMIN [DBZ-6479](https://issues.redhat.com/browse/DBZ-6479)
* Db2 connector can fail with NPE on notification sending [DBZ-6485](https://issues.redhat.com/browse/DBZ-6485)
* BigDecimal fails when queue memory size limit is in place [DBZ-6490](https://issues.redhat.com/browse/DBZ-6490)
* ORACLE table can not be captrued, got runtime.NoViableAltException [DBZ-6492](https://issues.redhat.com/browse/DBZ-6492)
* Signal poll interval has incorrect default value [DBZ-6496](https://issues.redhat.com/browse/DBZ-6496)
* Oracle JDBC driver 23.x throws ORA-18716 - not in any time zone [DBZ-6502](https://issues.redhat.com/browse/DBZ-6502)
* Alpine postgres images should use llvm/clang 15 explicitly [DBZ-6506](https://issues.redhat.com/browse/DBZ-6506)
* ExtractNewRecordState SMT in combination with HeaderToValue SMT results in Unexpected field name exception [DBZ-6486](https://issues.redhat.com/browse/DBZ-6486)


### Other changes since 2.3.0.Alpha1

* Verify MongoDB Connector with AWS DocumentDB [DBZ-6419](https://issues.redhat.com/browse/DBZ-6419)
* Enable set log level in tests [DBZ-6460](https://issues.redhat.com/browse/DBZ-6460)
* Check OOME on CI tests [DBZ-6462](https://issues.redhat.com/browse/DBZ-6462)
* Signaling data collection document should refer to source database [DBZ-6470](https://issues.redhat.com/browse/DBZ-6470)



## 2.3.0.Alpha1
May 11st 2023 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12406007)

### New features since 2.2.0.Final

* Enable Debezium to send notifications about it's status [DBZ-1973](https://issues.redhat.com/browse/DBZ-1973)
* Saving Debezium states to JDBC database [DBZ-3621](https://issues.redhat.com/browse/DBZ-3621)
* Make signalling channel configurable [DBZ-4027](https://issues.redhat.com/browse/DBZ-4027)
* Edit a connector in Debezium UI [DBZ-5313](https://issues.redhat.com/browse/DBZ-5313)
* Add connector display name and id to Config endpoint response [DBZ-5865](https://issues.redhat.com/browse/DBZ-5865)
* Introduce LogMiner query filtering modes [DBZ-6254](https://issues.redhat.com/browse/DBZ-6254)
* Ensure that the connector can start from a stale timestamp more than one hour into the past [DBZ-6307](https://issues.redhat.com/browse/DBZ-6307)
* Add JWT authentication to HTTP Client [DBZ-6348](https://issues.redhat.com/browse/DBZ-6348)
* Monitoring progress of Incremental Snapshots [DBZ-6354](https://issues.redhat.com/browse/DBZ-6354)
* log.mining.transaction.retention.hours should reference last offset and not sysdate [DBZ-6355](https://issues.redhat.com/browse/DBZ-6355)
* Support multiple tasks when streaming shard list [DBZ-6365](https://issues.redhat.com/browse/DBZ-6365)
* Kinesis Sink - AWS Credentials Provider [DBZ-6372](https://issues.redhat.com/browse/DBZ-6372)
* Fix existing bug in information schema query in the Spanner connector [DBZ-6385](https://issues.redhat.com/browse/DBZ-6385)
* change logging level of skip.messages.without.change [DBZ-6391](https://issues.redhat.com/browse/DBZ-6391)
* Debezium UI should ignore unsupported connectors, including unsupported Debezium connectors [DBZ-6426](https://issues.redhat.com/browse/DBZ-6426)
* Make DELETE sql configurable in JDBC Storage [DBZ-6433](https://issues.redhat.com/browse/DBZ-6433)
* Include redo/archive log metadata on ORA-01291 exceptions [DBZ-6436](https://issues.redhat.com/browse/DBZ-6436)


### Breaking changes since 2.2.0.Final

* Use (and add support for) prefer as the default SSL mode on the Postgres and MySQL connectors [DBZ-6340](https://issues.redhat.com/browse/DBZ-6340)


### Fixes since 2.2.0.Final

* Back button is not working on the review page UI [DBZ-5841](https://issues.redhat.com/browse/DBZ-5841)
* Toasted varying character array and date array are not correcly processed [DBZ-6122](https://issues.redhat.com/browse/DBZ-6122)
* Incorrect dependencies in Debezium Server for Cassandra connector [DBZ-6147](https://issues.redhat.com/browse/DBZ-6147)
* Lock contention on LOG_MINING_FLUSH table when multiple connectors deployed [DBZ-6256](https://issues.redhat.com/browse/DBZ-6256)
* Document Requirements for multiple connectors on same db host [DBZ-6321](https://issues.redhat.com/browse/DBZ-6321)
* The rs_id field is null in Oracle change event source information block [DBZ-6329](https://issues.redhat.com/browse/DBZ-6329)
* Using pg_replication_slot_advance which is not supported by PostgreSQL10. [DBZ-6353](https://issues.redhat.com/browse/DBZ-6353)
* 'CREATE TABLE t (c NATIONAL CHAR)' parsing failed [DBZ-6357](https://issues.redhat.com/browse/DBZ-6357)
* Toasted hstore are not correcly processed [DBZ-6379](https://issues.redhat.com/browse/DBZ-6379)
* Snapshotting does not work for hstore in Map mode [DBZ-6384](https://issues.redhat.com/browse/DBZ-6384)
* Oracle DDL shrink space for table partition can not be parsed [DBZ-6386](https://issues.redhat.com/browse/DBZ-6386)
* __source_ts_ms r (read) operation date is set to future for SQL Server [DBZ-6388](https://issues.redhat.com/browse/DBZ-6388)
* Connector cards are misaligned on first step  [DBZ-6392](https://issues.redhat.com/browse/DBZ-6392)
* Debezium Server snapshots are not published [DBZ-6395](https://issues.redhat.com/browse/DBZ-6395)
* PostgreSQL connector task fails to resume streaming because replication slot is active [DBZ-6396](https://issues.redhat.com/browse/DBZ-6396)
* MySql in debezium-parser-ddl :The inserted sql statement reports an error [DBZ-6401](https://issues.redhat.com/browse/DBZ-6401)
* MongoDB connector crashes on invalid resume token [DBZ-6402](https://issues.redhat.com/browse/DBZ-6402)
* Set (instead of adding) Authorization Headers [DBZ-6405](https://issues.redhat.com/browse/DBZ-6405)
* New SMT HeaderToValue not working [DBZ-6411](https://issues.redhat.com/browse/DBZ-6411)
* Debezium Server 2.2.0.Final BOM refers to debezium-build-parent 2.2.0-SNAPSHOT  [DBZ-6437](https://issues.redhat.com/browse/DBZ-6437)
* NPE on read-only MySQL connector start up [DBZ-6440](https://issues.redhat.com/browse/DBZ-6440)
* Oracle Connector failed parsing DDL Statement [DBZ-6442](https://issues.redhat.com/browse/DBZ-6442)
* Oracle DDL shrink space for index partition can not be parsed [DBZ-6446](https://issues.redhat.com/browse/DBZ-6446)


### Other changes since 2.2.0.Final

* Verify streaming off of secondary works [DBZ-1661](https://issues.redhat.com/browse/DBZ-1661)
* Remove the old connector type endpoints from the UI backend [DBZ-5604](https://issues.redhat.com/browse/DBZ-5604)
* Incremental snapshot completion notifications [DBZ-5632](https://issues.redhat.com/browse/DBZ-5632)
* Change connector test matrix jobs to pipeline jobs and migrate them to gitlab jenkins [DBZ-5861](https://issues.redhat.com/browse/DBZ-5861)
* Add Debezium steps when performing a PostgreSQL database upgrade [DBZ-6046](https://issues.redhat.com/browse/DBZ-6046)
* Test migration from Debezium 1.x to 2.x [DBZ-6126](https://issues.redhat.com/browse/DBZ-6126)
* Remove OCP 4.8 and 4.9 from 1.x supported configurations page  [DBZ-6132](https://issues.redhat.com/browse/DBZ-6132)
* Remove potentially dangerous JDBC props in MySQL connections [DBZ-6157](https://issues.redhat.com/browse/DBZ-6157)
* Refactor storage implementations [DBZ-6209](https://issues.redhat.com/browse/DBZ-6209)
* Align connector field *snapshot.mode* descriptions as per documentation [DBZ-6259](https://issues.redhat.com/browse/DBZ-6259)
* Document "incubating" status of incremental snapshot for sharded MongoDB clusters [DBZ-6342](https://issues.redhat.com/browse/DBZ-6342)
* Run debezium-connector-jdbc build on 'Build Debezium' CI workflow [DBZ-6360](https://issues.redhat.com/browse/DBZ-6360)
* Migrate Debezium UI MongoDB to MongoDbReplicaSet from core [DBZ-6363](https://issues.redhat.com/browse/DBZ-6363)
* Base the "replaceable" build numbers in legacy deployment instructions on `debezium-build-number` attribute [DBZ-6371](https://issues.redhat.com/browse/DBZ-6371)
* Align Debezium UI to Debezium 2.3 [DBZ-6406](https://issues.redhat.com/browse/DBZ-6406)
* Fix CORS error in UI due to Quarkus 3 upgrade [DBZ-6422](https://issues.redhat.com/browse/DBZ-6422)
* Improve debezium-storage CI build step [DBZ-6443](https://issues.redhat.com/browse/DBZ-6443)
* Use debezium-bom versions for shared dependencies in Debezium UI [DBZ-6453](https://issues.redhat.com/browse/DBZ-6453)



## 2.2.0.Final
April 20th 2023 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12406487)

### New features since 2.2.0.CR1

* Describe Postgres example configuration for Debezium Server [DBZ-6325](https://issues.redhat.com/browse/DBZ-6325)
* Improve messages in Redis sink in case of OOM [DBZ-6346](https://issues.redhat.com/browse/DBZ-6346)
* Stream shard list for debezium vitess connector [DBZ-6356](https://issues.redhat.com/browse/DBZ-6356)


### Breaking changes since 2.2.0.CR1

None


### Fixes since 2.2.0.CR1

* If column.include.list/column.exclude.list are used and the target table receives an update for the excluded (or not included) column - such events should be ignored [DBZ-2979](https://issues.redhat.com/browse/DBZ-2979)
* Connector offsets do not advance on transaction commit with filtered events when LOB enabled [DBZ-5395](https://issues.redhat.com/browse/DBZ-5395)
* Task failure when index is made on primary columns of table. [DBZ-6238](https://issues.redhat.com/browse/DBZ-6238)
* Oracle connector doesn't need to verify redo log when snapshotting only [DBZ-6276](https://issues.redhat.com/browse/DBZ-6276)
* MySQL connector cannot parse table with SYSTEM VERSIONING [DBZ-6331](https://issues.redhat.com/browse/DBZ-6331)
* MySql in debezium-parser-ddl does not support with keyword parsing [DBZ-6336](https://issues.redhat.com/browse/DBZ-6336)
* Duplicate JMX MBean names when multiple vitess tasks running in the same JVM [DBZ-6347](https://issues.redhat.com/browse/DBZ-6347)
* KafkaSignalThread#SIGNAL_POLL_TIMEOUT_MS option duplicate signal prefix [DBZ-6361](https://issues.redhat.com/browse/DBZ-6361)


### Other changes since 2.2.0.CR1

* Complete MongoDB incremental snapshotting implementation [DBZ-4427](https://issues.redhat.com/browse/DBZ-4427)
* Add documentation for the reactive variant of the Quarkus outbox extension [DBZ-5859](https://issues.redhat.com/browse/DBZ-5859)
* Create an annotation for flaky tests [DBZ-6324](https://issues.redhat.com/browse/DBZ-6324)
* 2.1.4 post-release documentation fixes [DBZ-6351](https://issues.redhat.com/browse/DBZ-6351)



## 2.2.0.CR1
April 14th 2023 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12405777)

### New features since 2.2.0.Beta1

* Capture evenents in order across mongodb shards [DBZ-5590](https://issues.redhat.com/browse/DBZ-5590)
* Pass through configurations for kafka topics/configuration [DBZ-6262](https://issues.redhat.com/browse/DBZ-6262)
* Enable the docker tag to be configurable in the Spanner connector [DBZ-6302](https://issues.redhat.com/browse/DBZ-6302)
* Support async producer for Pulsar sink to improve performance [DBZ-6319](https://issues.redhat.com/browse/DBZ-6319)


### Breaking changes since 2.2.0.Beta1

* Upgrade to Quarkus 3.0.0.Final [DBZ-6129](https://issues.redhat.com/browse/DBZ-6129)


### Fixes since 2.2.0.Beta1

* Failed retriable operations are retried infinitely [DBZ-4488](https://issues.redhat.com/browse/DBZ-4488)
* DDL events not stored in schema history topic for excluded tables [DBZ-6070](https://issues.redhat.com/browse/DBZ-6070)
* Oracle path used current batchSize to calculate end scn is wrong, need to use min batch size [DBZ-6155](https://issues.redhat.com/browse/DBZ-6155)
* Multiplatform build of example-postres fails [DBZ-6258](https://issues.redhat.com/browse/DBZ-6258)
* Add protoc version property to postgres connector pom.xml [DBZ-6261](https://issues.redhat.com/browse/DBZ-6261)
* Postgres connector doesn't need logical WAL level when snapshotting only [DBZ-6265](https://issues.redhat.com/browse/DBZ-6265)
* MySQL connector doesn't need to query binlog when snapshotting only [DBZ-6271](https://issues.redhat.com/browse/DBZ-6271)
* Table names with spaces are not correctly deserialized when using an Infinispan cache as the transaction buffer [DBZ-6273](https://issues.redhat.com/browse/DBZ-6273)
* Transaction buffer state can become corrupted when using Infinispan cache with LOBs [DBZ-6275](https://issues.redhat.com/browse/DBZ-6275)
* DDL statement couldn't be parsed - Oracle connector 2.1.3.Final [DBZ-6314](https://issues.redhat.com/browse/DBZ-6314)
* Unparsable DDL statements (MySQL/MariaDB) [DBZ-6316](https://issues.redhat.com/browse/DBZ-6316)
* Cassandra 3 cannot be built using JDK20 [DBZ-6320](https://issues.redhat.com/browse/DBZ-6320)


### Other changes since 2.2.0.Beta1

* Upgrade dependencies (Quarkus, etc) of Debezium UI [DBZ-4109](https://issues.redhat.com/browse/DBZ-4109)
* UI- Add the UI to configure the additional properties for a connector [DBZ-5365](https://issues.redhat.com/browse/DBZ-5365)
* Upgrade UI build to use Debezium 2.2 or latest [DBZ-6173](https://issues.redhat.com/browse/DBZ-6173)
* Oracle-Connector dbz##user needs more rights [DBZ-6198](https://issues.redhat.com/browse/DBZ-6198)
* Make quay.io primary image repository [DBZ-6216](https://issues.redhat.com/browse/DBZ-6216)
* Update config properties in RHEL deployment instructions [DBZ-6266](https://issues.redhat.com/browse/DBZ-6266)
* Fix errors in downstream Getting Started guide [DBZ-6268](https://issues.redhat.com/browse/DBZ-6268)
* Address review feedback in downstream RHEL and OCP installation guides [DBZ-6272](https://issues.redhat.com/browse/DBZ-6272)
* Infinispan cache configuration used by Oracle tests are not compatible with Infinispan 14.0.2 [DBZ-6274](https://issues.redhat.com/browse/DBZ-6274)
* Remove unused/migrated jobs from upstream repository [DBZ-6299](https://issues.redhat.com/browse/DBZ-6299)
* Upgrade MySQL JDBC driver to 8.0.32 [DBZ-6304](https://issues.redhat.com/browse/DBZ-6304)
* Allow specifying docker image reference in MongoDB testcontainers implementation [DBZ-6305](https://issues.redhat.com/browse/DBZ-6305)
* Use *MongoDbContainer* instead of *MongoDBContainer* test containers class  in ConnectorConfiguration class [DBZ-6306](https://issues.redhat.com/browse/DBZ-6306)
* Add documentation for JDBC sink connector [DBZ-6310](https://issues.redhat.com/browse/DBZ-6310)
* Fix all compliance warnings for Jenkins [DBZ-6315](https://issues.redhat.com/browse/DBZ-6315)
* Remove outdated information about SYS user accounts with Oracle [DBZ-6318](https://issues.redhat.com/browse/DBZ-6318)
* Bundle Jolokia with Debezium connect image  [DBZ-6323](https://issues.redhat.com/browse/DBZ-6323)



## 2.2.0.Beta1
March 31st 2023 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12404187)

### New features since 2.2.0.Alpha3

* Debezium JDBC Sink Connector [DBZ-3647](https://issues.redhat.com/browse/DBZ-3647)
* Create an endpoint to update a connector [DBZ-5314](https://issues.redhat.com/browse/DBZ-5314)
* Refactor snapshotting to use change streams instead of oplog [DBZ-5987](https://issues.redhat.com/browse/DBZ-5987)
* Update the design for Debezium based connectors Filter step [DBZ-6060](https://issues.redhat.com/browse/DBZ-6060)
* Connect and stream from sharded clusters through mongos instances [DBZ-6170](https://issues.redhat.com/browse/DBZ-6170)
* Support Postgres dialect for Spanner Kafka Connector [DBZ-6178](https://issues.redhat.com/browse/DBZ-6178)
* Support Azure blob storage as Debezium history storage [DBZ-6180](https://issues.redhat.com/browse/DBZ-6180)
* Support Database role in Connector Config. [DBZ-6192](https://issues.redhat.com/browse/DBZ-6192)
* Remove duplicated createDdlFilter method from historized connector config [DBZ-6197](https://issues.redhat.com/browse/DBZ-6197)
* Create new SMT to copy/move header to record value [DBZ-6201](https://issues.redhat.com/browse/DBZ-6201)
* Add support for columns of type "bytea[]" - array of bytea (byte array) [DBZ-6232](https://issues.redhat.com/browse/DBZ-6232)
* Support ImageFromDockerfile with Debezium's testcontainers suite [DBZ-6244](https://issues.redhat.com/browse/DBZ-6244)
* Expose EmbeddedEngine configurations [DBZ-6248](https://issues.redhat.com/browse/DBZ-6248)
* RabbitMQ Sink [DBZ-6260](https://issues.redhat.com/browse/DBZ-6260)


### Breaking changes since 2.2.0.Alpha3

None


### Fixes since 2.2.0.Alpha3

* NPE when setting schema.history.internal.store.only.captured.tables.ddl=true [DBZ-6072](https://issues.redhat.com/browse/DBZ-6072)
* Postgres connector stuck when replication slot does not have confirmed_flush_lsn [DBZ-6092](https://issues.redhat.com/browse/DBZ-6092)
* java.lang.NullPointerException in MySQL connector with max.queue.size.in.bytes [DBZ-6104](https://issues.redhat.com/browse/DBZ-6104)
* debezium-connector-mysql failed to parse serveral DDLs of 'CREATE TABLE' [DBZ-6124](https://issues.redhat.com/browse/DBZ-6124)
* Zerofill property failed for different int types [DBZ-6185](https://issues.redhat.com/browse/DBZ-6185)
* GRANT DELETE HISTORY couldn't be parsed in mariadb [DBZ-6186](https://issues.redhat.com/browse/DBZ-6186)
* ddl parse failed for key partition table [DBZ-6188](https://issues.redhat.com/browse/DBZ-6188)
* Config options internal.schema.history.internal.ddl.filter not working [DBZ-6190](https://issues.redhat.com/browse/DBZ-6190)
* Use CHARSET for alterByConvertCharset clause [DBZ-6194](https://issues.redhat.com/browse/DBZ-6194)
* Data loss upon connector restart [DBZ-6204](https://issues.redhat.com/browse/DBZ-6204)
* ParsingException: DDL statement couldn't be parsed [DBZ-6217](https://issues.redhat.com/browse/DBZ-6217)
* The CHARACTER/CHARACTER(p)/CHARACTER VARYING(p) data types not recognized as JDBC type CHAR [DBZ-6221](https://issues.redhat.com/browse/DBZ-6221)
* MySQL treats the BOOLEAN synonym differently when processed in snapshot vs streaming phases. [DBZ-6225](https://issues.redhat.com/browse/DBZ-6225)
* MySQL treats REAL synonym differently when processed in snapshot vs streaming phases. [DBZ-6226](https://issues.redhat.com/browse/DBZ-6226)
* Spanner Connector - Deadlock in BufferedPublisher when publish gives exception [DBZ-6227](https://issues.redhat.com/browse/DBZ-6227)
* Publish of sync event fails when message becomes very large.  [DBZ-6228](https://issues.redhat.com/browse/DBZ-6228)
* MySQL treats NCHAR/NVARCHAR differently when processed in snapshot vs streaming phases. [DBZ-6231](https://issues.redhat.com/browse/DBZ-6231)
* MySQL singleDeleteStatement parser does not support table alias [DBZ-6243](https://issues.redhat.com/browse/DBZ-6243)
* Testcontainers MongoDbReplicaSetTest failing with MongoDB 4.2 [DBZ-6247](https://issues.redhat.com/browse/DBZ-6247)
* Wrong error thrown when snapshot.custom_class=custom and no snapshot.custom.class [DBZ-6249](https://issues.redhat.com/browse/DBZ-6249)
* Missing GEOMETRY keyword which can be used as column name [DBZ-6250](https://issues.redhat.com/browse/DBZ-6250)
* Postgres connector stuck trying to fallback to restart_lsn when replication slot confirmed_flush_lsn is null. [DBZ-6251](https://issues.redhat.com/browse/DBZ-6251)
* MariaDB's UUID column type cannot be parsed when scheme is loaded [DBZ-6255](https://issues.redhat.com/browse/DBZ-6255)


### Other changes since 2.2.0.Alpha3

* Document message.key.columns and tombstone events limitations for default REPLICA IDENTITY [DBZ-5490](https://issues.redhat.com/browse/DBZ-5490)
* Reflect configuration changes for MongoDB connector in documentation [DBZ-6090](https://issues.redhat.com/browse/DBZ-6090)
* Create Oracle CI workflow [DBZ-6115](https://issues.redhat.com/browse/DBZ-6115)
* Provide instructions for upgrading from Debezium 1.x to 2.x  [DBZ-6128](https://issues.redhat.com/browse/DBZ-6128)
* Update connector configuration examples in deployment instructions  [DBZ-6153](https://issues.redhat.com/browse/DBZ-6153)
* Insert missing Nebel annotations for Oracle connector FAQ topic [DBZ-6215](https://issues.redhat.com/browse/DBZ-6215)
* Add metadata for MongoDB change streams topic [DBZ-6223](https://issues.redhat.com/browse/DBZ-6223)
* Remove incubation notice from Debezium Server page [DBZ-6235](https://issues.redhat.com/browse/DBZ-6235)
* Ensure correct build for Oracle CI in case of pull request [DBZ-6239](https://issues.redhat.com/browse/DBZ-6239)
* Fix broken link to Streams documentation in shared deployment files [DBZ-6263](https://issues.redhat.com/browse/DBZ-6263)
* Update config example in Installing Debezium on OpenShift [DBZ-6267](https://issues.redhat.com/browse/DBZ-6267)



## 2.2.0.Alpha3
March 8th 2023 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12402444)

### New features since 2.2.0.Alpha2

* Optionally parallelize initial snapshots [DBZ-823](https://issues.redhat.com/browse/DBZ-823)
* Server side database and collection filtering on MongoDB change stream [DBZ-5102](https://issues.redhat.com/browse/DBZ-5102)
* Create a Datastax connector based on Cassandra connector [DBZ-5951](https://issues.redhat.com/browse/DBZ-5951)
* Add support for honouring MongoDB read preference in change stream after promotion [DBZ-5953](https://issues.redhat.com/browse/DBZ-5953)
* Add support for header to all Debezium Server sinks [DBZ-6017](https://issues.redhat.com/browse/DBZ-6017)
* Add support for surrogate keys for incremental snapshots [DBZ-6023](https://issues.redhat.com/browse/DBZ-6023)
* Support String type for key in Mongo incremental snapshot [DBZ-6116](https://issues.redhat.com/browse/DBZ-6116)
* fix typo in sqlserver doc. change "evemts" to "events". [DBZ-6123](https://issues.redhat.com/browse/DBZ-6123)
* Support change stream filtering using MongoDB's aggregation pipeline step [DBZ-6131](https://issues.redhat.com/browse/DBZ-6131)
* Remove hardcoded list of system database exclusions that are not required for change streaming [DBZ-6152](https://issues.redhat.com/browse/DBZ-6152)


### Breaking changes since 2.2.0.Alpha2

* Debezium truncating micro/nanosecond part if it is all zeros with time zone [DBZ-6163](https://issues.redhat.com/browse/DBZ-6163)


### Fixes since 2.2.0.Alpha2

* When using `snapshot.collection.include.list`, relational schema isn't populated correctly [DBZ-3594](https://issues.redhat.com/browse/DBZ-3594)
* Debezium UI should use fast-jar again with Quarkus 2.x [DBZ-4621](https://issues.redhat.com/browse/DBZ-4621)
* GCP Spanner connector start failing when there are multiple indexes on a single column [DBZ-6101](https://issues.redhat.com/browse/DBZ-6101)
* Negative remaining attempts on MongoDB reconnect case [DBZ-6113](https://issues.redhat.com/browse/DBZ-6113)
* Tables with spaces or non-ASCII characters in their name are not captured by Oracle because they must be quoted. [DBZ-6120](https://issues.redhat.com/browse/DBZ-6120)
* Offsets are not advanced in a CDB deployment with low frequency of changes to PDB [DBZ-6125](https://issues.redhat.com/browse/DBZ-6125)
* Oracle TIMESTAMP WITH TIME ZONE is emitted as GMT during snapshot rather than the specified TZ [DBZ-6143](https://issues.redhat.com/browse/DBZ-6143)
* Debezium UI E2E Frontend build failing randomly with corrupted Node 16 tar file [DBZ-6146](https://issues.redhat.com/browse/DBZ-6146)
* Debezium UI SQL Server tests randomly fail due to slow agent start-up [DBZ-6149](https://issues.redhat.com/browse/DBZ-6149)
* RelationalSnapshotChangeEventSource swallows exception generated during snapshot [DBZ-6179](https://issues.redhat.com/browse/DBZ-6179)


### Other changes since 2.2.0.Alpha2

* Remove redundancies between MySqlJdbcContext and MySqlConnection [DBZ-4855](https://issues.redhat.com/browse/DBZ-4855)
* Refactor connection management for mongodb connector [DBZ-6032](https://issues.redhat.com/browse/DBZ-6032)
* Conditionalization anomalies in Oracle connector doc [DBZ-6073](https://issues.redhat.com/browse/DBZ-6073)
* Optimize debezium-testing-system image to build only modules necessary for tests [DBZ-6108](https://issues.redhat.com/browse/DBZ-6108)
* Migrate system test jobs to gitlab [DBZ-6109](https://issues.redhat.com/browse/DBZ-6109)
* Remove references to adding configuration settings to a .properties file  [DBZ-6130](https://issues.redhat.com/browse/DBZ-6130)
* Fix Debezium Server Redis random test failures [DBZ-6133](https://issues.redhat.com/browse/DBZ-6133)
* Allow TestContainers test framework to expose ConnectorConfiguration as JSON [DBZ-6136](https://issues.redhat.com/browse/DBZ-6136)
* Upgrade impsort-maven-plugin from 1.7.0 to 1.8.0 [DBZ-6144](https://issues.redhat.com/browse/DBZ-6144)
* Upgrade Quarkus dependencies to 2.16.3.Final [DBZ-6150](https://issues.redhat.com/browse/DBZ-6150)
* Github workflows not working for Cassandra job (step Build Debezium Connector Cassandra) [DBZ-6171](https://issues.redhat.com/browse/DBZ-6171)
* Create SSL scenarios for integration tests for MySQL connector [DBZ-6184](https://issues.redhat.com/browse/DBZ-6184)



## 2.2.0.Alpha2
February 16th 2023 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12400776)

### New features since 2.2.0.Alpha1

* Better control on debezium GTID usage [DBZ-2296](https://issues.redhat.com/browse/DBZ-2296)
* Adding new option for "ExtractNewRecordState" SMT to exclude unchanged fields [DBZ-5283](https://issues.redhat.com/browse/DBZ-5283)
* Reactive implementation of Outbox module [DBZ-5758](https://issues.redhat.com/browse/DBZ-5758)
* Debezium MongoDB connector wizard Filter definition page needs work [DBZ-5899](https://issues.redhat.com/browse/DBZ-5899)
* Debezium Storage add support for Apache RocketMQ [DBZ-5997](https://issues.redhat.com/browse/DBZ-5997)
* debezium-server Pulsar support non-default tenant and namespace [DBZ-6033](https://issues.redhat.com/browse/DBZ-6033)
* Add wallTime in mongodb source info  [DBZ-6038](https://issues.redhat.com/browse/DBZ-6038)
* Vitess: Support Mapping unsigned bigint mysql column type to long [DBZ-6043](https://issues.redhat.com/browse/DBZ-6043)
* Increase query.fetch.size default to something sensible above zero [DBZ-6079](https://issues.redhat.com/browse/DBZ-6079)
* Expose sequence field in CloudEvents message id [DBZ-6089](https://issues.redhat.com/browse/DBZ-6089)
* Reduce verbosity of skipped transactions if transaction has no events relevant to captured tables [DBZ-6094](https://issues.redhat.com/browse/DBZ-6094)
* Upgrade Kafka client to 3.4.0 [DBZ-6102](https://issues.redhat.com/browse/DBZ-6102)


### Breaking changes since 2.2.0.Alpha1

* Support unicode table names in topic names [DBZ-5743](https://issues.redhat.com/browse/DBZ-5743)
* Move debezium-server into a separate repository [DBZ-6049](https://issues.redhat.com/browse/DBZ-6049)
* Reading SSN field can lead to Numeric Overflow if a transaction contains more than Integer.MAX_VALUE SQL sequences [DBZ-6091](https://issues.redhat.com/browse/DBZ-6091)


### Fixes since 2.2.0.Alpha1

* Not all connectors are available in debezium server [DBZ-4038](https://issues.redhat.com/browse/DBZ-4038)
* Property event.processing.failure.handling.mode is not present in MySQL documentation [DBZ-4829](https://issues.redhat.com/browse/DBZ-4829)
* Data type conversion failed for mysql bigint [DBZ-5798](https://issues.redhat.com/browse/DBZ-5798)
* ActivateTracingSpan wrong timestamps reported [DBZ-5827](https://issues.redhat.com/browse/DBZ-5827)
* Unable to specify column or table include list if name contains a backslash \ [DBZ-5917](https://issues.redhat.com/browse/DBZ-5917)
* debezium-connector-cassandra 2.1.0.Alpha2 plugin can no longer run "out of the box" [DBZ-5925](https://issues.redhat.com/browse/DBZ-5925)
* MongoDB Incremental Snapshot not Working [DBZ-5973](https://issues.redhat.com/browse/DBZ-5973)
* Nullable columns marked with "optional: false" in DDL events [DBZ-6003](https://issues.redhat.com/browse/DBZ-6003)
* Vitess: Handle the shard list difference between current db shards and persisted shards [DBZ-6011](https://issues.redhat.com/browse/DBZ-6011)
* DDL statement with TokuDB engine specific "CLUSTERING KEY" couldn't be parsed [DBZ-6016](https://issues.redhat.com/browse/DBZ-6016)
* DDL parse fail for role revoke with "user-like" role name [DBZ-6019](https://issues.redhat.com/browse/DBZ-6019)
* DDL parse fail for ALTER USER x DEFAULT ROLE y; [DBZ-6020](https://issues.redhat.com/browse/DBZ-6020)
* Offsets are not flushed on connect offsets topic when encountering an error on Postgres connector [DBZ-6026](https://issues.redhat.com/browse/DBZ-6026)
* Unexpected format for TIME column: 8:00 [DBZ-6029](https://issues.redhat.com/browse/DBZ-6029)
* Oracle does not support compression/logging clauses after an LOB storage clause [DBZ-6031](https://issues.redhat.com/browse/DBZ-6031)
* Debezium is logging the full message along with the error [DBZ-6037](https://issues.redhat.com/browse/DBZ-6037)
* Improve resilience during internal schema history recovery from Kafka [DBZ-6039](https://issues.redhat.com/browse/DBZ-6039)
* Incremental snapshot sends the events from signalling DB to Kafka [DBZ-6051](https://issues.redhat.com/browse/DBZ-6051)
* Mask password in log statement [DBZ-6064](https://issues.redhat.com/browse/DBZ-6064)
* Loading Custom offset storage fails with Class not found error [DBZ-6075](https://issues.redhat.com/browse/DBZ-6075)
* SQL Server tasks fail if the number of databases is smaller than maxTasks [DBZ-6084](https://issues.redhat.com/browse/DBZ-6084)
* When using LOB support, an UPDATE against multiple rows can lead to inconsistent event data [DBZ-6107](https://issues.redhat.com/browse/DBZ-6107)


### Other changes since 2.2.0.Alpha1

* System test-suite ability to prepare OCP environment [DBZ-3832](https://issues.redhat.com/browse/DBZ-3832)
* TransactionMetadataIT is unstable for Db2 [DBZ-5149](https://issues.redhat.com/browse/DBZ-5149)
* Update Java Outreach job to use Java 20 [DBZ-5825](https://issues.redhat.com/browse/DBZ-5825)
* Upgrade to Quarkus 2.16.0.Final [DBZ-6005](https://issues.redhat.com/browse/DBZ-6005)
* Prepare MongoDB ExtractNewDocumentState SMT doc for downstream GA [DBZ-6006](https://issues.redhat.com/browse/DBZ-6006)
* SQL Server IncrementalSnapshotWithRecompileIT fails randomly [DBZ-6035](https://issues.redhat.com/browse/DBZ-6035)
* Remove the redundant "schema.history.internal" from MySqlConnectorConfig [DBZ-6040](https://issues.redhat.com/browse/DBZ-6040)
* Broken links on FAQ [DBZ-6042](https://issues.redhat.com/browse/DBZ-6042)
* Upgrade Kafka to 3.3.2 [DBZ-6054](https://issues.redhat.com/browse/DBZ-6054)
* Upgrade netty version in Pravega to 4.1.86.Final [DBZ-6057](https://issues.redhat.com/browse/DBZ-6057)
* Return back the driver class option for MySQL connector [DBZ-6059](https://issues.redhat.com/browse/DBZ-6059)
* Invalid links breaking downstream documentation build [DBZ-6069](https://issues.redhat.com/browse/DBZ-6069)
* Request SA for UMB [DBZ-6077](https://issues.redhat.com/browse/DBZ-6077)
* Create certificates for Jenkins for UMB [DBZ-6078](https://issues.redhat.com/browse/DBZ-6078)
* Request access to cpass UMB topic [DBZ-6080](https://issues.redhat.com/browse/DBZ-6080)
* Broken debezium-server source file link on docs page [DBZ-6111](https://issues.redhat.com/browse/DBZ-6111)



## 2.2.0.Alpha1
January 19th 2023 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12400295)

### New features since 2.1.1.Final

* Remove redundant modifiers of members for interface fields [DBZ-2439](https://issues.redhat.com/browse/DBZ-2439)
* Allow reading from read-only Oracle standby disaster/recovery [DBZ-3866](https://issues.redhat.com/browse/DBZ-3866)
* Remove option for specifying driver class from MySQL Connector [DBZ-4663](https://issues.redhat.com/browse/DBZ-4663)
* Support S3 bucket as Debezium history store [DBZ-5402](https://issues.redhat.com/browse/DBZ-5402)
* Update the DBZ-UI documentation page to incorporate the recently added "Custom properties" step details [DBZ-5878](https://issues.redhat.com/browse/DBZ-5878)
* Support retrying database connection failures during connector start [DBZ-5879](https://issues.redhat.com/browse/DBZ-5879)
* Add support for Connect Headers to Debezium Server [DBZ-5926](https://issues.redhat.com/browse/DBZ-5926)
* Sink adapter for Apache RocketMQ [DBZ-5962](https://issues.redhat.com/browse/DBZ-5962)
* Sink adapter for Infinispan [DBZ-5986](https://issues.redhat.com/browse/DBZ-5986)
* Add custom Debezium banner to Debezium Server [DBZ-6004](https://issues.redhat.com/browse/DBZ-6004)
* Postgres LSN check should honor event.processing.failure.handling.mode [DBZ-6012](https://issues.redhat.com/browse/DBZ-6012)
* Enhance the Spanner connector by adding features and/or solving bugs [DBZ-6014](https://issues.redhat.com/browse/DBZ-6014)


### Breaking changes since 2.1.1.Final

* Debezium truncating micro/nanosecond part if it is all zeros [DBZ-5996](https://issues.redhat.com/browse/DBZ-5996)


### Fixes since 2.1.1.Final

* Debezium is not working with apicurio and custom truststores [DBZ-5282](https://issues.redhat.com/browse/DBZ-5282)
*  Show/Hide password does not work on Connectors View details screen [DBZ-5322](https://issues.redhat.com/browse/DBZ-5322)
* Snapshotter#snapshotCompleted is invoked regardless of snapshot result [DBZ-5852](https://issues.redhat.com/browse/DBZ-5852)
* Oracle cannot undo change [DBZ-5907](https://issues.redhat.com/browse/DBZ-5907)
* Postgresql Data Loss on restarts [DBZ-5915](https://issues.redhat.com/browse/DBZ-5915)
* Oracle Multithreading lost data [DBZ-5945](https://issues.redhat.com/browse/DBZ-5945)
* Spanner connector is missing JSR-310 dependency [DBZ-5959](https://issues.redhat.com/browse/DBZ-5959)
* Truncate records incompatible with ExtractNewRecordState [DBZ-5966](https://issues.redhat.com/browse/DBZ-5966)
* Computed partition must not be negative [DBZ-5967](https://issues.redhat.com/browse/DBZ-5967)
* Stream tag images are not published [DBZ-5979](https://issues.redhat.com/browse/DBZ-5979)
* Table size log message for snapshot.select.statement.overrides tables not correct [DBZ-5985](https://issues.redhat.com/browse/DBZ-5985)
* NPE in execute snapshot signal with exclude.tables config on giving wrong table name [DBZ-5988](https://issues.redhat.com/browse/DBZ-5988)
* There is a problem with postgresql connector parsing the boundary value of money type [DBZ-5991](https://issues.redhat.com/browse/DBZ-5991)
* Log statement for unparseable DDL statement in MySqlDatabaseSchema contains placeholder [DBZ-5993](https://issues.redhat.com/browse/DBZ-5993)
* Synchronize all actions with core CI & fix GitHub Actions set-output command [DBZ-5998](https://issues.redhat.com/browse/DBZ-5998)
* Postgresql connector parses the null of the money type into 0 [DBZ-6001](https://issues.redhat.com/browse/DBZ-6001)
* Run PostgresConnectorIT.shouldReceiveChangesForChangeColumnDefault() failed [DBZ-6002](https://issues.redhat.com/browse/DBZ-6002)


### Other changes since 2.1.1.Final

* Plug-in version information duplicated [DBZ-4669](https://issues.redhat.com/browse/DBZ-4669)
* Move common code in Cassandra connector core module [DBZ-5950](https://issues.redhat.com/browse/DBZ-5950)
* website-builder image cannot be built [DBZ-5971](https://issues.redhat.com/browse/DBZ-5971)
* Zookeeper 3.6.3 available only on archive [DBZ-5972](https://issues.redhat.com/browse/DBZ-5972)
* Jenkins pipelines don't provide information about FAILURE status [DBZ-5974](https://issues.redhat.com/browse/DBZ-5974)
* Remove incubating documentation text for MongoDB ExtractNewDocumentState SMT  [DBZ-5975](https://issues.redhat.com/browse/DBZ-5975)
* Use replace rather than replaceAll [DBZ-5976](https://issues.redhat.com/browse/DBZ-5976)
* Upgrade Apicurio to 2.4.1.Final [DBZ-5977](https://issues.redhat.com/browse/DBZ-5977)
* Upgrade JDBC driver to 42.5.1 [DBZ-5980](https://issues.redhat.com/browse/DBZ-5980)
* Update TestContainers to 1.17.6 [DBZ-5990](https://issues.redhat.com/browse/DBZ-5990)
* Align pipeline tests with new connector pipelines [DBZ-5999](https://issues.redhat.com/browse/DBZ-5999)
* Db2 incremental snapshot test execution is blocked [DBZ-6008](https://issues.redhat.com/browse/DBZ-6008)



## 2.1.0.Final
December 22nd 2022 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12400034)

### New features since 2.1.0.Beta1

* Implement support for JSON_TABLE in MySQL parser [DBZ-3575](https://issues.redhat.com/browse/DBZ-3575)
* Provide Debezium Spanner connector [DBZ-5937](https://issues.redhat.com/browse/DBZ-5937)
* Print the readable data class name in JdbcValueConverters.handleUnknownData [DBZ-5946](https://issues.redhat.com/browse/DBZ-5946)


### Breaking changes since 2.1.0.Beta1

* MongoDB connector to use secondary node [DBZ-4339](https://issues.redhat.com/browse/DBZ-4339)
* Vitess: Support snapshot feature [DBZ-5930](https://issues.redhat.com/browse/DBZ-5930)


### Fixes since 2.1.0.Beta1

* Cannot expand JSON payload with nested arrays of objects [DBZ-5344](https://issues.redhat.com/browse/DBZ-5344)
* field.exclude.list in MongoDB Connector v2.0 doesn't accept * as a wildcard for collectionName [DBZ-5818](https://issues.redhat.com/browse/DBZ-5818)
* Debezium UI documentation link is not accessible to the user via documentation side navigation menu. [DBZ-5900](https://issues.redhat.com/browse/DBZ-5900)
* Toasted json/int/bigint arrays are not properly processed [DBZ-5936](https://issues.redhat.com/browse/DBZ-5936)
* No table filters found for filtered publication [DBZ-5949](https://issues.redhat.com/browse/DBZ-5949)


### Other changes since 2.1.0.Beta1

None



## 2.1.0.Beta1
December 16th 2022 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12399345)

### New features since 2.1.0.Alpha2

* Postgres: Disable LSN confirmation to database [DBZ-5811](https://issues.redhat.com/browse/DBZ-5811)
* Realize data distribution according to specified fields [DBZ-5847](https://issues.redhat.com/browse/DBZ-5847)
* Support predicate parameters in Debezium Server [DBZ-5940](https://issues.redhat.com/browse/DBZ-5940)
* Use the Patternfly database icon as a placeholder for Oracle Database [DBZ-5941](https://issues.redhat.com/browse/DBZ-5941)


### Breaking changes since 2.1.0.Alpha2

* Replace simple string for range tombstones for JSON object [DBZ-5912](https://issues.redhat.com/browse/DBZ-5912)
* Cassandra TimeUUID values should be encoded as string [DBZ-5923](https://issues.redhat.com/browse/DBZ-5923)


### Fixes since 2.1.0.Alpha2

* Handle toasted String array [DBZ-4941](https://issues.redhat.com/browse/DBZ-4941)
* Cassandra deletes log files on exit when real time processing is enabled [DBZ-5776](https://issues.redhat.com/browse/DBZ-5776)
* ReplicationConnectionIT test fails [DBZ-5800](https://issues.redhat.com/browse/DBZ-5800)
* MongoDB docs for incremental snapshots is SQL specific [DBZ-5804](https://issues.redhat.com/browse/DBZ-5804)
* Conflicting documentation for snapshot.mode property in MongoDB connector v2.0 [DBZ-5812](https://issues.redhat.com/browse/DBZ-5812)
* IllegalStateException is thrown if task is recovering while other tasks are running [DBZ-5855](https://issues.redhat.com/browse/DBZ-5855)
* Negative decimal number scale is not supported by Avro [DBZ-5880](https://issues.redhat.com/browse/DBZ-5880)
* Connector deployment instructions provide incorrect Maven path for Debezium scripting component  [DBZ-5882](https://issues.redhat.com/browse/DBZ-5882)
* Incorrect Streams Kafka version in connector deployment instructions for creating a custom image [DBZ-5883](https://issues.redhat.com/browse/DBZ-5883)
* Run postgres connector RecordsStreamProducerIT failed [DBZ-5895](https://issues.redhat.com/browse/DBZ-5895)
* Suppport INSERT INTO statements with dots in column names  [DBZ-5904](https://issues.redhat.com/browse/DBZ-5904)
* Incorrect default value for additional-condition docs [DBZ-5906](https://issues.redhat.com/browse/DBZ-5906)
* ConnectorLifecycle is not logging anymore the exception stacktrace when startup fails [DBZ-5908](https://issues.redhat.com/browse/DBZ-5908)
* Debezium Server stops with NPE when Redis does not report the "maxmemory" field in "info memory" command [DBZ-5911](https://issues.redhat.com/browse/DBZ-5911)
* PostgresConnectorIT#shouldAckLsnOnSourceByDefault and #shouldNotAckLsnOnSource fails [DBZ-5914](https://issues.redhat.com/browse/DBZ-5914)
* SQL Server connector database.instance config option is ignored [DBZ-5924](https://issues.redhat.com/browse/DBZ-5924)
* Wrong java version in Installing Debezium documentation [DBZ-5928](https://issues.redhat.com/browse/DBZ-5928)
* Toasted varchar array is not correctly processed [DBZ-5944](https://issues.redhat.com/browse/DBZ-5944)


### Other changes since 2.1.0.Alpha2

* Use static import for Assertions in all tests [DBZ-2432](https://issues.redhat.com/browse/DBZ-2432)
* Test window function in MySQL parser [DBZ-3576](https://issues.redhat.com/browse/DBZ-3576)
* Run test against Apicurio registry [DBZ-5838](https://issues.redhat.com/browse/DBZ-5838)
* Add tests against multinode RS and (ideally) sharded cluster  [DBZ-5857](https://issues.redhat.com/browse/DBZ-5857)
* Update documentation for Debezium Server with Cassandra Connector [DBZ-5885](https://issues.redhat.com/browse/DBZ-5885)
* Allow CI deploy clusters to PSI [DBZ-5887](https://issues.redhat.com/browse/DBZ-5887)
* Mariadb and Mysql have different syntax [DBZ-5888](https://issues.redhat.com/browse/DBZ-5888)
* Execute IT tests in alphabetical order [DBZ-5889](https://issues.redhat.com/browse/DBZ-5889)
* Migrate debezium-server-nats-jetstream to AssertJ [DBZ-5901](https://issues.redhat.com/browse/DBZ-5901)
* Reduce jenkins jobs footprint [DBZ-5905](https://issues.redhat.com/browse/DBZ-5905)
* Move Debezium Cassandra connector out from incubation [DBZ-5922](https://issues.redhat.com/browse/DBZ-5922)
* Clean up "doSnapshot" config code [DBZ-5931](https://issues.redhat.com/browse/DBZ-5931)
* Version badge on README in Cassandra connector is stuck [DBZ-5932](https://issues.redhat.com/browse/DBZ-5932)
* Make startup of Cassandra container faster [DBZ-5933](https://issues.redhat.com/browse/DBZ-5933)
* Fix logging for tests for Cassandra connector [DBZ-5934](https://issues.redhat.com/browse/DBZ-5934)



## 2.1.0.Alpha2
November 30th 2022 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12398904)

### New features since 2.1.0.Alpha1

* Expose Cassandra Connector via Debezium Server [DBZ-2098](https://issues.redhat.com/browse/DBZ-2098)
* Validate Debezium Server configuration properties [DBZ-4720](https://issues.redhat.com/browse/DBZ-4720)
* Enable pass-thru of additional config options in Debezium UI [DBZ-5324](https://issues.redhat.com/browse/DBZ-5324)
* Sink adapter for Nats JetStream [DBZ-5772](https://issues.redhat.com/browse/DBZ-5772)
* Replace obsolete DebeziumDownload attribute [DBZ-5835](https://issues.redhat.com/browse/DBZ-5835)
* Reduce container image sizes by consolidating operations per layer [DBZ-5864](https://issues.redhat.com/browse/DBZ-5864)
* Typo error in Oracle connector documentation 2.0 [DBZ-5877](https://issues.redhat.com/browse/DBZ-5877)


### Breaking changes since 2.1.0.Alpha1

* Add Debezium REST extension to tagged KC container image [DBZ-4303](https://issues.redhat.com/browse/DBZ-4303)
* Upgrade Debezium base image to Fedora 37 [DBZ-5461](https://issues.redhat.com/browse/DBZ-5461)
* Postgres connector results in silent data loss if replication slot is recreated [DBZ-5739](https://issues.redhat.com/browse/DBZ-5739)


### Fixes since 2.1.0.Alpha1

* Embedded Engine or Server retrying indefinitely on all types of retriable errors [DBZ-5661](https://issues.redhat.com/browse/DBZ-5661)
* PostgreSQL missing metadata info [DBZ-5789](https://issues.redhat.com/browse/DBZ-5789)
* For outbox transformation, when 'table.expand.json.payload' is set to true null values are not correctly deserialized [DBZ-5796](https://issues.redhat.com/browse/DBZ-5796)
* Cassandra decimal values are not deserialized using Debezium Cassandra Connector [DBZ-5807](https://issues.redhat.com/browse/DBZ-5807)
* Cassandra varint type is currently not supported [DBZ-5808](https://issues.redhat.com/browse/DBZ-5808)
* 'topic.prefix' default value in MongoDB connector v2.0 [DBZ-5817](https://issues.redhat.com/browse/DBZ-5817)
* Quarkus outbox extention never finishes the open tracing span [DBZ-5821](https://issues.redhat.com/browse/DBZ-5821)
* fix names of range fields in schema to comply with Avro standard [DBZ-5826](https://issues.redhat.com/browse/DBZ-5826)
* ExtractNewDocumentState does not support updateDescription.updatedFields field [DBZ-5834](https://issues.redhat.com/browse/DBZ-5834)
* CREATE/ALTER user does not support COMMENT token [DBZ-5836](https://issues.redhat.com/browse/DBZ-5836)
* Invalid Java object for schema with type FLOAT64: class java.lang.Float [DBZ-5843](https://issues.redhat.com/browse/DBZ-5843)
* Message contents might not get logged in case of error [DBZ-5874](https://issues.redhat.com/browse/DBZ-5874)
* CREATE/ALTER user does not support ATTRIBUTE token [DBZ-5876](https://issues.redhat.com/browse/DBZ-5876)


### Other changes since 2.1.0.Alpha1

* SQL table rename affect on Kafka connector and topic [DBZ-5423](https://issues.redhat.com/browse/DBZ-5423)
* Create RHAF version of Debezium docs [DBZ-5729](https://issues.redhat.com/browse/DBZ-5729)
* Add Debezium doc section to RHAF [DBZ-5730](https://issues.redhat.com/browse/DBZ-5730)
* Create new Debezium section in the docs. [DBZ-5731](https://issues.redhat.com/browse/DBZ-5731)
* Add Debezium docs to DDF [DBZ-5732](https://issues.redhat.com/browse/DBZ-5732)
* Create ARO provisioning job [DBZ-5742](https://issues.redhat.com/browse/DBZ-5742)
* Amend Confluent Avro converter installation documentation [DBZ-5762](https://issues.redhat.com/browse/DBZ-5762)
* Modify ocp system tests to archive test results and logs [DBZ-5785](https://issues.redhat.com/browse/DBZ-5785)
* GitHub Actions: Deprecating save-state and set-output commands [DBZ-5824](https://issues.redhat.com/browse/DBZ-5824)
* Change logging levels of several schema change handler log entries [DBZ-5833](https://issues.redhat.com/browse/DBZ-5833)
* Revert running tests against Apicurio registry [DBZ-5839](https://issues.redhat.com/browse/DBZ-5839)
* Add Kubernetes plugin to Jenkins [DBZ-5844](https://issues.redhat.com/browse/DBZ-5844)
* OracleConnectorIT shouldIgnoreAllTablesInExcludedSchemas test may randomly fail [DBZ-5850](https://issues.redhat.com/browse/DBZ-5850)
* Upgrade wildfly-elytron to 1.15.5 / 1.16.1 due to CVE-2021-3642 [DBZ-5854](https://issues.redhat.com/browse/DBZ-5854)
* Upgrade PostgreSQL example images to Postgres 15 [DBZ-5860](https://issues.redhat.com/browse/DBZ-5860)
* GitHub Actions deprecation of Node 12 - actions/checkout [DBZ-5870](https://issues.redhat.com/browse/DBZ-5870)



## 2.1.0.Alpha1
November 10th 2022 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12397585)

### New features since 2.0.0.Final

* Support for Postgres 15 [DBZ-5370](https://issues.redhat.com/browse/DBZ-5370)
* Add support for SMT predicates in Debezium Engine [DBZ-5530](https://issues.redhat.com/browse/DBZ-5530)
* MySQL Connector capture TRUNCATE command as message in table topic [DBZ-5610](https://issues.redhat.com/browse/DBZ-5610)
* Improve LogMiner query performance by reducing REGEXP_LIKE disjunctions [DBZ-5648](https://issues.redhat.com/browse/DBZ-5648)
* Expose heartbeatFrequency setting for mongodb connector [DBZ-5736](https://issues.redhat.com/browse/DBZ-5736)
* Provide Redis storage as store module [DBZ-5749](https://issues.redhat.com/browse/DBZ-5749)
* Redis Sink wait for Redis Replica writes [DBZ-5752](https://issues.redhat.com/browse/DBZ-5752)
* Redis sink back-pressure mechanism when Redis memory is almost full [DBZ-5782](https://issues.redhat.com/browse/DBZ-5782)
* Enhance the ability to sanitize topic name [DBZ-5790](https://issues.redhat.com/browse/DBZ-5790)


### Breaking changes since 2.0.0.Final

None


### Fixes since 2.0.0.Final

* Using snapshot boundary mode "all" causes DebeziumException on Oracle RAC [DBZ-5302](https://issues.redhat.com/browse/DBZ-5302)
* ORA-01003: no statement parsed [DBZ-5352](https://issues.redhat.com/browse/DBZ-5352)
* Missing snapshot pending transactions [DBZ-5482](https://issues.redhat.com/browse/DBZ-5482)
* Db2 documentation refers to invalid SMALLMONEY and MONEY data types  [DBZ-5504](https://issues.redhat.com/browse/DBZ-5504)
* Using snapshot.mode ALWAYS uses SCN from offsets [DBZ-5626](https://issues.redhat.com/browse/DBZ-5626)
* MongoDB multiple tasks monitor misalignment [DBZ-5629](https://issues.redhat.com/browse/DBZ-5629)
* UNIQUE INDEX with NULL value throws exception when lob.enabled is true [DBZ-5682](https://issues.redhat.com/browse/DBZ-5682)
* Oracle SQL parsing error when collation used [DBZ-5726](https://issues.redhat.com/browse/DBZ-5726)
* Columns are not excluded when doing incremental snapshots [DBZ-5727](https://issues.redhat.com/browse/DBZ-5727)
* Unparseable DDL statement [DBZ-5734](https://issues.redhat.com/browse/DBZ-5734)
* NullPointerException thrown during snapshot of tables in Oracle source connector [DBZ-5738](https://issues.redhat.com/browse/DBZ-5738)
* Remove note from snapshot metrics docs file that flags incremental snapshots as TP feature [DBZ-5748](https://issues.redhat.com/browse/DBZ-5748)
* Hostname not available for load balanced ocp services in ARO [DBZ-5753](https://issues.redhat.com/browse/DBZ-5753)
* Exclude Oracle Compression Advisor tables from capture to avoid infinite loop [DBZ-5756](https://issues.redhat.com/browse/DBZ-5756)
* More Oracle logging  [DBZ-5759](https://issues.redhat.com/browse/DBZ-5759)
* Oracle should only log row contents at TRACE level [DBZ-5760](https://issues.redhat.com/browse/DBZ-5760)
* Update system test artifact preparation to reflect naming changes in downstream [DBZ-5767](https://issues.redhat.com/browse/DBZ-5767)
* Outbox Router documentation outdated regarding value converter [DBZ-5770](https://issues.redhat.com/browse/DBZ-5770)
* Using DBMS_LOB.ERASE by itself can lead to an unexpected UPDATE with null BLOB value [DBZ-5773](https://issues.redhat.com/browse/DBZ-5773)
* Suppress logging of undetermined optionality for explicitly excluded columns [DBZ-5783](https://issues.redhat.com/browse/DBZ-5783)
* Oracle connector does not attempt restart when ORA-01089 exception is nested [DBZ-5791](https://issues.redhat.com/browse/DBZ-5791)
* Message with LSN 'LSN{XYZ}' not present among LSNs seen in the location phase [DBZ-5792](https://issues.redhat.com/browse/DBZ-5792)
* The merge method of configuration is not work [DBZ-5801](https://issues.redhat.com/browse/DBZ-5801)
* Mysql connector alter table with database name parse failed [DBZ-5802](https://issues.redhat.com/browse/DBZ-5802)


### Other changes since 2.0.0.Final

* Execute tests with Apicurio converters [DBZ-2131](https://issues.redhat.com/browse/DBZ-2131)
* Revision info missing on website [DBZ-5083](https://issues.redhat.com/browse/DBZ-5083)
* Debezium on ARO sanity testing [DBZ-5647](https://issues.redhat.com/browse/DBZ-5647)
* SQL Server connector docs should mention multi-task support [DBZ-5714](https://issues.redhat.com/browse/DBZ-5714)
* Remove downstream TP designation for RAC content in Oracle connector docs  [DBZ-5735](https://issues.redhat.com/browse/DBZ-5735)
* Update Pulsar client to 2.10.1 [DBZ-5737](https://issues.redhat.com/browse/DBZ-5737)
* Parametrize Strimzi operator name to enable multiple testsuites running on same cluster  [DBZ-5744](https://issues.redhat.com/browse/DBZ-5744)
* Enable CI to report results to ReportPortal instance [DBZ-5745](https://issues.redhat.com/browse/DBZ-5745)
* Debezium connectors ship with an old version of google-protobuf vulnerable to CVE-2022-3171 [DBZ-5747](https://issues.redhat.com/browse/DBZ-5747)
* Testsuite unable to connect to SQLServer due to encryption  [DBZ-5763](https://issues.redhat.com/browse/DBZ-5763)
* Testsuite uses incorrect jdbc driver class for SQLServer with docker [DBZ-5764](https://issues.redhat.com/browse/DBZ-5764)
* Upgrade com.jayway.jsonpath:json-path [DBZ-5766](https://issues.redhat.com/browse/DBZ-5766)
* Product profile is not used when running Oracle matrix against downstream [DBZ-5768](https://issues.redhat.com/browse/DBZ-5768)
* Upgrade to Quarkus 2.14.CR1 [DBZ-5774](https://issues.redhat.com/browse/DBZ-5774)
* Switch from Fest to AssertJ [DBZ-5779](https://issues.redhat.com/browse/DBZ-5779)
* Upgrade postgres driver to version 42.5.0 [DBZ-5780](https://issues.redhat.com/browse/DBZ-5780)
* Upgrade to Quarkus 2.14.0.Final [DBZ-5786](https://issues.redhat.com/browse/DBZ-5786)
* Doc Typo in cloudevents [DBZ-5788](https://issues.redhat.com/browse/DBZ-5788)
* Fix DB2 reporting script path [DBZ-5799](https://issues.redhat.com/browse/DBZ-5799)
* Add ORA-01555 to Oracle documentation [DBZ-5816](https://issues.redhat.com/browse/DBZ-5816)
* Change visibility of BaseSourceTask#logStatistics method to protected  [DBZ-5822](https://issues.redhat.com/browse/DBZ-5822)
* Upgrade Postgres images to Debian 11 [DBZ-5823](https://issues.redhat.com/browse/DBZ-5823)



## 2.0.0.Final
October 14th 2022 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12385340)

### New features since 2.0.0.CR1

None


### Breaking changes since 2.0.0.CR1

* Transaction IDs in PostgreSQL transaction metadata topics subject to wraparound [DBZ-5329](https://issues.redhat.com/browse/DBZ-5329)


### Fixes since 2.0.0.CR1

* ORA-01289: cannot add duplicate logfile [DBZ-5276](https://issues.redhat.com/browse/DBZ-5276)
* Function DATE_ADD can be used as an identifier [DBZ-5679](https://issues.redhat.com/browse/DBZ-5679)
* MySqlConnector parse create view statement failed [DBZ-5708](https://issues.redhat.com/browse/DBZ-5708)
* The DDL_FILTER of SchemaHistory doesn't work for including break lines ddl statement [DBZ-5709](https://issues.redhat.com/browse/DBZ-5709)
* Debezium Server 1.9.6 is using MSSQL JDBC 7.2.2 instead of 9.4.1 [DBZ-5711](https://issues.redhat.com/browse/DBZ-5711)
* Invalid prop names in MongoDB outbox router docs [DBZ-5715](https://issues.redhat.com/browse/DBZ-5715)
* tests are running forever [DBZ-5718](https://issues.redhat.com/browse/DBZ-5718)
* cassandra connector first startup ever may fail [DBZ-5719](https://issues.redhat.com/browse/DBZ-5719)
* Vitess: Handle Vstream error: unexpected server EOF [DBZ-5722](https://issues.redhat.com/browse/DBZ-5722)
* ParsingException: DDL statement couldn't be parsed (index hints) [DBZ-5724](https://issues.redhat.com/browse/DBZ-5724)


### Other changes since 2.0.0.CR1

* Remove whilelisted/blacklisted from log messages [DBZ-5710](https://issues.redhat.com/browse/DBZ-5710)
* MySqlSchemaMigrationIT runs failed [DBZ-5728](https://issues.redhat.com/browse/DBZ-5728)



## 2.0.0.CR1
October 7th 2022 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12397018)

### New features since 2.0.0.Beta2

* Implement retries for Debezium embedded engine [DBZ-4629](https://issues.redhat.com/browse/DBZ-4629)
* MySqlErrorHandler should handle SocketException [DBZ-5486](https://issues.redhat.com/browse/DBZ-5486)
* Traditional snapshot process setting source.ts_ms [DBZ-5591](https://issues.redhat.com/browse/DBZ-5591)
* Clean up "logical name" config [DBZ-5594](https://issues.redhat.com/browse/DBZ-5594)
* Upgrade Kafka client to 3.3.1 [DBZ-5600](https://issues.redhat.com/browse/DBZ-5600)
* When writing docs, use website stylesheet for IDE preview in IntelliJ [DBZ-5616](https://issues.redhat.com/browse/DBZ-5616)
* Support READ ONLY/ENCRYPTION options for alter database statment [DBZ-5622](https://issues.redhat.com/browse/DBZ-5622)
* Clarify semantics of include/exclude options [DBZ-5625](https://issues.redhat.com/browse/DBZ-5625)
* Added support for Mongo pre-image in change stream [DBZ-5628](https://issues.redhat.com/browse/DBZ-5628)
* Support for seting stats_sample_pages=default in alter table statements [DBZ-5631](https://issues.redhat.com/browse/DBZ-5631)
* support for using any expression in kill statements [DBZ-5636](https://issues.redhat.com/browse/DBZ-5636)
* Logging enhancement for non-incremental snapshot in postgres connector [DBZ-5639](https://issues.redhat.com/browse/DBZ-5639)
* Support set statement in mariadb [DBZ-5650](https://issues.redhat.com/browse/DBZ-5650)
* Add Mongo-initiator 6.0 container image [DBZ-5666](https://issues.redhat.com/browse/DBZ-5666)
* Remove logic name parameter from sub connector config [DBZ-5671](https://issues.redhat.com/browse/DBZ-5671)


### Breaking changes since 2.0.0.Beta2

* Default schema.name.adjustment.mode to "none" [DBZ-5541](https://issues.redhat.com/browse/DBZ-5541)


### Fixes since 2.0.0.Beta2

* ConvertingEngineBuilder looses the accents [DBZ-4213](https://issues.redhat.com/browse/DBZ-4213)
* Debezium Db2 Connector fails to handle default values in schema when is making the snapshot [DBZ-4990](https://issues.redhat.com/browse/DBZ-4990)
* Debezium 2.0.0.Beta1 Azure SQL breaking change [DBZ-5496](https://issues.redhat.com/browse/DBZ-5496)
* Oracle connector parsing SELECT_LOB_LOCATOR event missing constant `unavailable.value.placeholder` [DBZ-5581](https://issues.redhat.com/browse/DBZ-5581)
* Starting Embedded Engine swallows ClassNotFoundException so user cannot see why engine does not work [DBZ-5583](https://issues.redhat.com/browse/DBZ-5583)
* Message with LSN foo larger than expected LSN bar [DBZ-5597](https://issues.redhat.com/browse/DBZ-5597)
* Fix broken anchors in docs [DBZ-5618](https://issues.redhat.com/browse/DBZ-5618)
* DDL Parsing Error [DBZ-5623](https://issues.redhat.com/browse/DBZ-5623)
* MySQL connector cannot parse default value of decimal colum enclosed in double quotes [DBZ-5630](https://issues.redhat.com/browse/DBZ-5630)
* Support grant LOAD FROM S3, SELECT INTO S3, INVOKE LAMBDA with aws mysql [DBZ-5633](https://issues.redhat.com/browse/DBZ-5633)
* Continuously WARNs about undo transactions when LOB is enabled [DBZ-5635](https://issues.redhat.com/browse/DBZ-5635)
* Literal "${project.version}" in the source record instead of the actual version [DBZ-5640](https://issues.redhat.com/browse/DBZ-5640)
* TABLE_TYPE keyword can be used as identifier [DBZ-5643](https://issues.redhat.com/browse/DBZ-5643)
* Large numbers of ROLLBACK transactions can lead to memory leak when LOB is not enabled. [DBZ-5645](https://issues.redhat.com/browse/DBZ-5645)
* Race in DebeziumContainer during startup [DBZ-5651](https://issues.redhat.com/browse/DBZ-5651)
* Outbox pattern nested payload leads to connector crash [DBZ-5654](https://issues.redhat.com/browse/DBZ-5654)
* Allow the word STATEMENT to be a table / column name [DBZ-5662](https://issues.redhat.com/browse/DBZ-5662)
* ValidatePostgresConnectionIT.testInvalidPostgresConnection fails [DBZ-5664](https://issues.redhat.com/browse/DBZ-5664)
* Hardcoded driver task properties are not being passed to underlying connections [DBZ-5670](https://issues.redhat.com/browse/DBZ-5670)
* Keyword virtual can be used as an identifier [DBZ-5674](https://issues.redhat.com/browse/DBZ-5674)
* MongoDB Connector with DocumentDB errors with "{$natural: -1} is not supported" [DBZ-5677](https://issues.redhat.com/browse/DBZ-5677)


### Other changes since 2.0.0.Beta2

* Align connector properties to have an empty default cell if property has no default [DBZ-3327](https://issues.redhat.com/browse/DBZ-3327)
* Improve Filter SMT documentation / examples [DBZ-4417](https://issues.redhat.com/browse/DBZ-4417)
* Test failure on CI: SqlServerConnectorIT#updatePrimaryKeyTwiceWithRestartInMiddleOfTx [DBZ-4475](https://issues.redhat.com/browse/DBZ-4475)
* Intermittent test failure: SqlServerConnectorIT#updatePrimaryKeyWithRestartInMiddle() [DBZ-4490](https://issues.redhat.com/browse/DBZ-4490)
* Edit content newly added to the MongoDB connector doc  [DBZ-5542](https://issues.redhat.com/browse/DBZ-5542)
* Upgrade apicurio to 2.2.5.Final [DBZ-5549](https://issues.redhat.com/browse/DBZ-5549)
* Modify the Instantiator to not require classloader [DBZ-5585](https://issues.redhat.com/browse/DBZ-5585)
* Use quay.io in test containers [DBZ-5603](https://issues.redhat.com/browse/DBZ-5603)
* Remove records from being logged at all levels [DBZ-5612](https://issues.redhat.com/browse/DBZ-5612)
* Upgrade binary log client to 0.27.2 [DBZ-5620](https://issues.redhat.com/browse/DBZ-5620)
* Allow to change docker maven properties from command line [DBZ-5657](https://issues.redhat.com/browse/DBZ-5657)
* Update docker maven plugin [DBZ-5658](https://issues.redhat.com/browse/DBZ-5658)
* Run UI tests on all connector changes [DBZ-5660](https://issues.redhat.com/browse/DBZ-5660)
* Cleanup UI e2e tests after removing default value for topic.prefix [DBZ-5667](https://issues.redhat.com/browse/DBZ-5667)



## 2.0.0.Beta2
September 16th 2022 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12392459)

### New features since 2.0.0.Beta1

* Support binlog compression for MySQL [DBZ-2663](https://issues.redhat.com/browse/DBZ-2663)
* Limit log output for "Streaming requested from LSN" warnings [DBZ-3007](https://issues.redhat.com/browse/DBZ-3007)
* Redis Sink - Change the format of the message sent to the stream [DBZ-4441](https://issues.redhat.com/browse/DBZ-4441)
* Debezium UI frontend should use new URLs and new JSON schema descriptors [DBZ-4619](https://issues.redhat.com/browse/DBZ-4619)
* Provide a signal to pause/resume a running incremental snapshot [DBZ-4727](https://issues.redhat.com/browse/DBZ-4727)
* support mongodb connection string as configuration option [DBZ-4733](https://issues.redhat.com/browse/DBZ-4733)
* Update Readme on github for Cassandra 4.x support [DBZ-4839](https://issues.redhat.com/browse/DBZ-4839)
* Debezium Server verifies existence and format of the config file [DBZ-5116](https://issues.redhat.com/browse/DBZ-5116)
* Include Oracle Debezium Connector in Debezium Server distribution [DBZ-5122](https://issues.redhat.com/browse/DBZ-5122)
* Smart Backfills | Ability to backfill selective data [DBZ-5327](https://issues.redhat.com/browse/DBZ-5327)
* Support multiple tasks in vitess connector [DBZ-5382](https://issues.redhat.com/browse/DBZ-5382)
* Enhancing Cassandra 4 Connector to read incremental changes and not wait for Commit Log file to be marked complete [DBZ-5410](https://issues.redhat.com/browse/DBZ-5410)
* Unsupported non-relational tables should be gracefully skipped by the connector during streaming [DBZ-5441](https://issues.redhat.com/browse/DBZ-5441)
* Support incremental snapshot stop-snapshot signal sourced from Kafka topic [DBZ-5453](https://issues.redhat.com/browse/DBZ-5453)
* Upgrade Kafka client to 3.2.1 [DBZ-5463](https://issues.redhat.com/browse/DBZ-5463)
* Restart SQL Server task on "Socket closed" exception [DBZ-5478](https://issues.redhat.com/browse/DBZ-5478)
* Augment a uniqueness key filed/value in regex topic naming strategy [DBZ-5480](https://issues.redhat.com/browse/DBZ-5480)
* Support wait/nowait clause in mariadb [DBZ-5485](https://issues.redhat.com/browse/DBZ-5485)
* Adapt create function syntax of mariadb [DBZ-5487](https://issues.redhat.com/browse/DBZ-5487)
* add schema doc from column comments [DBZ-5489](https://issues.redhat.com/browse/DBZ-5489)
* My connector parse the mariadb relevant sequence statement failed [DBZ-5505](https://issues.redhat.com/browse/DBZ-5505)
* Expose default values and enum values in schema history messages [DBZ-5511](https://issues.redhat.com/browse/DBZ-5511)
* Simplify passing of SINK config properties to OffsetBackingStore [DBZ-5513](https://issues.redhat.com/browse/DBZ-5513)
* Support BASE64_URL_SAFE in BinaryHandlingMode [DBZ-5544](https://issues.redhat.com/browse/DBZ-5544)
* Handle Vstream Connection reset [DBZ-5551](https://issues.redhat.com/browse/DBZ-5551)
* Supply partition when comiting offsets with source database [DBZ-5557](https://issues.redhat.com/browse/DBZ-5557)
* Vitess: Filter table.include.list during VStream subscription [DBZ-5572](https://issues.redhat.com/browse/DBZ-5572)
* Improve documentation editing experience by setting attributes for the preview [DBZ-5576](https://issues.redhat.com/browse/DBZ-5576)


### Breaking changes since 2.0.0.Beta1

* Implement object size calculator based on object schema [DBZ-2766](https://issues.redhat.com/browse/DBZ-2766)
* Avoid unnamed Struct schemas [DBZ-4365](https://issues.redhat.com/browse/DBZ-4365)
* Revisit the parameter naming [DBZ-5043](https://issues.redhat.com/browse/DBZ-5043)
* Introduce and centralize message schema versioning [DBZ-5044](https://issues.redhat.com/browse/DBZ-5044)
* Reverse the logic of handling retriable errors - retry by default [DBZ-5244](https://issues.redhat.com/browse/DBZ-5244)
* Change skipped.operations behavior to default to truncate [DBZ-5497](https://issues.redhat.com/browse/DBZ-5497)
* Require Java 11 for tests [DBZ-5568](https://issues.redhat.com/browse/DBZ-5568)


### Fixes since 2.0.0.Beta1

* Source info of incremental snapshot events exports wrong data [DBZ-4329](https://issues.redhat.com/browse/DBZ-4329)
* "No maximum LSN recorded" log message can be spammed on low-activity databases [DBZ-4631](https://issues.redhat.com/browse/DBZ-4631)
* Redis Sink config properties are not passed to DB history  [DBZ-5035](https://issues.redhat.com/browse/DBZ-5035)
* HTTP sink not retrying failing requests [DBZ-5307](https://issues.redhat.com/browse/DBZ-5307)
* Translation from mongodb document to kafka connect schema fails when nested arrays contain no elements [DBZ-5434](https://issues.redhat.com/browse/DBZ-5434)
* Duplicate SCNs on same thread Oracle RAC mode incorrectly processed [DBZ-5439](https://issues.redhat.com/browse/DBZ-5439)
* Typo in postgresql document. [DBZ-5450](https://issues.redhat.com/browse/DBZ-5450)
* Unit test fails on Windows [DBZ-5452](https://issues.redhat.com/browse/DBZ-5452)
* Missing the regex properties validation before start connector of DefaultRegexTopicNamingStrategy  [DBZ-5471](https://issues.redhat.com/browse/DBZ-5471)
* Create Index DDL fails to parse when using TABLESPACE clause with quoted identifier [DBZ-5472](https://issues.redhat.com/browse/DBZ-5472)
* Outbox doesn't check array consistecy properly when it detemines its schema [DBZ-5475](https://issues.redhat.com/browse/DBZ-5475)
* Misleading statistics written to the log [DBZ-5476](https://issues.redhat.com/browse/DBZ-5476)
* Debezium connector task didn't retry when failover in mongodb 5 [DBZ-5479](https://issues.redhat.com/browse/DBZ-5479)
* ReadOnlyIncrementalSnapshotIT testStopSnapshotKafkaSignal randomly fails [DBZ-5483](https://issues.redhat.com/browse/DBZ-5483)
* Better error reporting for signal table failures [DBZ-5484](https://issues.redhat.com/browse/DBZ-5484)
* Oracle DATADUMP DDL cannot be parsed [DBZ-5488](https://issues.redhat.com/browse/DBZ-5488)
* Mysql connector parser the ddl statement failed when including keyword "buckets" [DBZ-5499](https://issues.redhat.com/browse/DBZ-5499)
* duplicate call to config.validateAndRecord() in RedisDatabaseHistory [DBZ-5506](https://issues.redhat.com/browse/DBZ-5506)
* DDL statement couldn't be parsed : mismatched input 'ENGINE' [DBZ-5508](https://issues.redhat.com/browse/DBZ-5508)
* Use “database.dbnames” in SQL Server docs [DBZ-5516](https://issues.redhat.com/browse/DBZ-5516)
* LogMiner DML parser incorrectly interprets concatenation operator inside quoted column value [DBZ-5521](https://issues.redhat.com/browse/DBZ-5521)
* Mysql Connector DDL Parser does not parse all privileges [DBZ-5522](https://issues.redhat.com/browse/DBZ-5522)
* SQL Server random test failures - EventProcessingFailureHandlingIT [DBZ-5525](https://issues.redhat.com/browse/DBZ-5525)
* CREATE TABLE with JSON-based CHECK constraint clause causes MultipleParsingExceptions [DBZ-5526](https://issues.redhat.com/browse/DBZ-5526)
* SQL Server test failure - verifyOffsets [DBZ-5527](https://issues.redhat.com/browse/DBZ-5527)
* Unit test fails on Windows [DBZ-5533](https://issues.redhat.com/browse/DBZ-5533)
* EmbeddedEngine should initialize Connector using SourceConnectorContext [DBZ-5534](https://issues.redhat.com/browse/DBZ-5534)
* Unclear validation error when required field is missing [DBZ-5538](https://issues.redhat.com/browse/DBZ-5538)
* Testsuite is missing server.id in MySQL connector's configuration [DBZ-5539](https://issues.redhat.com/browse/DBZ-5539)
* Support EMPTY column identifier [DBZ-5550](https://issues.redhat.com/browse/DBZ-5550)
* Testsuite doesn't reflect changes to SQLServer connector [DBZ-5554](https://issues.redhat.com/browse/DBZ-5554)
* Use TCCL as the default classloader to load interface implementations [DBZ-5561](https://issues.redhat.com/browse/DBZ-5561)
* max.queue.size.in.bytes is invalid [DBZ-5569](https://issues.redhat.com/browse/DBZ-5569)
* Language type for listings in automatic topic creation [DBZ-5573](https://issues.redhat.com/browse/DBZ-5573)
* Vitess: Handle VStream close unepectedly [DBZ-5579](https://issues.redhat.com/browse/DBZ-5579)
* Unreliable RedisDatabaseHistoryIT [DBZ-5582](https://issues.redhat.com/browse/DBZ-5582)
* Error when parsing alter sql  [DBZ-5587](https://issues.redhat.com/browse/DBZ-5587)
* Field validation errors are misleading for positive, non-zero expectations [DBZ-5588](https://issues.redhat.com/browse/DBZ-5588)
* Mysql connector can't handle the case sensitive of rename/change column statement [DBZ-5589](https://issues.redhat.com/browse/DBZ-5589)
* LIST_VALUE_CLAUSE not allowing TIMESTAMP LITERAL [DBZ-5592](https://issues.redhat.com/browse/DBZ-5592)
* Orcale DDL does not support comments on materialized views [DBZ-5595](https://issues.redhat.com/browse/DBZ-5595)
* Oracle DDL does not support DEFAULT ON NULL [DBZ-5605](https://issues.redhat.com/browse/DBZ-5605)
* Datatype mdsys.sdo_geometry not supported [DBZ-5609](https://issues.redhat.com/browse/DBZ-5609)


### Other changes since 2.0.0.Beta1

* Add signal table automatically to include list [DBZ-3293](https://issues.redhat.com/browse/DBZ-3293)
* No documentation for snapshot.include.collection.list property for Db2 connector [DBZ-4345](https://issues.redhat.com/browse/DBZ-4345)
* Deprecate internal key/value converter options  [DBZ-4617](https://issues.redhat.com/browse/DBZ-4617)
* Run system testsuite inside OpenShift  [DBZ-5165](https://issues.redhat.com/browse/DBZ-5165)
* Upgrade SQL Server driver to 10.2.1.jre8 [DBZ-5290](https://issues.redhat.com/browse/DBZ-5290)
* Rewrite oracle tests pipeline job to matrix job [DBZ-5412](https://issues.redhat.com/browse/DBZ-5412)
* Debezium on ROSA sanity testing [DBZ-5416](https://issues.redhat.com/browse/DBZ-5416)
* Update link format in shared tutorial file [DBZ-5422](https://issues.redhat.com/browse/DBZ-5422)
* Deprecate legacy topic selector for all connectors [DBZ-5457](https://issues.redhat.com/browse/DBZ-5457)
* Remove community conditionalization in signaling doc for Oracle incremental and ad hoc snapshots content [DBZ-5458](https://issues.redhat.com/browse/DBZ-5458)
* Remove the dependency of JdbcConnection on DatabaseSchema [DBZ-5470](https://issues.redhat.com/browse/DBZ-5470)
* Remove SQL Server SourceTimestampMode [DBZ-5477](https://issues.redhat.com/browse/DBZ-5477)
* Maintanence branch builds on connector repos should build against proper branch [DBZ-5492](https://issues.redhat.com/browse/DBZ-5492)
* Upgrade PostgreSQL driver to 42.4.1 [DBZ-5493](https://issues.redhat.com/browse/DBZ-5493)
* Force updating snapshots when building the UI in the workflow [DBZ-5501](https://issues.redhat.com/browse/DBZ-5501)
* Restrict connector workflows based on individual grammar changes in DDL module [DBZ-5528](https://issues.redhat.com/browse/DBZ-5528)
* Disable preferring DDL before logical schema in history recovery [DBZ-5535](https://issues.redhat.com/browse/DBZ-5535)
* Disable Eager loading for federated module bundles.  [DBZ-5545](https://issues.redhat.com/browse/DBZ-5545)
* Missing format value option in debezium-server doc [DBZ-5546](https://issues.redhat.com/browse/DBZ-5546)
* Debezium inputs with number types have the wrong name of the input [DBZ-5553](https://issues.redhat.com/browse/DBZ-5553)
* MySQL read.only property incorrectly appears in downstream documentation [DBZ-5555](https://issues.redhat.com/browse/DBZ-5555)
* Add the Fed module running script and update readme [DBZ-5560](https://issues.redhat.com/browse/DBZ-5560)
* Logging improvements in TestSuite [DBZ-5563](https://issues.redhat.com/browse/DBZ-5563)
* Formatting characters in properties tables rendered in published content [DBZ-5565](https://issues.redhat.com/browse/DBZ-5565)
* Upgrade mysql-binlog-connector-java library version [DBZ-5574](https://issues.redhat.com/browse/DBZ-5574)
* MySQL database.server.id indicates default value is random but that no longer applies [DBZ-5577](https://issues.redhat.com/browse/DBZ-5577)
* Switch test containers to Debezium nightly [DBZ-5601](https://issues.redhat.com/browse/DBZ-5601)
* GitHub CI fails for DB2 connector [DBZ-5606](https://issues.redhat.com/browse/DBZ-5606)
* ValidateSqlServerFiltersIT fails in CI [DBZ-5613](https://issues.redhat.com/browse/DBZ-5613)



## 2.0.0.Beta1
July 26th 2022 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12391139)

### New features since 2.0.0.Alpha3

* Pluggable topic selector [DBZ-4180](https://issues.redhat.com/browse/DBZ-4180)
* Read Debezium Metrics From Debezium Server Consumer [DBZ-5235](https://issues.redhat.com/browse/DBZ-5235)
* Treat SQLServerException with "Broken pipe (Write failed)" exception message as a retriable exception [DBZ-5292](https://issues.redhat.com/browse/DBZ-5292)
* Include user that committed change in metadata (oracle) [DBZ-5358](https://issues.redhat.com/browse/DBZ-5358)
* UI Add  debezium-ui i18n zh translation [DBZ-5379](https://issues.redhat.com/browse/DBZ-5379)
* Support storing extended attributes in relational model and JSON schema history topic [DBZ-5396](https://issues.redhat.com/browse/DBZ-5396)
* Validate topic naming strategy relative topic name properties [DBZ-5414](https://issues.redhat.com/browse/DBZ-5414)
* Verify the unique index whether including function or arbitrary expression [DBZ-5424](https://issues.redhat.com/browse/DBZ-5424)
* Remove the duplicated SimpleDdlParserListener from mysql connector [DBZ-5425](https://issues.redhat.com/browse/DBZ-5425)


### Breaking changes since 2.0.0.Alpha3

* Remove "single partition" mode [DBZ-4726](https://issues.redhat.com/browse/DBZ-4726)
* Define Centralized and Modular aproach for debezium storage [DBZ-5229](https://issues.redhat.com/browse/DBZ-5229)


### Fixes since 2.0.0.Alpha3

* MongoConnector's field exclusion configuration does not work with fields with the same name but from different collections [DBZ-4846](https://issues.redhat.com/browse/DBZ-4846)
* User input are not consistent on Filter step for the DBZ connectors [DBZ-5246](https://issues.redhat.com/browse/DBZ-5246)
* KafkaDatabaseHistory without check database history topic create result  caused UnknowTopicOrPartitionException [DBZ-5249](https://issues.redhat.com/browse/DBZ-5249)
* Lob type data is inconsistent between source and sink, after modifying the primary key [DBZ-5295](https://issues.redhat.com/browse/DBZ-5295)
* Caused by: java.io.EOFException: Failed to read next byte from position 2005308603 [DBZ-5333](https://issues.redhat.com/browse/DBZ-5333)
* Incremental Snapshot: Oracle table name parsing does not support periods in DB name [DBZ-5336](https://issues.redhat.com/browse/DBZ-5336)
* Support PostgreSQL default value function calls with schema prefixes [DBZ-5340](https://issues.redhat.com/browse/DBZ-5340)
* Unsigned tinyint conversion fails for MySQL 8.x [DBZ-5343](https://issues.redhat.com/browse/DBZ-5343)
* Log a warning when an unsupported LogMiner operation is detected for a captured table [DBZ-5351](https://issues.redhat.com/browse/DBZ-5351)
* NullPointerException thrown when unique index based on both system and non-system generated columns [DBZ-5356](https://issues.redhat.com/browse/DBZ-5356)
* MySQL Connector column hash v2 does not work [DBZ-5366](https://issues.redhat.com/browse/DBZ-5366)
* Outbox JSON expansion fails when nested arrays contain no elements [DBZ-5367](https://issues.redhat.com/browse/DBZ-5367)
* docker-maven-plugin needs to be upgraded for Mac Apple M1 [DBZ-5369](https://issues.redhat.com/browse/DBZ-5369)
* AWS DocumentDB (with MongoDB Compatibility) Connect Fail [DBZ-5371](https://issues.redhat.com/browse/DBZ-5371)
* Oracle Xstream does not propagate commit timestamp to transaction metadata [DBZ-5373](https://issues.redhat.com/browse/DBZ-5373)
* UI View connector config in non-first cluster return 404 [DBZ-5378](https://issues.redhat.com/browse/DBZ-5378)
* CommitScn not logged in expected format [DBZ-5381](https://issues.redhat.com/browse/DBZ-5381)
* org.postgresql.util.PSQLException: Bad value for type timestamp/date/time: CURRENT_TIMESTAMP [DBZ-5384](https://issues.redhat.com/browse/DBZ-5384)
* Missing "previousId" property with parsing the rename statement in kafka history topic [DBZ-5386](https://issues.redhat.com/browse/DBZ-5386)
* Check constraint introduces a column based on constraint in the schema change event. [DBZ-5390](https://issues.redhat.com/browse/DBZ-5390)
* The column is referenced as PRIMARY KEY, but a matching column is not defined in table [DBZ-5398](https://issues.redhat.com/browse/DBZ-5398)
* Clarify which database name to use for signal.data.collection when using Oracle with pluggable database support [DBZ-5399](https://issues.redhat.com/browse/DBZ-5399)
* Timestamp with time zone column's default values not in GMT [DBZ-5403](https://issues.redhat.com/browse/DBZ-5403)
* Upgrade to Kafka 3.1 broke build compatibility with Kafka 2.x and Kafka 3.0 [DBZ-5404](https://issues.redhat.com/browse/DBZ-5404)
* PostgresConnectorIT#shouldRecoverFromRetriableException fails randomly [DBZ-5408](https://issues.redhat.com/browse/DBZ-5408)


### Other changes since 2.0.0.Alpha3

* Clean-up unused documentation variables [DBZ-2595](https://issues.redhat.com/browse/DBZ-2595)
* Intermittent test failures on CI: EventProcessingFailureHandlingIT [DBZ-4004](https://issues.redhat.com/browse/DBZ-4004)
* Clarify whether SQL Server on Azure is a supported configuration or not [DBZ-4312](https://issues.redhat.com/browse/DBZ-4312)
* Remove redundant setting of last events [DBZ-5047](https://issues.redhat.com/browse/DBZ-5047)
* Rename `docker-images` repository and JIRA component to `container-images` [DBZ-5048](https://issues.redhat.com/browse/DBZ-5048)
* Update instructions for deploying Debezium on RHEL (downstream-only change) [DBZ-5293](https://issues.redhat.com/browse/DBZ-5293)
* Add ts_ms field to examples of transaction boundary events and examples and update property description in documentation [DBZ-5334](https://issues.redhat.com/browse/DBZ-5334)
* Oracle GitHub actions workflow no longer run tests on pushes [DBZ-5349](https://issues.redhat.com/browse/DBZ-5349)
* Unify job names in jenkins system-tests [DBZ-5392](https://issues.redhat.com/browse/DBZ-5392)
* Build stable branches for connector-specific repos [DBZ-5409](https://issues.redhat.com/browse/DBZ-5409)
* Oracle non-cdb builds do not use the correct environment settings [DBZ-5411](https://issues.redhat.com/browse/DBZ-5411)
* Update the topic naming strategy doc to all connectors [DBZ-5413](https://issues.redhat.com/browse/DBZ-5413)
* Address User guide review comments for Oracle connector [DBZ-5418](https://issues.redhat.com/browse/DBZ-5418)
* OracleSchemaMigrationIT fails on non-pluggable (non-CDB) databases [DBZ-5419](https://issues.redhat.com/browse/DBZ-5419)



## 2.0.0.Alpha3
July 1st 2022 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12385342)

### New features since 2.0.0.Alpha2

* Mysql Commit Timestamp [DBZ-5170](https://issues.redhat.com/browse/DBZ-5170)
* Include event scn in Oracle records [DBZ-5225](https://issues.redhat.com/browse/DBZ-5225)
* Redis Store does not work with GCP Managed Redis [DBZ-5268](https://issues.redhat.com/browse/DBZ-5268)


### Breaking changes since 2.0.0.Alpha2

None


### Fixes since 2.0.0.Alpha2

* Incorrect loading of LSN from offsets [DBZ-3942](https://issues.redhat.com/browse/DBZ-3942)
* Database history recovery will retain old tables after they've been renamed [DBZ-4451](https://issues.redhat.com/browse/DBZ-4451)
* Adding new table with incremental snapshots not working [DBZ-4834](https://issues.redhat.com/browse/DBZ-4834)
* BigDecimal has mismatching scale value for given Decimal schema [DBZ-4890](https://issues.redhat.com/browse/DBZ-4890)
* Debezium has never found starting LSN [DBZ-5031](https://issues.redhat.com/browse/DBZ-5031)
* Data duplication problem using postgresql source on debezium server [DBZ-5070](https://issues.redhat.com/browse/DBZ-5070)
* Cursor fetch is used for all results during connection [DBZ-5084](https://issues.redhat.com/browse/DBZ-5084)
* Debezuim connector fails at parsing select statement overrides when table name has space [DBZ-5198](https://issues.redhat.com/browse/DBZ-5198)
* DDL statement couldn't be parsed 2 - Oracle connector 1.9.3.Final [DBZ-5230](https://issues.redhat.com/browse/DBZ-5230)
* Debezium server duplicates scripting jar files [DBZ-5232](https://issues.redhat.com/browse/DBZ-5232)
* Cannot convert field type tinyint(1) unsigned to boolean [DBZ-5236](https://issues.redhat.com/browse/DBZ-5236)
* Oracle unparsable ddl create table [DBZ-5237](https://issues.redhat.com/browse/DBZ-5237)
* Postgres Incremental Snapshot on parent partitioned table not working [DBZ-5240](https://issues.redhat.com/browse/DBZ-5240)
* Character set influencers are not properly parsed on default values [DBZ-5241](https://issues.redhat.com/browse/DBZ-5241)
* Dupicate SCNs on Oracle RAC installations incorrectly processed [DBZ-5245](https://issues.redhat.com/browse/DBZ-5245)
* NPE when using Debezium Embedded in Quarkus [DBZ-5251](https://issues.redhat.com/browse/DBZ-5251)
* Oracle LogMiner may fail with an in-progress transaction in an archive log that has been deleted [DBZ-5256](https://issues.redhat.com/browse/DBZ-5256)
* Order of source block table names in a rename schema change event is not deterministic [DBZ-5257](https://issues.redhat.com/browse/DBZ-5257)
* Debezium fails to connect to replicaset if a node is down [DBZ-5260](https://issues.redhat.com/browse/DBZ-5260)
* No changes to commit_scn when oracle-connector got new lob data [DBZ-5266](https://issues.redhat.com/browse/DBZ-5266)
* Invalid date 'SEPTEMBER 31' [DBZ-5267](https://issues.redhat.com/browse/DBZ-5267)
* database.history.store.only.captured.tables.ddl not suppressing logs [DBZ-5270](https://issues.redhat.com/browse/DBZ-5270)
* io.debezium.text.ParsingException: DDL statement couldn't be parsed [DBZ-5271](https://issues.redhat.com/browse/DBZ-5271)
* Deadlock during snapshot with Mongo connector [DBZ-5272](https://issues.redhat.com/browse/DBZ-5272)
* Mysql parser is not able to handle variables in KILL command [DBZ-5273](https://issues.redhat.com/browse/DBZ-5273)
* Debezium server fail when connect to Azure Event Hubs [DBZ-5279](https://issues.redhat.com/browse/DBZ-5279)
* ORA-01086 savepoint never established raised when database history topic cannot be created or does not exist [DBZ-5281](https://issues.redhat.com/browse/DBZ-5281)
* Enabling database.history.store.only.captured.tables.ddl does not restrict history topic records [DBZ-5285](https://issues.redhat.com/browse/DBZ-5285)


### Other changes since 2.0.0.Alpha2

* Add script SMT test case to OCP test suite [DBZ-2581](https://issues.redhat.com/browse/DBZ-2581)
* Confusing example for schema change topic [DBZ-4713](https://issues.redhat.com/browse/DBZ-4713)
* Update cache-invalidation example [DBZ-4754](https://issues.redhat.com/browse/DBZ-4754)
* Switch from static yaml descriptors to dynamic objects [DBZ-4830](https://issues.redhat.com/browse/DBZ-4830)
* Verify that snapshot deployments build and deploy javadocs [DBZ-4875](https://issues.redhat.com/browse/DBZ-4875)
* DelayStrategy should accept Duration rather than long ms [DBZ-4902](https://issues.redhat.com/browse/DBZ-4902)
* Use maven 3.8.4 version with enforcer plugin [DBZ-5069](https://issues.redhat.com/browse/DBZ-5069)
* Add option for '*' wildcard usage testsuite preparation jenkins jobs [DBZ-5190](https://issues.redhat.com/browse/DBZ-5190)
* Use the Maven wrapper in the Github and Jenkins workflows [DBZ-5207](https://issues.redhat.com/browse/DBZ-5207)
* Improve performance of OracleConnectorIT shouldIgnoreAllTablesInExcludedSchemas test [DBZ-5226](https://issues.redhat.com/browse/DBZ-5226)
* Document use of JAR artifact to build Debezium scripting SMT into Kafka Connect [DBZ-5227](https://issues.redhat.com/browse/DBZ-5227)
* Create shared adoc fragments for specifying MBean name format in connector metrics sections [DBZ-5233](https://issues.redhat.com/browse/DBZ-5233)
* Build Oracle connector by default without Maven profiles [DBZ-5234](https://issues.redhat.com/browse/DBZ-5234)
* Remove reference to removed case insensitive option in Oracle README.md [DBZ-5250](https://issues.redhat.com/browse/DBZ-5250)
* Several Oracle tests do not get database name from TestHelper [DBZ-5258](https://issues.redhat.com/browse/DBZ-5258)
* Upgrade to Quarkus 2.10.0.Final [DBZ-5259](https://issues.redhat.com/browse/DBZ-5259)
* Upgrade PostgreSQL driver to 42.4.0 [DBZ-5261](https://issues.redhat.com/browse/DBZ-5261)
* Refactor ChangeEventQueue to better support n:1 threads [DBZ-5277](https://issues.redhat.com/browse/DBZ-5277)
* Upgrade MongoDB driver to 4.6.1 [DBZ-5287](https://issues.redhat.com/browse/DBZ-5287)



## 2.0.0.Alpha2
June 9th 2022 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12385341)

### New features since 2.0.0.Alpha1

* Provide a signal to stop the running incremental snapshot [DBZ-4251](https://issues.redhat.com/browse/DBZ-4251)
* SQL Server - Fail connector when a user doesn't have the right permission (CDCReader) [DBZ-4346](https://issues.redhat.com/browse/DBZ-4346)
* Allow mongodb-connector to decode Binary payloads [DBZ-4600](https://issues.redhat.com/browse/DBZ-4600)
* Add UI backend tests for SQL Server connector [DBZ-4867](https://issues.redhat.com/browse/DBZ-4867)
* direct usage of debezium engine ignores ChangeConsumer.supportsTombstoneEvents [DBZ-5052](https://issues.redhat.com/browse/DBZ-5052)
* Config the cache size property for ByLogicalTableRouter caches [DBZ-5072](https://issues.redhat.com/browse/DBZ-5072)
* Introduce a new extension api for query debezium version [DBZ-5092](https://issues.redhat.com/browse/DBZ-5092)
* Introduce a new field "ts_ms" to identify the process time for schema change event [DBZ-5098](https://issues.redhat.com/browse/DBZ-5098)
* MongoDB Connector should use RawBsonDocument instead of Document [DBZ-5113](https://issues.redhat.com/browse/DBZ-5113)


### Breaking changes since 2.0.0.Alpha1

* Debezium MySql connector does not capture floating point numbers with the right precision [DBZ-3865](https://issues.redhat.com/browse/DBZ-3865)
* Remove oplog support from MongoDB connector [DBZ-4951](https://issues.redhat.com/browse/DBZ-4951)
* Introduce multi-partition/task code to all connectors [DBZ-5042](https://issues.redhat.com/browse/DBZ-5042)
* Clean-up connector parameters [DBZ-5045](https://issues.redhat.com/browse/DBZ-5045)


### Fixes since 2.0.0.Alpha1

* Postgres existing publication is not updated with the new table [DBZ-3921](https://issues.redhat.com/browse/DBZ-3921)
* Error and connector stops when DDL contains lateral [DBZ-4780](https://issues.redhat.com/browse/DBZ-4780)
* Schema changes should flush SCN to offsets if there are no other active transactions [DBZ-4782](https://issues.redhat.com/browse/DBZ-4782)
* Connector stops streaming after a re-balance [DBZ-4792](https://issues.redhat.com/browse/DBZ-4792)
* MySQL connector increment snapshot failed parse datetime column lenth when connector set "snapshot.fetch.size": 20000  [DBZ-4939](https://issues.redhat.com/browse/DBZ-4939)
* [MySQL Debezium] DDL Parsing error - CREATE OR REPLACE TABLE [DBZ-4958](https://issues.redhat.com/browse/DBZ-4958)
* InstanceAlreadyExistsException during MongoDb connector metrics registration [DBZ-5011](https://issues.redhat.com/browse/DBZ-5011)
* DateTimeParseException: Text 'infinity' could not be parsed in Postgres connector [DBZ-5014](https://issues.redhat.com/browse/DBZ-5014)
* PostgreSQL ENUM default values are missing from generated schema [DBZ-5038](https://issues.redhat.com/browse/DBZ-5038)
* Debezium official documentation typo [DBZ-5040](https://issues.redhat.com/browse/DBZ-5040)
* Fix inconsistent transaction id when handling transactional messages in Vitess connector [DBZ-5063](https://issues.redhat.com/browse/DBZ-5063)
* 4 Connections per connector (postgres) [DBZ-5074](https://issues.redhat.com/browse/DBZ-5074)
* Oracle documentation refers to archive_log_target rather than archive_lag_target [DBZ-5076](https://issues.redhat.com/browse/DBZ-5076)
* 'ALTER TABLE mytable DROP FOREIGN KEY IF EXISTS mytable_fk' no viable alternative at input 'ALTER TABLE mytable DROP FOREIGN KEY IF' [DBZ-5077](https://issues.redhat.com/browse/DBZ-5077)
* Oracle Logminer: records missed during switch from snapshot to streaming mode [DBZ-5085](https://issues.redhat.com/browse/DBZ-5085)
* Interrupting a snapshot process can hang for some JDBC drivers [DBZ-5087](https://issues.redhat.com/browse/DBZ-5087)
* Debezium fails to undo change event due to transaction id ending in ffffffff with LogMiner [DBZ-5090](https://issues.redhat.com/browse/DBZ-5090)
* Table changes are not filled in schema changes from snapshot [DBZ-5096](https://issues.redhat.com/browse/DBZ-5096)
* Postgresql connector does not retry one some errors when postgres is taken offline [DBZ-5097](https://issues.redhat.com/browse/DBZ-5097)
* Parsing zero day fails [DBZ-5099](https://issues.redhat.com/browse/DBZ-5099)
* Cannot Set debezium.sink.kafka.producer.ssl.endpoint.identification.algorithm to empty value  [DBZ-5105](https://issues.redhat.com/browse/DBZ-5105)
* Debezium connector failed with create table statement [DBZ-5108](https://issues.redhat.com/browse/DBZ-5108)
* Current version of surefire/failsafe skips tests on failure in BeforeAll [DBZ-5112](https://issues.redhat.com/browse/DBZ-5112)


### Other changes since 2.0.0.Alpha1

* Restructure documentation for custom converters [DBZ-4588](https://issues.redhat.com/browse/DBZ-4588)
* Document *xmin.fetch.interval.ms* property for Postgres connector [DBZ-4734](https://issues.redhat.com/browse/DBZ-4734)
* Update to Quarkus 2.9.2.Final [DBZ-4806](https://issues.redhat.com/browse/DBZ-4806)
* Upgrade Oracle driver to 21.5.0.0 [DBZ-4877](https://issues.redhat.com/browse/DBZ-4877)
* Execute Debezium UI build when core library is changed [DBZ-4947](https://issues.redhat.com/browse/DBZ-4947)
* Remove unused Oracle connector code [DBZ-4973](https://issues.redhat.com/browse/DBZ-4973)
* Links to cassandra 3 and 4 artifacts no longer work for Debezium 1.9+ [DBZ-5055](https://issues.redhat.com/browse/DBZ-5055)
* Align Postgresql driver with Quarkus [DBZ-5060](https://issues.redhat.com/browse/DBZ-5060)
* Outdated links in Javadoc documentation [DBZ-5075](https://issues.redhat.com/browse/DBZ-5075)
* Rename "Mysql" to "MySql" in related MysqlFieldReader interface [DBZ-5078](https://issues.redhat.com/browse/DBZ-5078)
* Create CI job for maven repository verification [DBZ-5082](https://issues.redhat.com/browse/DBZ-5082)
* Remove database.server.id default value handler, no longer auto-generated. [DBZ-5100](https://issues.redhat.com/browse/DBZ-5100)
* Upgrade Jackson Databind to 2.13.2.2 [DBZ-5107](https://issues.redhat.com/browse/DBZ-5107)
* Switch to released version of Fixture5 extension in System testsuite [DBZ-5114](https://issues.redhat.com/browse/DBZ-5114)



## 2.0.0.Alpha1
April 28th 2022 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12380203)

### New features since 1.9.0.Final

* Implement Pub/Sub Lite change consumer [DBZ-4450](https://issues.redhat.com/browse/DBZ-4450)
* Include Instant Client in Docker build for Oracle in Tutorial examples [DBZ-1013](https://issues.redhat.com/browse/DBZ-1013)
* Add Google Pub/Sub emulator support [DBZ-4491](https://issues.redhat.com/browse/DBZ-4491)
* Making Postgres `PSQLException: This connection has been closed.` retriable [DBZ-4948](https://issues.redhat.com/browse/DBZ-4948)
* ORA-04030: out of process memory when trying to allocate 65568 bytes (Logminer LCR c,krvxrib:buffer) [DBZ-4963](https://issues.redhat.com/browse/DBZ-4963)
* Should store event header timestamp in HistoryRecord [DBZ-4998](https://issues.redhat.com/browse/DBZ-4998)
* DBZ-UI: In the Edit/Duplicate connector flow make the access/secret key/password/Client Secret filed as editable. [DBZ-5001](https://issues.redhat.com/browse/DBZ-5001)
* adjust LogMiner batch size based on comparison with currently used batch size [DBZ-5005](https://issues.redhat.com/browse/DBZ-5005)


### Breaking changes since 1.9.0.Final

* Phase out a code supporting old version of protobuf decoder [DBZ-703](https://issues.redhat.com/browse/DBZ-703)
* Remove wal2json support [DBZ-4156](https://issues.redhat.com/browse/DBZ-4156)
* Remove legacy implementation of MySQL connector [DBZ-4950](https://issues.redhat.com/browse/DBZ-4950)
* Remove Confluent Avro converters from connect-base image [DBZ-4952](https://issues.redhat.com/browse/DBZ-4952)
* Remove JDBC legacy date time properties from MySQL connector [DBZ-4965](https://issues.redhat.com/browse/DBZ-4965)
* Use Maven 3.8.4 for Debezium builds [DBZ-5064](https://issues.redhat.com/browse/DBZ-5064)
* Switch to Java 11 as a baseline [DBZ-4949](https://issues.redhat.com/browse/DBZ-4949)


### Fixes since 1.9.0.Final

* Connector throws java.lang.ArrayIndexOutOfBoundsException [DBZ-3848](https://issues.redhat.com/browse/DBZ-3848)
* Document no relevant tables should be in the SYS or SYSTEM tablespaces. [DBZ-4762](https://issues.redhat.com/browse/DBZ-4762)
* Getting java.sql.SQLException: ORA-01291: missing logfile while running with archive log only [DBZ-4879](https://issues.redhat.com/browse/DBZ-4879)
* Debezium uses wrong LCR format for Oracle 12.1 [DBZ-4932](https://issues.redhat.com/browse/DBZ-4932)
* Oracle duplicates on connector restart [DBZ-4936](https://issues.redhat.com/browse/DBZ-4936)
* Oracle truncate causes exception [DBZ-4953](https://issues.redhat.com/browse/DBZ-4953)
* NPE caused by io.debezium.connector.oracle.antlr.listener.ColumnDefinitionParserListener.resolveColumnDataType [DBZ-4976](https://issues.redhat.com/browse/DBZ-4976)
* Oracle connector may throw NullPointerException when stopped after an unsuccessful startup [DBZ-4978](https://issues.redhat.com/browse/DBZ-4978)
* NPE for non-table related DDLs [DBZ-4979](https://issues.redhat.com/browse/DBZ-4979)
* CTE statements aren't parsed by MySQL connector [DBZ-4980](https://issues.redhat.com/browse/DBZ-4980)
* Missing SSL configuration option in the debezium mongodb connector UI [DBZ-4981](https://issues.redhat.com/browse/DBZ-4981)
* Unsupported MySQL Charsets during Snapshotting for fields with custom converter [DBZ-4983](https://issues.redhat.com/browse/DBZ-4983)
* Outbox Transform does not allow expanded payload with additional fields in the envelope [DBZ-4989](https://issues.redhat.com/browse/DBZ-4989)
* Redis Sink - clientSetname is taking place before auth [DBZ-4993](https://issues.redhat.com/browse/DBZ-4993)
* CLOB with single quotes causes parser exception [DBZ-4994](https://issues.redhat.com/browse/DBZ-4994)
* Oracle DDL parser fails on references_clause with no column list [DBZ-4996](https://issues.redhat.com/browse/DBZ-4996)
* Can't use 'local' database through mongos [DBZ-5003](https://issues.redhat.com/browse/DBZ-5003)
* Triggering Incremental Snapshot on MongoDB connector throws json parsing error [DBZ-5015](https://issues.redhat.com/browse/DBZ-5015)
* Jenkins jobs fail to download debezium-bom [DBZ-5017](https://issues.redhat.com/browse/DBZ-5017)
* Redis Sink - Check if client is not null before closing it [DBZ-5019](https://issues.redhat.com/browse/DBZ-5019)
* Cassandra 3 handler does not process partition deletions correctly [DBZ-5022](https://issues.redhat.com/browse/DBZ-5022)
* Keyspaces should be initialised in all schema change listeners on sessions startup. [DBZ-5023](https://issues.redhat.com/browse/DBZ-5023)
* SQL Server in multi-partition mode fails if a new database is added to an existing configuration [DBZ-5033](https://issues.redhat.com/browse/DBZ-5033)
* Mysql tests start before MySQL DB constainer is running [DBZ-5054](https://issues.redhat.com/browse/DBZ-5054)
* Debezium server configuration properties not rendered correctly [DBZ-5058](https://issues.redhat.com/browse/DBZ-5058)


### Other changes since 1.9.0.Final

* Add integration test for Oracle database.url configurations [DBZ-3318](https://issues.redhat.com/browse/DBZ-3318)
* Build Cassandra 3.x connector with Java 11 [DBZ-4910](https://issues.redhat.com/browse/DBZ-4910)
* Add ignoreSnapshots build option to release pipeline [DBZ-4957](https://issues.redhat.com/browse/DBZ-4957)
* Update Pulsar client version used by Debezium Server [DBZ-4961](https://issues.redhat.com/browse/DBZ-4961)
* Intermittent failure of RedisStreamIT.testRedisConnectionRetry [DBZ-4966](https://issues.redhat.com/browse/DBZ-4966)
* Add triggers for 2.x paths in Github CI [DBZ-4971](https://issues.redhat.com/browse/DBZ-4971)
* Debezium raised an exception and the task was still running [DBZ-4987](https://issues.redhat.com/browse/DBZ-4987)
* Nexus Staging Maven plugin is incompatible with OpenJDK 17 [DBZ-5025](https://issues.redhat.com/browse/DBZ-5025)
* Duplicate definition of Maven plugins [DBZ-5026](https://issues.redhat.com/browse/DBZ-5026)
* OracleOffsetContextTest should be scoped to LogMiner only [DBZ-5028](https://issues.redhat.com/browse/DBZ-5028)
* Scope several new Oracle tests to LogMiner only [DBZ-5029](https://issues.redhat.com/browse/DBZ-5029)
* Failure in jdk outreach jobs [DBZ-5041](https://issues.redhat.com/browse/DBZ-5041)
* Update artifact server job listing script [DBZ-5051](https://issues.redhat.com/browse/DBZ-5051)
* Add FAQ about ORA-01882 and Oracle 11 to documentation [DBZ-5057](https://issues.redhat.com/browse/DBZ-5057)
* Upgrade to Quarkus 2.8.2.Final [DBZ-5062](https://issues.redhat.com/browse/DBZ-5062)



## 1.9.0.Final
April 5th 2022 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12379896)

### New features since 1.9.0.CR1

* Ability to support all Redis connection schemes [DBZ-4511](https://issues.redhat.com/browse/DBZ-4511)
* pass SINK config properties to OffsetStore and DatabaseHistory adapters [DBZ-4864](https://issues.redhat.com/browse/DBZ-4864)
* Migrate test-suite fixtures to JUnit extension [DBZ-4892](https://issues.redhat.com/browse/DBZ-4892)
* Use Jedis' clientSetname when establishing Redis connections [DBZ-4911](https://issues.redhat.com/browse/DBZ-4911)


### Breaking changes since 1.9.0.CR1

None


### Fixes since 1.9.0.CR1

* MySQL connector fails to parse default integer value expressed as decimal [DBZ-3541](https://issues.redhat.com/browse/DBZ-3541)
* Cannot use Secrets in Debezium server connector config [DBZ-4742](https://issues.redhat.com/browse/DBZ-4742)
* spatial_ref_sys table should be excluded in Postgres connector [DBZ-4814](https://issues.redhat.com/browse/DBZ-4814)
* Oracle: Parsing failed for SEL_LOB_LOCATOR sql: 'DECLARE [DBZ-4862](https://issues.redhat.com/browse/DBZ-4862)
* Oracle connector stops calling logminer without any error message [DBZ-4884](https://issues.redhat.com/browse/DBZ-4884)
* Single quotes replication  [DBZ-4891](https://issues.redhat.com/browse/DBZ-4891)
* Oracle keeps trying old scn even if it had no changes [DBZ-4907](https://issues.redhat.com/browse/DBZ-4907)
* Redis Sink - using Transaction does not work in sharded Redis  [DBZ-4912](https://issues.redhat.com/browse/DBZ-4912)
* Oracle connector page have typo since version 1.5. [DBZ-4913](https://issues.redhat.com/browse/DBZ-4913)
* CVE-2022-26520 jdbc-postgresql: postgresql-jdbc: Arbitrary File Write Vulnerability [rhint-debezium-1] [DBZ-4916](https://issues.redhat.com/browse/DBZ-4916)
* Kafka topics list throw exception [DBZ-4920](https://issues.redhat.com/browse/DBZ-4920)
* Spelling mistake in doc about Oracle metrics [DBZ-4926](https://issues.redhat.com/browse/DBZ-4926)
* MariaDB Trigger Parsing Error [DBZ-4927](https://issues.redhat.com/browse/DBZ-4927)
* NPE during snapshotting MySQL database if custom converters present and column is null [DBZ-4933](https://issues.redhat.com/browse/DBZ-4933)
* Avro converter requires Guava in lib directory [DBZ-4935](https://issues.redhat.com/browse/DBZ-4935)
* Debezium Server 1.9 Fails to start up when transferring 1.8 offsets [DBZ-4937](https://issues.redhat.com/browse/DBZ-4937)
* Missing images for 1.9.0.Beta1 and 1.9.0.CR1 releases [DBZ-4943](https://issues.redhat.com/browse/DBZ-4943)


### Other changes since 1.9.0.CR1

* Document "schema.include.list"/"schema.exclude.list" for SQL Server connector [DBZ-2793](https://issues.redhat.com/browse/DBZ-2793)
* Align decimal.handling.mode documentation for Oracle like other connectors [DBZ-3317](https://issues.redhat.com/browse/DBZ-3317)
* Use Red Hat Maven repo for custom build image in docs [DBZ-4392](https://issues.redhat.com/browse/DBZ-4392)
* Upgrade postgres driver to version 42.3.3 [DBZ-4919](https://issues.redhat.com/browse/DBZ-4919)
* Update Quality Outreach workflow to official Oracle Java GH action [DBZ-4924](https://issues.redhat.com/browse/DBZ-4924)
* Bump jackson to 2.13.2 [DBZ-4955](https://issues.redhat.com/browse/DBZ-4955)



## 1.9.0.CR1
March 25th 2022 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12379895)

### New features since 1.9.0.Beta1

* Add support for Cassandra 4.x [DBZ-2514](https://issues.redhat.com/browse/DBZ-2514)
* Exclude dummy events from database history [DBZ-3762](https://issues.redhat.com/browse/DBZ-3762)
* Define how MCS container images should be build [DBZ-4006](https://issues.redhat.com/browse/DBZ-4006)
* Document kafka-connect-offset related properties [DBZ-4014](https://issues.redhat.com/browse/DBZ-4014)
* Update UI dependency and it's configuration accordingly  [DBZ-4636](https://issues.redhat.com/browse/DBZ-4636)
* Save and load database history in Redis [DBZ-4771](https://issues.redhat.com/browse/DBZ-4771)
* Provide the Federated module UI component for DBZ Connector edit Flow [DBZ-4785](https://issues.redhat.com/browse/DBZ-4785)
* Switch to fabric8 model provided by Apicurio team [DBZ-4790](https://issues.redhat.com/browse/DBZ-4790)
* Merge the Data and Runtime option page in federated component. [DBZ-4804](https://issues.redhat.com/browse/DBZ-4804)
* Add task id and partition to the logging context for multi-partition connectors [DBZ-4809](https://issues.redhat.com/browse/DBZ-4809)
* run.sh is not working in windows environment [DBZ-4821](https://issues.redhat.com/browse/DBZ-4821)
* Log the tableId is null when filter out some tables [DBZ-4823](https://issues.redhat.com/browse/DBZ-4823)
* Debezium Mysql connector can't handle CREATE INDEX IF NOT EXISTS (MariaDB) [DBZ-4841](https://issues.redhat.com/browse/DBZ-4841)
* Postgresql connector prints uninformative log on snapshot phase [DBZ-4861](https://issues.redhat.com/browse/DBZ-4861)


### Breaking changes since 1.9.0.Beta1

None


### Fixes since 1.9.0.Beta1

* SchemaNameAdjuster is too restrictive by default [DBZ-3535](https://issues.redhat.com/browse/DBZ-3535)
* CVE-2022-21363 mysql-connector-java: Difficult to exploit vulnerability allows high privileged attacker with network access via multiple protocols to compromise MySQL Connectors [rhint-debezium-1] [DBZ-4758](https://issues.redhat.com/browse/DBZ-4758)
* java.lang.NullPointerException while handling DROP column query [DBZ-4786](https://issues.redhat.com/browse/DBZ-4786)
* Not reading the keystore/truststore when enabling MySQL SSL authentication [DBZ-4787](https://issues.redhat.com/browse/DBZ-4787)
* "DebeziumException: Unable to find primary from MongoDB connection" post upgrade to 1.8.1 [DBZ-4802](https://issues.redhat.com/browse/DBZ-4802)
* Oracle TO_DATE cannot be parsed when NLS parameter is provided [DBZ-4810](https://issues.redhat.com/browse/DBZ-4810)
* Oracle test FlushStrategyIT fails [DBZ-4819](https://issues.redhat.com/browse/DBZ-4819)
* Mysql: Getting ERROR `Failed due to error: connect.errors.ConnectException: For input string: "false"` [DBZ-4822](https://issues.redhat.com/browse/DBZ-4822)
* Expect the null value with snapshot CapturedTables metric when skipping snapshotting [DBZ-4824](https://issues.redhat.com/browse/DBZ-4824)
* MySQL 5.7 - no viable alternative at input 'ALTER TABLE ORD_ALLOCATION_CONFIG CHANGE RANK' [DBZ-4833](https://issues.redhat.com/browse/DBZ-4833)
* missing notes on using db2 connector [DBZ-4835](https://issues.redhat.com/browse/DBZ-4835)
* ParsingException when adding a new table to an existing oracle connector [DBZ-4836](https://issues.redhat.com/browse/DBZ-4836)
* Supplemental log check fails when restarting connector after table dropped [DBZ-4842](https://issues.redhat.com/browse/DBZ-4842)
* CREATE_TOPIC docker image regression [DBZ-4844](https://issues.redhat.com/browse/DBZ-4844)
* Logminer mining session stopped due to several kinds of SQL exceptions [DBZ-4850](https://issues.redhat.com/browse/DBZ-4850)
* DDL statement couldn't be parsed [DBZ-4851](https://issues.redhat.com/browse/DBZ-4851)
* Gracefully pass unsupported column types from DDL parser as OracleTypes.OTHER [DBZ-4852](https://issues.redhat.com/browse/DBZ-4852)
* Debezium oracle connector stopped because of Unsupported column type: LONG  [DBZ-4853](https://issues.redhat.com/browse/DBZ-4853)
* Compilation of SqlServerConnectorIntegrator fails [DBZ-4856](https://issues.redhat.com/browse/DBZ-4856)
* Maven cannot compile  debezium-microbenchmark-oracle [DBZ-4860](https://issues.redhat.com/browse/DBZ-4860)
* oracle connector fails because of Supplemental logging not properly configured  [DBZ-4869](https://issues.redhat.com/browse/DBZ-4869)
* Re-read incremental snapshot chunk on DDL event [DBZ-4878](https://issues.redhat.com/browse/DBZ-4878)
* oracle connector fails because of unsupported column type nclob  [DBZ-4880](https://issues.redhat.com/browse/DBZ-4880)
* Debezium throws CNFE for Avro converter [DBZ-4885](https://issues.redhat.com/browse/DBZ-4885)


### Other changes since 1.9.0.Beta1

* OpenShift deployment instruction improvements [DBZ-2594](https://issues.redhat.com/browse/DBZ-2594)
* Add Kubernetes version of deployment page [DBZ-2646](https://issues.redhat.com/browse/DBZ-2646)
* Log DML replication events instead of throwing an error [DBZ-3949](https://issues.redhat.com/browse/DBZ-3949)
* Review SqlServerConnector properties [DBZ-4052](https://issues.redhat.com/browse/DBZ-4052)
* Promote Outbox Quarkus extension to stable [DBZ-4430](https://issues.redhat.com/browse/DBZ-4430)
* Restructure Oracle connector documentation [DBZ-4436](https://issues.redhat.com/browse/DBZ-4436)
* Downstream docs for outbox event routing SMTs [DBZ-4652](https://issues.redhat.com/browse/DBZ-4652)
* Promote incremental snapshots to stable and GA [DBZ-4655](https://issues.redhat.com/browse/DBZ-4655)
* Remove legacy --zookeeper option from example instructions [DBZ-4660](https://issues.redhat.com/browse/DBZ-4660)
* Use JdbcConfiguration instead of Configuration for JDBC config values [DBZ-4801](https://issues.redhat.com/browse/DBZ-4801)
* Don't set truststore/keystore parameters to system variables [DBZ-4832](https://issues.redhat.com/browse/DBZ-4832)
* Docs: JDBC driver should go to Oracle connector dir [DBZ-4883](https://issues.redhat.com/browse/DBZ-4883)



## 1.9.0.Beta1
March 3rd 2022 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12379893)

### New features since 1.9.0.Alpha2

* Support Knative Eventing [DBZ-2097](https://issues.redhat.com/browse/DBZ-2097)
* Provide UI option to view the configuration of the registered Debezium connector  [DBZ-3137](https://issues.redhat.com/browse/DBZ-3137)
* Handle out of order transaction start event [DBZ-4287](https://issues.redhat.com/browse/DBZ-4287)
* Partition-scoped metrics for the SQL Server connector [DBZ-4478](https://issues.redhat.com/browse/DBZ-4478)
* Save and load offsets in Redis [DBZ-4509](https://issues.redhat.com/browse/DBZ-4509)
* Debezium Deploy Snapshots job is blocked for a long time [DBZ-4628](https://issues.redhat.com/browse/DBZ-4628)
* Change DBZ UI Frontend to use new `data_shape` fields for Kafka message format [DBZ-4714](https://issues.redhat.com/browse/DBZ-4714)
* Expect plain value instead of scientific exponential notation when using decimal string mode [DBZ-4730](https://issues.redhat.com/browse/DBZ-4730)


### Breaking changes since 1.9.0.Alpha2

None


### Fixes since 1.9.0.Alpha2

* Long running transaction in Debezium 1.2.0 (PostgreSQL) [DBZ-2306](https://issues.redhat.com/browse/DBZ-2306)
* "snapshot.include.collection.list" doesn't work with the new MySQL connector implementation [DBZ-3952](https://issues.redhat.com/browse/DBZ-3952)
* When running the NPM build I always end up with an updated/diverged package-lock.json [DBZ-4622](https://issues.redhat.com/browse/DBZ-4622)
* Upgrade of Oracle connector causes NullPointerException [DBZ-4635](https://issues.redhat.com/browse/DBZ-4635)
* Oracle-Connector fails parsing a DDL statement (external tables) [DBZ-4641](https://issues.redhat.com/browse/DBZ-4641)
* oracle-connector DDL statement couldn't be parsed [DBZ-4662](https://issues.redhat.com/browse/DBZ-4662)
* Oracle parsing error for ALTER TABLE EXT_SIX LOCATION [DBZ-4706](https://issues.redhat.com/browse/DBZ-4706)
* MySQL unparseable DDL - CREATE PROCEDURE  [DBZ-4707](https://issues.redhat.com/browse/DBZ-4707)
* Source timestamp timezone differs between snapshot and streaming records [DBZ-4715](https://issues.redhat.com/browse/DBZ-4715)
* Document that Oracle Xstream emits DBMS_LOB method calls as separate events [DBZ-4716](https://issues.redhat.com/browse/DBZ-4716)
* ORA-00308 raised due to offset SCN not being updated in a low traffic environment [DBZ-4718](https://issues.redhat.com/browse/DBZ-4718)
* Property "log.mining.view.fetch.size" does not take effect [DBZ-4723](https://issues.redhat.com/browse/DBZ-4723)
* Postgres debezium send wrong value of column has default NULL::::character varying in kafka message  [DBZ-4736](https://issues.redhat.com/browse/DBZ-4736)
* Oracle Logminer: streaming start offset is off by one [DBZ-4737](https://issues.redhat.com/browse/DBZ-4737)
* Apache Pulsar example doesn't work [DBZ-4739](https://issues.redhat.com/browse/DBZ-4739)
* Oracle dbname/signal with dots parsed incorrectly  [DBZ-4744](https://issues.redhat.com/browse/DBZ-4744)
* Oracle DDL statement couldn't be parsed [DBZ-4746](https://issues.redhat.com/browse/DBZ-4746)
* Overly verbose Debezium Server Redis logs [DBZ-4751](https://issues.redhat.com/browse/DBZ-4751)
* DDL statement couldn't be parsed [DBZ-4752](https://issues.redhat.com/browse/DBZ-4752)
* Redis runs OOM log in wrong scenario [DBZ-4760](https://issues.redhat.com/browse/DBZ-4760)
* Relax parsing of Heap and Index organized DDL clauses [DBZ-4763](https://issues.redhat.com/browse/DBZ-4763)
* java.lang.NoSuchMethodError: org.apache.kafka.clients.admin.NewTopic [DBZ-4773](https://issues.redhat.com/browse/DBZ-4773)
* Connection validation fails for Db2 [DBZ-4777](https://issues.redhat.com/browse/DBZ-4777)
* Test suite unable to run due to jackson dependency overlaps  [DBZ-4781](https://issues.redhat.com/browse/DBZ-4781)


### Other changes since 1.9.0.Alpha2

* Improve rendering of linked option names [DBZ-4301](https://issues.redhat.com/browse/DBZ-4301)
* Oracle connector downstream docs for 1.9 [DBZ-4325](https://issues.redhat.com/browse/DBZ-4325)
* Use images from quay.io in docs and examples [DBZ-4440](https://issues.redhat.com/browse/DBZ-4440)
* Create an internal FAQ for Oracle Connector [DBZ-4557](https://issues.redhat.com/browse/DBZ-4557)
* Improve documentation about max_replication_slots [DBZ-4603](https://issues.redhat.com/browse/DBZ-4603)
* Connector doc formatting and link fixes [DBZ-4606](https://issues.redhat.com/browse/DBZ-4606)
* Add a backend service for UI to fetch the connector configuration  [DBZ-4627](https://issues.redhat.com/browse/DBZ-4627)
* Update downstream Getting Started guide to describe revised deployment mechanism [DBZ-4632](https://issues.redhat.com/browse/DBZ-4632)
* Update downstream OCP Installation guide to describe revised deployment mechanism [DBZ-4633](https://issues.redhat.com/browse/DBZ-4633)
* Changes config for renovate bot to auto-merge only for non-major update [DBZ-4719](https://issues.redhat.com/browse/DBZ-4719)
* Incorrect connector version in Debezium RHEL Installation Guide  [DBZ-4721](https://issues.redhat.com/browse/DBZ-4721)
* Verify Debezium connector can be used with MongoDB Atlas [DBZ-4731](https://issues.redhat.com/browse/DBZ-4731)
* Remove NATS example [DBZ-4738](https://issues.redhat.com/browse/DBZ-4738)
* Upgrade to Quarkus 2.7.1.Final [DBZ-4743](https://issues.redhat.com/browse/DBZ-4743)
* UI layout fixes [DBZ-4748](https://issues.redhat.com/browse/DBZ-4748)
* Upgrade MySQL JDBC driver to 8.0.28 [DBZ-4759](https://issues.redhat.com/browse/DBZ-4759)
* Nightly build artifacts not published [DBZ-4766](https://issues.redhat.com/browse/DBZ-4766)
* Clarify need for link attributes in docs [DBZ-4776](https://issues.redhat.com/browse/DBZ-4776)


## 1.9.0.Alpha2
February 9th 2022 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12379892)

### New features since 1.9.0.Alpha1

* Use main repo workflow for CI/CD checks in Debezium UI repository checks  [DBZ-3143](https://issues.redhat.com/browse/DBZ-3143)
* Build and deploy Debezium OpenAPI / JSON Schema definitions with every Debezium release [DBZ-4394](https://issues.redhat.com/browse/DBZ-4394)
* Redis sink - Retry in case of connection error/OOM [DBZ-4510](https://issues.redhat.com/browse/DBZ-4510)
* Make KAFKA_QUERY_TIMEOUT configurable [DBZ-4518](https://issues.redhat.com/browse/DBZ-4518)
* MySQL history topic creation needs DESCRIBE_CONFIGS at the Cluster level [DBZ-4547](https://issues.redhat.com/browse/DBZ-4547)
* Redis Sink - change records should be streamed in batches [DBZ-4637](https://issues.redhat.com/browse/DBZ-4637)
* Link for apicurio-registry-distro-connect-converter packege is broken [DBZ-4659](https://issues.redhat.com/browse/DBZ-4659)
* Extend Debezium Schema Generator [DBZ-4665](https://issues.redhat.com/browse/DBZ-4665)


### Breaking changes since 1.9.0.Alpha1

* Add bytes support for blob and binary types in Vitess connector [DBZ-4705](https://issues.redhat.com/browse/DBZ-4705)


### Fixes since 1.9.0.Alpha1

* Database.include.list results in tables being returned twice [DBZ-3679](https://issues.redhat.com/browse/DBZ-3679)
* Suspected inconsistent documentation for 'Ad-hoc read-only Incremental snapshot' [DBZ-4171](https://issues.redhat.com/browse/DBZ-4171)
* CVE-2021-2471 mysql-connector-java: unauthorized access to critical [rhint-debezium-1] [DBZ-4283](https://issues.redhat.com/browse/DBZ-4283)
* Rhel preparation jenkins job pushes extra image [DBZ-4296](https://issues.redhat.com/browse/DBZ-4296)
* Oracle Logminer: snapshot->stream switch misses DB changes in ongoing transactions [DBZ-4367](https://issues.redhat.com/browse/DBZ-4367)
* Incremental snapshots does not honor column case sensitivity [DBZ-4584](https://issues.redhat.com/browse/DBZ-4584)
* JSON data corrupted in update events [DBZ-4605](https://issues.redhat.com/browse/DBZ-4605)
* nCaused by: Multiple parsing errors\nio.debezium.text.ParsingException: DDL statement couldn't be parsed. Please open a Jira [DBZ-4609](https://issues.redhat.com/browse/DBZ-4609)
* Jenkins job for creating image snapshot does not update gitlab certificate correctly [DBZ-4611](https://issues.redhat.com/browse/DBZ-4611)
* Update the UI README node and npm requirements [DBZ-4630](https://issues.redhat.com/browse/DBZ-4630)
* Parse including keyword column table ddl error [DBZ-4640](https://issues.redhat.com/browse/DBZ-4640)
* Nightly installation links do not use snapshot repository download links [DBZ-4644](https://issues.redhat.com/browse/DBZ-4644)
* schema_only_recovery mode not working for FileDatabaseHistory  [DBZ-4646](https://issues.redhat.com/browse/DBZ-4646)
* SQL Server ad-hoc snapshot - SnapshotType is case sensitive [DBZ-4648](https://issues.redhat.com/browse/DBZ-4648)
* DDL parsing issue: ALTER TABLE ... MODIFY PARTITION ... [DBZ-4649](https://issues.redhat.com/browse/DBZ-4649)
* Mark incompatible Xstream tests as LogMiner only [DBZ-4650](https://issues.redhat.com/browse/DBZ-4650)
* DDL statement couldn't be parsed  mismatched input '`encrypted` [DBZ-4661](https://issues.redhat.com/browse/DBZ-4661)
* debezium-examples fail when using confluentinc/cp-schema-registry:7.0.0 [DBZ-4666](https://issues.redhat.com/browse/DBZ-4666)
* DDL parsing exception [DBZ-4675](https://issues.redhat.com/browse/DBZ-4675)
* JdbcConnection#executeWithoutCommitting commits when auto-commit is enabled [DBZ-4701](https://issues.redhat.com/browse/DBZ-4701)
* OracleSchemaMigrationIT fails with Xstream adapter [DBZ-4703](https://issues.redhat.com/browse/DBZ-4703)
* Cannot expand JSON payload with nested arrays of objects [DBZ-4704](https://issues.redhat.com/browse/DBZ-4704)


### Other changes since 1.9.0.Alpha1

* Possible performance issue after Debezium 1.6.1 upgrade (from 1.5) [DBZ-3872](https://issues.redhat.com/browse/DBZ-3872)
* Upgrade Jenkins and Introduce JCasC to jnovotny [DBZ-3980](https://issues.redhat.com/browse/DBZ-3980)
* Random test failure - ZZZGtidSetIT#shouldProcessPurgedGtidSet [DBZ-4294](https://issues.redhat.com/browse/DBZ-4294)
* Verify compatibility with Oracle 21c (21.3.0.0.0) [DBZ-4305](https://issues.redhat.com/browse/DBZ-4305)
* Add metadata to OracleConnectorConfig for Debezium UI [DBZ-4314](https://issues.redhat.com/browse/DBZ-4314)
* Release pipeline should check existence of GA version [DBZ-4623](https://issues.redhat.com/browse/DBZ-4623)
* Release pipeline - conditionalize and parameterize backport check [DBZ-4624](https://issues.redhat.com/browse/DBZ-4624)
* Migrating UI from  webpack-dev-server v3 to v4 [DBZ-4642](https://issues.redhat.com/browse/DBZ-4642)
* Don't run checkstyle/dependency check on documentation-only pull requests or commits [DBZ-4645](https://issues.redhat.com/browse/DBZ-4645)
* Cron-based Github Action to notify documentation changes in last x days [DBZ-4653](https://issues.redhat.com/browse/DBZ-4653)
* Oracle DDL parser failure with supplemental log group clause with a custom name [DBZ-4654](https://issues.redhat.com/browse/DBZ-4654)
* Build MCS container images for Debezium 1.9.0.Alpha1 and deploy to RHOAS quay container registry [DBZ-4656](https://issues.redhat.com/browse/DBZ-4656)
* Upgrade postgres driver to version 42.3.2 [DBZ-4658](https://issues.redhat.com/browse/DBZ-4658)
* Make sure right protoc version is applied [DBZ-4668](https://issues.redhat.com/browse/DBZ-4668)
* Build trigger issues [DBZ-4672](https://issues.redhat.com/browse/DBZ-4672)
* MongoUtilIT test failure - unable to connect to primary [DBZ-4676](https://issues.redhat.com/browse/DBZ-4676)
* Upgrade to Quarkus 2.7.0.Final [DBZ-4677](https://issues.redhat.com/browse/DBZ-4677)
* Update shared UG deployment file for use with downstream OCP Install Guide [DBZ-4700](https://issues.redhat.com/browse/DBZ-4700)
* Indicate ROWID is not supported by XStream [DBZ-4702](https://issues.redhat.com/browse/DBZ-4702)



## 1.9.0.Alpha1
January 26th 2022 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12375781)

### New features since 1.8.0.Final

* Debezium MySQL connector encounter latency in large DML of MySQL [DBZ-3477](https://issues.redhat.com/browse/DBZ-3477)
* Add create/update/delete event seen metrics for monitor upstream dml operation [DBZ-4351](https://issues.redhat.com/browse/DBZ-4351)
* Allow additional config options for Debezium Server Pubsub Connector [DBZ-4375](https://issues.redhat.com/browse/DBZ-4375)
* Allow adhoc snapshots using signals in Oracle versions prior to 12c [DBZ-4404](https://issues.redhat.com/browse/DBZ-4404)
* Fail MongoDB start when oplog is used for MongoDB 5+ [DBZ-4415](https://issues.redhat.com/browse/DBZ-4415)
* Deprecated TruncateHandlingMode config property in favor of skipped_operations [DBZ-4419](https://issues.redhat.com/browse/DBZ-4419)
* Introduce interfaces and default implementations for change event source metrics [DBZ-4459](https://issues.redhat.com/browse/DBZ-4459)
* Create a Debezium schema generator for Debezium connectors (follow-up work) [DBZ-4460](https://issues.redhat.com/browse/DBZ-4460)
* Make connector task partition readability for logs [DBZ-4472](https://issues.redhat.com/browse/DBZ-4472)
* Remove unused brackets in MySqlParser [DBZ-4473](https://issues.redhat.com/browse/DBZ-4473)
* Document DB permissions for Oracle Connector [DBZ-4494](https://issues.redhat.com/browse/DBZ-4494)
* Add support for extra gRPC headers in Vitess connector [DBZ-4532](https://issues.redhat.com/browse/DBZ-4532)
* Mining session stopped due to 'No more data to read from socket' [DBZ-4536](https://issues.redhat.com/browse/DBZ-4536)
* A failure to register JMX metrics should fail the connector [DBZ-4541](https://issues.redhat.com/browse/DBZ-4541)
* Debezium Engine should use topic names for conversion [DBZ-4566](https://issues.redhat.com/browse/DBZ-4566)
* Allow user to define custom retriable message [DBZ-4577](https://issues.redhat.com/browse/DBZ-4577)
* Implement Renovate to fix legacy-peer-deps issue with npm [DBZ-4585](https://issues.redhat.com/browse/DBZ-4585)
* Typo in connect README [DBZ-4589](https://issues.redhat.com/browse/DBZ-4589)
* Unsupported column type 'ROWID' error [DBZ-4595](https://issues.redhat.com/browse/DBZ-4595)
* Cleanup project management in testsuite job [DBZ-4602](https://issues.redhat.com/browse/DBZ-4602)


### Breaking changes since 1.8.0.Final

* Deprecate wal2json support [DBZ-3953](https://issues.redhat.com/browse/DBZ-3953)
* Oracle Logminer: LOB truncated in streaming mode [DBZ-4366](https://issues.redhat.com/browse/DBZ-4366)
* Remove CVE affected files from log4j 1.x JAR [DBZ-4568](https://issues.redhat.com/browse/DBZ-4568)


### Fixes since 1.8.0.Final

* NPE on PostgreSQL Domain Array [DBZ-3657](https://issues.redhat.com/browse/DBZ-3657)
* MysqlSourceConnector issue with latin1 tables [DBZ-3700](https://issues.redhat.com/browse/DBZ-3700)
* JSON Payload not expanding when enabling it [DBZ-4457](https://issues.redhat.com/browse/DBZ-4457)
* Kafka Connect REST extension cannot be built with 1.9 [DBZ-4465](https://issues.redhat.com/browse/DBZ-4465)
* DDL statement couldn't be parsed [DBZ-4485](https://issues.redhat.com/browse/DBZ-4485)
* Parse multiple signed/unsigned keyword from ddl statement failed [DBZ-4497](https://issues.redhat.com/browse/DBZ-4497)
* Set the correct binlog serverId & threadId [DBZ-4500](https://issues.redhat.com/browse/DBZ-4500)
* Null out query in read-only incremental snapshot [DBZ-4501](https://issues.redhat.com/browse/DBZ-4501)
* R/O incremental snapshot can blocks the binlog stream on restart [DBZ-4502](https://issues.redhat.com/browse/DBZ-4502)
* Drop the primary key column getting exception [DBZ-4503](https://issues.redhat.com/browse/DBZ-4503)
* [MySQL Debezium] DDL Parsing error - curdate() & cast() [DBZ-4504](https://issues.redhat.com/browse/DBZ-4504)
* Extra file checker-qual in PostgreSQL package [DBZ-4507](https://issues.redhat.com/browse/DBZ-4507)
* website-builder image is not buildable [DBZ-4508](https://issues.redhat.com/browse/DBZ-4508)
* Job for creating gold image not reading credentials correctly  [DBZ-4516](https://issues.redhat.com/browse/DBZ-4516)
* Replication stream retries are not configured correctly [DBZ-4517](https://issues.redhat.com/browse/DBZ-4517)
* Add backend errors among retriable for Postgres connector [DBZ-4520](https://issues.redhat.com/browse/DBZ-4520)
* Infinispan doesn't work with underscores inside cache names [DBZ-4526](https://issues.redhat.com/browse/DBZ-4526)
* Connector list should update immediately when a connector is deleted [DBZ-4538](https://issues.redhat.com/browse/DBZ-4538)
* Mongo filters page show nulls in namespace name [DBZ-4540](https://issues.redhat.com/browse/DBZ-4540)
* LogMinerHelperIT fails when running Oracle CI with a fresh database [DBZ-4542](https://issues.redhat.com/browse/DBZ-4542)
* Oracle-Connector fails parsing a DDL statement (VIRTUAL keyword) [DBZ-4546](https://issues.redhat.com/browse/DBZ-4546)
* DatabaseVersionResolver comparison logic skips tests unintendedly [DBZ-4548](https://issues.redhat.com/browse/DBZ-4548)
* io.debezium.text.ParsingException when column name is 'seq' [DBZ-4553](https://issues.redhat.com/browse/DBZ-4553)
* MySQL `FLUSH TABLE[S]` with empty table list not handled  [DBZ-4561](https://issues.redhat.com/browse/DBZ-4561)
* Debezium apicurio version is not aligned with Quarkus [DBZ-4565](https://issues.redhat.com/browse/DBZ-4565)
* Oracle built-in schema exclusions should also apply to DDL changes [DBZ-4567](https://issues.redhat.com/browse/DBZ-4567)
* mongo-source-connector  config database.include.list does not work [DBZ-4575](https://issues.redhat.com/browse/DBZ-4575)
* Can't process column definition with length exceeding Integer.MAX_VALUE [DBZ-4583](https://issues.redhat.com/browse/DBZ-4583)
* Oracle connector can't find the SCN [DBZ-4597](https://issues.redhat.com/browse/DBZ-4597)


### Other changes since 1.8.0.Final

* Set up CI for Oracle [DBZ-732](https://issues.redhat.com/browse/DBZ-732)
* Migrate logger used for tests to Logback [DBZ-2224](https://issues.redhat.com/browse/DBZ-2224)
* Update downstream docs in regards to deprecated elements [DBZ-3881](https://issues.redhat.com/browse/DBZ-3881)
* Broken links to the Transaction metadata topics from descriptions for provide.transaction.metadata property [DBZ-3997](https://issues.redhat.com/browse/DBZ-3997)
* Add script to check for missing backports [DBZ-4063](https://issues.redhat.com/browse/DBZ-4063)
* Protect release from using invalid version name [DBZ-4072](https://issues.redhat.com/browse/DBZ-4072)
* Upgrade to Quarkus 2.6.2.Final [DBZ-4117](https://issues.redhat.com/browse/DBZ-4117)
* Use Postgres 10 by default [DBZ-4131](https://issues.redhat.com/browse/DBZ-4131)
* Give debezium-builder user privileges to access internal issues [DBZ-4271](https://issues.redhat.com/browse/DBZ-4271)
* Point to supported versions in connector pages [DBZ-4300](https://issues.redhat.com/browse/DBZ-4300)
* Allow for additional custom columns in an outbox table [DBZ-4317](https://issues.redhat.com/browse/DBZ-4317)
* Log problematic values if they cannot be processed [DBZ-4371](https://issues.redhat.com/browse/DBZ-4371)
* Run Jenkins CI on weekends too [DBZ-4373](https://issues.redhat.com/browse/DBZ-4373)
* Update Postgres JDBC driver to 42.3.1 [DBZ-4374](https://issues.redhat.com/browse/DBZ-4374)
* Release pipeline should use Jira API token [DBZ-4383](https://issues.redhat.com/browse/DBZ-4383)
* Remove log.mining.log.file.query.max.retries configuration property [DBZ-4408](https://issues.redhat.com/browse/DBZ-4408)
* Add Debezium Server example using Postgres and Pub/Sub [DBZ-4438](https://issues.redhat.com/browse/DBZ-4438)
* Document Outbox SMT behaviour with postgres bytea_output = escape [DBZ-4461](https://issues.redhat.com/browse/DBZ-4461)
* Run formatting check in the same connector/module workflows  [DBZ-4462](https://issues.redhat.com/browse/DBZ-4462)
* Upgrade SQL Server driver to 9.4 [DBZ-4463](https://issues.redhat.com/browse/DBZ-4463)
* Add snapshot repository to Vitess connector [DBZ-4464](https://issues.redhat.com/browse/DBZ-4464)
* REST extension tests must not depend on source code version [DBZ-4466](https://issues.redhat.com/browse/DBZ-4466)
* snapshotPreceededBySchemaChange should not be tested for Db2 [DBZ-4467](https://issues.redhat.com/browse/DBZ-4467)
* Debezium Server workflow should build PG connector without tests [DBZ-4468](https://issues.redhat.com/browse/DBZ-4468)
* PostgresShutdownIT must not depend on Postgres version [DBZ-4469](https://issues.redhat.com/browse/DBZ-4469)
* Updating jenkins job creating image snapshots   [DBZ-4486](https://issues.redhat.com/browse/DBZ-4486)
* Set jenkins jobs to store last 10 builds [DBZ-4506](https://issues.redhat.com/browse/DBZ-4506)
* Provide a script to generate release notes section [DBZ-4513](https://issues.redhat.com/browse/DBZ-4513)
* Remove INTERNAL_KEY_CONVERTER and INTERNAL_VALUE_CONVERTER env vars [DBZ-4514](https://issues.redhat.com/browse/DBZ-4514)
* Bump protobuf version to the latest 3.x [DBZ-4527](https://issues.redhat.com/browse/DBZ-4527)
* Document automatic log-switch setting for low-frequency change systems [DBZ-4528](https://issues.redhat.com/browse/DBZ-4528)
* Organize properties of Db2 connector [DBZ-4537](https://issues.redhat.com/browse/DBZ-4537)
* Update release procedure to cover required documentation config changes [DBZ-4539](https://issues.redhat.com/browse/DBZ-4539)
* Module debezium-testing-testcontainers tests are not executed [DBZ-4544](https://issues.redhat.com/browse/DBZ-4544)
* Check Debezium user logging after auth change [DBZ-4545](https://issues.redhat.com/browse/DBZ-4545)
* Fix links to connector incremental snapshots topic [DBZ-4552](https://issues.redhat.com/browse/DBZ-4552)
* Vitess connector image cannot be built [DBZ-4559](https://issues.redhat.com/browse/DBZ-4559)
* Reduce GitHub action build times with formatting [DBZ-4562](https://issues.redhat.com/browse/DBZ-4562)
* Doc updates to address downstream build issues [DBZ-4563](https://issues.redhat.com/browse/DBZ-4563)
* Upgrade Avro converter to 7.0.1 and Apicurio to 2.1.5.Final [DBZ-4569](https://issues.redhat.com/browse/DBZ-4569)
* Older degree of parallelism DDL syntax causes parsing exception [DBZ-4571](https://issues.redhat.com/browse/DBZ-4571)
* Conditionalize note about outbox event router incompatibility [DBZ-4573](https://issues.redhat.com/browse/DBZ-4573)
* Update description of snapshot.mode in postgresql.adoc [DBZ-4574](https://issues.redhat.com/browse/DBZ-4574)
* Avoid build warning about maven-filtering missing plugin descriptor [DBZ-4580](https://issues.redhat.com/browse/DBZ-4580)
* Fix build failure when xstream missing when building the micro benchmark for Oracle [DBZ-4581](https://issues.redhat.com/browse/DBZ-4581)
* Update shared UG deployment file to clarify that connectors can use existing KC instance [DBZ-4582](https://issues.redhat.com/browse/DBZ-4582)
* Test Failure - RecordsStreamProducerIT [DBZ-4592](https://issues.redhat.com/browse/DBZ-4592)
* Upgrade Kafka to 3.1.0 [DBZ-4610](https://issues.redhat.com/browse/DBZ-4610)
* Server transformation properties should refer to "type" rather than "class" [DBZ-4613](https://issues.redhat.com/browse/DBZ-4613)


## 1.8.0.Final
December 16th 2021 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12377386)

### New features since 1.8.0.CR1

* Allow to configure custom Hibernate user types for Quarkus outbox extension [DBZ-3552](https://issues.redhat.com/browse/DBZ-3552)
* Create a Debezium schema generator for Debezium connectors (initial work) [DBZ-4393](https://issues.redhat.com/browse/DBZ-4393)

### Breaking changes since 1.8.0.CR1

* MongoDB ExtractNewDocumentState SMT overwrites existing document ID field  [DBZ-4413](https://issues.redhat.com/browse/DBZ-4413)


### Fixes since 1.8.0.CR1

* Outbox Event Router not working in Oracle Connector [DBZ-3940](https://issues.redhat.com/browse/DBZ-3940)
* some data type is not working for sending signals to a Debezium connector [DBZ-4298](https://issues.redhat.com/browse/DBZ-4298)
* Debezium UI - Connector create fails if topic group defaults not specified [DBZ-4378](https://issues.redhat.com/browse/DBZ-4378)


### Other changes since 1.8.0.CR1

* Intermittent test failure: SqlServerChangeTableSetIT#readHistoryAfterRestart() [DBZ-3306](https://issues.redhat.com/browse/DBZ-3306)
* Upgrade to Apicurio Registry 2.0 (QE, docs) [DBZ-3629](https://issues.redhat.com/browse/DBZ-3629)
* Oracle upstream tests in internal CI  [DBZ-4185](https://issues.redhat.com/browse/DBZ-4185)
* Document MongoDB source format [DBZ-4420](https://issues.redhat.com/browse/DBZ-4420)
* Missing log message for snapshot.locking.mode = none [DBZ-4426](https://issues.redhat.com/browse/DBZ-4426)
* Caching not working in formatting job [DBZ-4429](https://issues.redhat.com/browse/DBZ-4429)
* Optionally assemble Oracle connector distribution without Infinispan [DBZ-4446](https://issues.redhat.com/browse/DBZ-4446)
* Simplify the implementation of method duration in debezium/util/Strings.java [DBZ-4423](https://issues.redhat.com/browse/DBZ-4423)
* Exclude log4j from Debezium Server distribution in 1.8 [DBZ-4452](https://issues.redhat.com/browse/DBZ-4452)


## 1.8.0.CR1
December 9th 2021 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12377385)

### New features since 1.8.0.Beta1

* Implement incremental snapshotting for MongoDB [DBZ-3342](https://issues.redhat.com/browse/DBZ-3342)
* Add schema descriptors for the UI JSON Schema for SQL Server Connector [DBZ-3697](https://issues.redhat.com/browse/DBZ-3697)
* Optionally add OPTION(RECOMPILE) to incremental snapshot queries [DBZ-4249](https://issues.redhat.com/browse/DBZ-4249)
* Log count of changed records sent [DBZ-4341](https://issues.redhat.com/browse/DBZ-4341)
* Add support for truncate in oracle connector [DBZ-4385](https://issues.redhat.com/browse/DBZ-4385)
* Support binary_handling_mode for Oracle connector [DBZ-4400](https://issues.redhat.com/browse/DBZ-4400)
* Enforce consistent vgtid representation in vitess connector [DBZ-4409](https://issues.redhat.com/browse/DBZ-4409)


### Breaking changes since 1.8.0.Beta1

* Fix source fields and keyspace field in vitess connector [DBZ-4412](https://issues.redhat.com/browse/DBZ-4412)
* Fix issues with blob and uint64 types in Vitess connector [DBZ-4403](https://issues.redhat.com/browse/DBZ-4403)
* Clean up "source" strucure for Vitess connector [DBZ-4428](https://issues.redhat.com/browse/DBZ-4428)


### Fixes since 1.8.0.Beta1

* Parallel write can be lost during catch-up phase [DBZ-2792](https://issues.redhat.com/browse/DBZ-2792)
* None of log files contains offset SCN (SCN offset is no longer available in the online redo logs) [DBZ-3635](https://issues.redhat.com/browse/DBZ-3635)
* [Debezium Server] Event Hubs exporter slow/Event data was too large [DBZ-4277](https://issues.redhat.com/browse/DBZ-4277)
* NullPointer exception on Final stage of snapshot for Oracle connector [DBZ-4376](https://issues.redhat.com/browse/DBZ-4376)
* Oracle pipeline matrix docker conflict [DBZ-4377](https://issues.redhat.com/browse/DBZ-4377)
* System testsuite unable to pull apicurio operator from quay [DBZ-4382](https://issues.redhat.com/browse/DBZ-4382)
* Oracle DDL Parser Error [DBZ-4388](https://issues.redhat.com/browse/DBZ-4388)
* DDL couldn't be parsed: 'analyze table schema.table estimate statistics sample 5 percent;' [DBZ-4396](https://issues.redhat.com/browse/DBZ-4396)
* MySQL: DDL Statement could not be parsed 'GRANT' [DBZ-4397](https://issues.redhat.com/browse/DBZ-4397)
* Support keyword CHAR SET for defining charset options [DBZ-4402](https://issues.redhat.com/browse/DBZ-4402)
* Xstream support with LOB unavailable value placeholder support is inconsistent [DBZ-4422](https://issues.redhat.com/browse/DBZ-4422)
* Oracle Infinispan buffer fails to serialize unavailable value placeholders [DBZ-4425](https://issues.redhat.com/browse/DBZ-4425)
* VStream gRPC connection closed after being idle for a few minutes [DBZ-4389](https://issues.redhat.com/browse/DBZ-4389)


### Other changes since 1.8.0.Beta1

* Oracle testing in system-level testsuite [DBZ-3963](https://issues.redhat.com/browse/DBZ-3963)
* Upgrade to Quarkus 2.5.0.Final [DBZ-4035](https://issues.redhat.com/browse/DBZ-4035)
* Document incremental chunk size setting [DBZ-4127](https://issues.redhat.com/browse/DBZ-4127)
* Complete CDC implementation based on MongoDB Change Streams [DBZ-4205](https://issues.redhat.com/browse/DBZ-4205)
* Record video demo showing Kafka topics creation and transformation UIs [DBZ-4260](https://issues.redhat.com/browse/DBZ-4260)
* Add Oracle 12.2.0.1 to internal CI Oracle job [DBZ-4322](https://issues.redhat.com/browse/DBZ-4322)
* OracleClobDataTypeIT shouldNotStreamAnyChangesWhenLobEraseIsDetected may fail randomly [DBZ-4384](https://issues.redhat.com/browse/DBZ-4384)
* Upgrade impsort-maven-plugin from 1.6.0 to 1.6.2 [DBZ-4386](https://issues.redhat.com/browse/DBZ-4386)
* Upgrade formatter-maven-plugin from 2.15.0 to 2.16.0 [DBZ-4387](https://issues.redhat.com/browse/DBZ-4387)
* Unstable test for online DDL changes [DBZ-4391](https://issues.redhat.com/browse/DBZ-4391)
* Create Debezium Kafka Connect REST Extension [DBZ-4028](https://issues.redhat.com/browse/DBZ-4028)



## 1.8.0.Beta1
November 30th 2021 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12375780)

### New features since 1.8.0.Alpha2

* Support pg_logical_emit_message [DBZ-2363](https://issues.redhat.com/browse/DBZ-2363)
* Outbox Event Router for MongoDB [DBZ-3528](https://issues.redhat.com/browse/DBZ-3528)
* Improve interval type support in Oracle [DBZ-1539](https://issues.redhat.com/browse/DBZ-1539)
* money data type should be controlled by decimal.handling.mode [DBZ-1931](https://issues.redhat.com/browse/DBZ-1931)
* Support for Multiple Databases per SQL Server Connector [DBZ-2975](https://issues.redhat.com/browse/DBZ-2975)
* Debezium server stops with wrong exit code (0) [DBZ-3570](https://issues.redhat.com/browse/DBZ-3570)
* Change Debezium UI configurations property names [DBZ-4066](https://issues.redhat.com/browse/DBZ-4066)
* Extend configuration support for Infinispan caches [DBZ-4169](https://issues.redhat.com/browse/DBZ-4169)
* Support schema changes during incremental snapshot [DBZ-4196](https://issues.redhat.com/browse/DBZ-4196)
* Handle login failure during instance upgrade as retriable [DBZ-4285](https://issues.redhat.com/browse/DBZ-4285)
* Modify the type of aggregateid in MongoDB Outbox Event Router [DBZ-4318](https://issues.redhat.com/browse/DBZ-4318)
* Explicit the MS SQL Materialized view limitation  [DBZ-4330](https://issues.redhat.com/browse/DBZ-4330)


### Breaking changes since 1.8.0.Alpha2

* Support passing an unavailable placeholder value for CLOB/BLOB column types [DBZ-4276](https://issues.redhat.com/browse/DBZ-4276)
* Remove vtctld dependency in Vitess connector [DBZ-4324](https://issues.redhat.com/browse/DBZ-4324)


### Fixes since 1.8.0.Alpha2

* PostgresConnector does not allow a numeric slot name [DBZ-1042](https://issues.redhat.com/browse/DBZ-1042)
* False empty schema warning for snapshot mode never [DBZ-1344](https://issues.redhat.com/browse/DBZ-1344)
* Tutorial shows incorrectly shows "op": "c" for initial change events [DBZ-3786](https://issues.redhat.com/browse/DBZ-3786)
* SQL Server fails to read CDC events if there is a schema change ahead [DBZ-3992](https://issues.redhat.com/browse/DBZ-3992)
* Once user click on "Review and finish" button that step in link in not enabled in wizard side menu. [DBZ-4119](https://issues.redhat.com/browse/DBZ-4119)
* DDL statement couldn't be parsed [DBZ-4224](https://issues.redhat.com/browse/DBZ-4224)
* The lastOffset variable in MySqlStreamingChangeEventSource is always null [DBZ-4225](https://issues.redhat.com/browse/DBZ-4225)
* Unknown entity: io.debezium.outbox.quarkus.internal.OutboxEvent [DBZ-4232](https://issues.redhat.com/browse/DBZ-4232)
* Signal based incremental snapshot is failing when launched right after a schema change [DBZ-4272](https://issues.redhat.com/browse/DBZ-4272)
* SQL Server connector doesn't handle multiple capture instances for the same table with equal start LSN [DBZ-4273](https://issues.redhat.com/browse/DBZ-4273)
* Debezium UI - some issues with browser support for replaceAll [DBZ-4274](https://issues.redhat.com/browse/DBZ-4274)
* AbstractDatabaseHistory.java has typo [DBZ-4275](https://issues.redhat.com/browse/DBZ-4275)
* OracleConnectorIT - two tests fail when using Xstream [DBZ-4279](https://issues.redhat.com/browse/DBZ-4279)
* ParsingException: DDL statement couldn't be parsed [DBZ-4280](https://issues.redhat.com/browse/DBZ-4280)
* Topic Group UI step does not refresh correctly after setting properties [DBZ-4293](https://issues.redhat.com/browse/DBZ-4293)
* Add MariaDB specific username for MySQL parser [DBZ-4304](https://issues.redhat.com/browse/DBZ-4304)
* NullPointerException may be thrown when validating table and column lengths [DBZ-4308](https://issues.redhat.com/browse/DBZ-4308)
* RelationalChangeRecordEmitter calls "LoggerFactory.getLogger(getClass())" for each instance of the emitter [DBZ-4309](https://issues.redhat.com/browse/DBZ-4309)
*  support for JSON function in MySQL index [DBZ-4320](https://issues.redhat.com/browse/DBZ-4320)
* Avoid holding table metadata lock in read-only incremental snapshots [DBZ-4331](https://issues.redhat.com/browse/DBZ-4331)
* Convert mysql time type default value error [DBZ-4334](https://issues.redhat.com/browse/DBZ-4334)
* Wrong configuration option name for MongoDB Outbox SMT [DBZ-4337](https://issues.redhat.com/browse/DBZ-4337)
* Incremental Snapshot does not pick up table [DBZ-4343](https://issues.redhat.com/browse/DBZ-4343)
* Oracle connector - Cannot parse column default value 'NULL ' to type '2' [DBZ-4360](https://issues.redhat.com/browse/DBZ-4360)


### Other changes since 1.8.0.Alpha2

* Add canonical URL links to older doc versions [DBZ-3897](https://issues.redhat.com/browse/DBZ-3897)
* Set up testing job for MongoDB 5.0 [DBZ-3938](https://issues.redhat.com/browse/DBZ-3938)
* Misc. documentation changes for the Debezium MySQL connector [DBZ-3974](https://issues.redhat.com/browse/DBZ-3974)
* Promote Outbox SMT to GA [DBZ-4012](https://issues.redhat.com/browse/DBZ-4012)
* Test failure: SchemaHistoryTopicIT::schemaChangeAfterSnapshot() [DBZ-4082](https://issues.redhat.com/browse/DBZ-4082)
* Jenkins job for creating image snapshot used by new Jenkins nodes [DBZ-4122](https://issues.redhat.com/browse/DBZ-4122)
* Use SMT/Transformation UI backend endpoint [DBZ-4146](https://issues.redhat.com/browse/DBZ-4146)
* Create GH Action for tearing down abandoned website preview environments [DBZ-4214](https://issues.redhat.com/browse/DBZ-4214)
* Unify Memory and Infinispan event processor implementations [DBZ-4236](https://issues.redhat.com/browse/DBZ-4236)
* Update system-level testsuite CI job [DBZ-4267](https://issues.redhat.com/browse/DBZ-4267)
* Upgrade MySQL JDBC driver to 8.0.27 [DBZ-4286](https://issues.redhat.com/browse/DBZ-4286)
* Only build debezium-core and dependences in cross-repo builds [DBZ-4289](https://issues.redhat.com/browse/DBZ-4289)
* Reduce log verbosity [DBZ-4291](https://issues.redhat.com/browse/DBZ-4291)
* Vitess connector should expose vstream flags [DBZ-4295](https://issues.redhat.com/browse/DBZ-4295)
* Vitess connector should allow client to config starting VGTID [DBZ-4297](https://issues.redhat.com/browse/DBZ-4297)
* Layout glitch on docs landing page [DBZ-4299](https://issues.redhat.com/browse/DBZ-4299)
* Provide outbox routing example for MongoDB [DBZ-4302](https://issues.redhat.com/browse/DBZ-4302)
* Fix wrong option names in examples [DBZ-4311](https://issues.redhat.com/browse/DBZ-4311)
* Update functional test CI to work with downstream source archive [DBZ-4316](https://issues.redhat.com/browse/DBZ-4316)
* Provide example showing usage of remote Infinispan cache [DBZ-4326](https://issues.redhat.com/browse/DBZ-4326)
* Provide CI for MongoDB 4.4 [DBZ-4327](https://issues.redhat.com/browse/DBZ-4327)
* Test case for schema migration in Vitess connector [DBZ-4353](https://issues.redhat.com/browse/DBZ-4353)
* Enable transaction metadata for vitess connector [DBZ-4355](https://issues.redhat.com/browse/DBZ-4355)
* io.debezium.data.VerifyRecord.isValid(SourceRecord) is a no-op [DBZ-4364](https://issues.redhat.com/browse/DBZ-4364)
* SignalsIT times out after 6h on CI [DBZ-4370](https://issues.redhat.com/browse/DBZ-4370)
* Document incremental chunk size setting [DBZ-4127](https://issues.redhat.com/browse/DBZ-4127)



## 1.8.0.Alpha2
November 11th 2021 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12377154)

### New features since 1.8.0.Alpha1

* TableChangesSerializer ignored defaultValue and enumValues [DBZ-3966](https://issues.redhat.com/browse/DBZ-3966)
* Support for heartbeat action queries for MySQL [DBZ-4029](https://issues.redhat.com/browse/DBZ-4029)
* Expose the transaction topicname as a config [DBZ-4077](https://issues.redhat.com/browse/DBZ-4077)
* Improvement to the topic creation step [DBZ-4172](https://issues.redhat.com/browse/DBZ-4172)
* Process transaction started/committed in MySQL read-only incremental snapshot [DBZ-4197](https://issues.redhat.com/browse/DBZ-4197)
* Ability to use base image from authenticated registry with KC build mechanism [DBZ-4227](https://issues.redhat.com/browse/DBZ-4227)
* Remove SqlServerConnector database.user Required Validator [DBZ-4231](https://issues.redhat.com/browse/DBZ-4231)
* Specify database hot name as 0.0.0.0 for Oracle connector tests CI [DBZ-4242](https://issues.redhat.com/browse/DBZ-4242)
* Suport all charsets in MySQL parser [DBZ-4261](https://issues.redhat.com/browse/DBZ-4261)


### Breaking changes since 1.8.0.Alpha1

* Store buffered events in separate Infinispan cache [DBZ-4159](https://issues.redhat.com/browse/DBZ-4159)


### Fixes since 1.7.0.Alpha1

* "table" is null for table.include.list and column.include.list [DBZ-3611](https://issues.redhat.com/browse/DBZ-3611)
* Debezium server crashes when deleting a record from a SQLServer table (redis sink) [DBZ-3708](https://issues.redhat.com/browse/DBZ-3708)
* Invalid default value error on captured table DDL with default value [DBZ-3710](https://issues.redhat.com/browse/DBZ-3710)
* Incremental snapshot doesn't work without primary key [DBZ-4107](https://issues.redhat.com/browse/DBZ-4107)
* Error: PostgresDefaultValueConverter - Cannot parse column default value 'NULL::numeric' to type 'numeric'. Expression evaluation is not supported. [DBZ-4137](https://issues.redhat.com/browse/DBZ-4137)
* Container images for Apache Kafka and ZooKeeper fail to start up [DBZ-4160](https://issues.redhat.com/browse/DBZ-4160)
* Debezium 1.7 image disables unsecure algorithms. Breaks unpatched databases [DBZ-4167](https://issues.redhat.com/browse/DBZ-4167)
* DDL statement couldn't be parsed - Modify Column [DBZ-4174](https://issues.redhat.com/browse/DBZ-4174)
* DML statement couldn't be parsed [DBZ-4194](https://issues.redhat.com/browse/DBZ-4194)
* Debezium log miner processes get terminated with ORA-04030 error in idle database environment. [DBZ-4204](https://issues.redhat.com/browse/DBZ-4204)
* DDL with Oracle SDO_GEOMETRY cannot be parsed [DBZ-4206](https://issues.redhat.com/browse/DBZ-4206)
* DDL with Oracle sequence as default for primary key fails schema generation [DBZ-4208](https://issues.redhat.com/browse/DBZ-4208)
* io.debezium.text.ParsingException: DDL statement couldn't be parsed. Please open a Jira issue with the statement 'DROP TABLE IF EXISTS condition' [DBZ-4210](https://issues.redhat.com/browse/DBZ-4210)
* Support MySQL Dual Passwords in DDL Parser [DBZ-4215](https://issues.redhat.com/browse/DBZ-4215)
* Debezium Metrics not being set correctly [DBZ-4222](https://issues.redhat.com/browse/DBZ-4222)
* CREATE PROCEDURE DDL throws ParsingException [DBZ-4229](https://issues.redhat.com/browse/DBZ-4229)
* Exception ORA-00310 is not gracefully handled during streaming [DBZ-4230](https://issues.redhat.com/browse/DBZ-4230)
* CHAR / NCHAR precision is not correctly derived from DDL statements [DBZ-4233](https://issues.redhat.com/browse/DBZ-4233)
* Oracle connector parses NUMBER(*,0) as NUMBER(0,0) in DDL [DBZ-4240](https://issues.redhat.com/browse/DBZ-4240)
* Signal based incremental snapshot is failing if database name contains dash  [DBZ-4244](https://issues.redhat.com/browse/DBZ-4244)
* SQL Server connector doesn't handle retriable errors during database state transitions [DBZ-4245](https://issues.redhat.com/browse/DBZ-4245)
* Does Debezium support database using charset GB18030? [DBZ-4246](https://issues.redhat.com/browse/DBZ-4246)
* Broken anchors in Debezium Documentation [DBZ-4254](https://issues.redhat.com/browse/DBZ-4254)
* Reduce verbosity of logging Oracle memory metrics [DBZ-4255](https://issues.redhat.com/browse/DBZ-4255)
* When Debezium executes `select *` in the snapshot phase, it does not catch the sql exception, resulting in confusing exceptions and logs [DBZ-4257](https://issues.redhat.com/browse/DBZ-4257)


### Other changes since 1.8.0.Alpha1

* Rename "master" branches to "main" for remaining repos [DBZ-3626](https://issues.redhat.com/browse/DBZ-3626)
* Support Oracle Logminer docker image in system level test-suite [DBZ-3929](https://issues.redhat.com/browse/DBZ-3929)
* Missing documentation for max.iteration.transactions option [DBZ-4129](https://issues.redhat.com/browse/DBZ-4129)
* Use topic auto-creation UI backend endpoint [DBZ-4148](https://issues.redhat.com/browse/DBZ-4148)
* Remove superfluous build triggers [DBZ-4200](https://issues.redhat.com/browse/DBZ-4200)
* Tag debezium/tooling:1.2 version [DBZ-4238](https://issues.redhat.com/browse/DBZ-4238)
* Rework MySqlTimestampColumnIT test [DBZ-4241](https://issues.redhat.com/browse/DBZ-4241)
* Remove unused code [DBZ-4252](https://issues.redhat.com/browse/DBZ-4252)
* Optimize tooling image [DBZ-4258](https://issues.redhat.com/browse/DBZ-4258)
* Change DB2 image in testsuite to use private registry [DBZ-4268](https://issues.redhat.com/browse/DBZ-4268)



## 1.8.0.Alpha1
October 27th 2021 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12355606)

### New features since 1.7.0.Final

* Provide MongoDB CDC implementation based on 4.0 change streams [DBZ-435](https://issues.redhat.com/browse/DBZ-435)
* No option fullDocument for the connection to MongoDB oplog.rs [DBZ-1847](https://issues.redhat.com/browse/DBZ-1847)
* Make antora playbook_author.yml use current branch [DBZ-2546](https://issues.redhat.com/browse/DBZ-2546)
* Support Kerberos for Debezium MS SQL plugin [DBZ-3517](https://issues.redhat.com/browse/DBZ-3517)
* Make "snapshot.include.collection.list" case insensitive like "table.include.list" [DBZ-3895](https://issues.redhat.com/browse/DBZ-3895)
* Exclude usernames at transaction level [DBZ-3978](https://issues.redhat.com/browse/DBZ-3978)
* [oracle] Add the SCHEMA_ONLY_RECOVERY snapshot mode [DBZ-3986](https://issues.redhat.com/browse/DBZ-3986)
* Support parse table and columns comment [DBZ-4000](https://issues.redhat.com/browse/DBZ-4000)
* Upgrade postgres JDBC driver to version 42.2.24 [DBZ-4046](https://issues.redhat.com/browse/DBZ-4046)
* Support JSON logging formatting [DBZ-4114](https://issues.redhat.com/browse/DBZ-4114)
* Upgrade mysql-binlog-connector-java to v0.25.4 [DBZ-4152](https://issues.redhat.com/browse/DBZ-4152)
* Wrong class name in SMT predicates documentation  [DBZ-4153](https://issues.redhat.com/browse/DBZ-4153)
* Log warning when table/column name exceeds maximum allowed by LogMiner [DBZ-4161](https://issues.redhat.com/browse/DBZ-4161)
* Add Redis to debezium-server-architecture.png [DBZ-4190](https://issues.redhat.com/browse/DBZ-4190)
* wrong variable naming in an unit test for Outbox Event Router SMT [DBZ-4191](https://issues.redhat.com/browse/DBZ-4191)
* MongoDB connector support user defined topic delimiter [DBZ-4192](https://issues.redhat.com/browse/DBZ-4192)
* Parse the "window" keyword for agg and nonagg function in mysql8 [DBZ-4193](https://issues.redhat.com/browse/DBZ-4193)
* wrong field on change event message example in MongoDB Connector documentation [DBZ-4201](https://issues.redhat.com/browse/DBZ-4201)
* Add a backend service for UI to fetch the SMT and topic auto-creation configuration properties  [DBZ-3874](https://issues.redhat.com/browse/DBZ-3874)


### Breaking changes since 1.7.0.Final

None


### Fixes since 1.7.0.Final

* Debezium build is unstable for Oracle connector [DBZ-3807](https://issues.redhat.com/browse/DBZ-3807)
* Row hashing in LogMiner Query not able to differentiate between rows of a statement. [DBZ-3834](https://issues.redhat.com/browse/DBZ-3834)
* The chunk select statement is incorrect for combined primary key in incremental snapshot [DBZ-3860](https://issues.redhat.com/browse/DBZ-3860)
* Crash processing MariaDB DATETIME fields returns empty blob instead of null (Snapshotting with useCursorFetch option) [DBZ-4032](https://issues.redhat.com/browse/DBZ-4032)
* column.the mask.hash.hashAlgorithm.with.... data corruption occurs when using this feature [DBZ-4033](https://issues.redhat.com/browse/DBZ-4033)
* Compilation of MySQL grammar displays warnings [DBZ-4034](https://issues.redhat.com/browse/DBZ-4034)
* Infinispan SPI throws NPE with more than one connector configured to the same Oracle database [DBZ-4064](https://issues.redhat.com/browse/DBZ-4064)
* Extra double quotes on Kafka message produced by Quarkus Outbox Extension [DBZ-4068](https://issues.redhat.com/browse/DBZ-4068)
* Debezium Server might contain driver versions pulled from Quarkus [DBZ-4070](https://issues.redhat.com/browse/DBZ-4070)
* Connection failure while reading chunk during incremental snapshot [DBZ-4078](https://issues.redhat.com/browse/DBZ-4078)
* Postgres 12/13 images are not buildable [DBZ-4080](https://issues.redhat.com/browse/DBZ-4080)
* Postgres testsuite hangs on PostgresConnectorIT#exportedSnapshotShouldNotSkipRecordOfParallelTx [DBZ-4081](https://issues.redhat.com/browse/DBZ-4081)
* CloudEventsConverter omits payload data of deleted documents [DBZ-4083](https://issues.redhat.com/browse/DBZ-4083)
* Database history is constantly being reconfigured [DBZ-4106](https://issues.redhat.com/browse/DBZ-4106)
* projectId not being set when injecting a custom PublisherBuilder [DBZ-4111](https://issues.redhat.com/browse/DBZ-4111)
* Oracle flush table should not contain multiple rows [DBZ-4118](https://issues.redhat.com/browse/DBZ-4118)
* Can't parse DDL for View [DBZ-4121](https://issues.redhat.com/browse/DBZ-4121)
* SQL Server Connector fails to wrap in flat brackets [DBZ-4125](https://issues.redhat.com/browse/DBZ-4125)
* Oracle Connector DDL Parsing Exception [DBZ-4126](https://issues.redhat.com/browse/DBZ-4126)
* Debezium deals with Oracle DDL appeared IndexOutOfBoundsException: Index: 0, Size: 0 [DBZ-4135](https://issues.redhat.com/browse/DBZ-4135)
* Oracle connector throws NPE during streaming in archive only mode [DBZ-4140](https://issues.redhat.com/browse/DBZ-4140)
* debezium-api and debezium-core jars missing in NIGHTLY Kafka Connect container image libs dir [DBZ-4147](https://issues.redhat.com/browse/DBZ-4147)
* Trim numerical defaultValue before converting [DBZ-4150](https://issues.redhat.com/browse/DBZ-4150)
* Possible OutOfMemoryError with tracking schema changes [DBZ-4151](https://issues.redhat.com/browse/DBZ-4151)
* DDL ParsingException - not all table compression modes are supported [DBZ-4158](https://issues.redhat.com/browse/DBZ-4158)
* Producer failure NullPointerException [DBZ-4166](https://issues.redhat.com/browse/DBZ-4166)
* DDL Statement couldn't be parsed [DBZ-4170](https://issues.redhat.com/browse/DBZ-4170)
* In multiple connect clusters monitoring, no matter which cluster is selected from the dropdown list, the detailed information is always for the first cluster. [DBZ-4181](https://issues.redhat.com/browse/DBZ-4181)
* Remove MINUSMINUS operator [DBZ-4184](https://issues.redhat.com/browse/DBZ-4184)
* OracleSchemaMigrationIT#shouldNotEmitDdlEventsForNonTableObjects fails for Xstream [DBZ-4186](https://issues.redhat.com/browse/DBZ-4186)
* Certain LogMiner-specific tests are not being skipped while using Xstreams [DBZ-4188](https://issues.redhat.com/browse/DBZ-4188)
* Missing debezium/postgres:14-alpine in Docker Hub [DBZ-4195](https://issues.redhat.com/browse/DBZ-4195)
* nulls for some MySQL properties in the connector-types backend response [DBZ-3108](https://issues.redhat.com/browse/DBZ-3108)


### Other changes since 1.7.0.Final

* Test with new deployment mechanism in AMQ Streams [DBZ-1777](https://issues.redhat.com/browse/DBZ-1777)
* Incorrect documentation for message.key.columns [DBZ-3437](https://issues.redhat.com/browse/DBZ-3437)
* Re-enable building PostgreSQL alpine images [DBZ-3691](https://issues.redhat.com/browse/DBZ-3691)
* Upgrade to Quarkus 2.2.3.Final [DBZ-3785](https://issues.redhat.com/browse/DBZ-3785)
* Document awareness of Oracle database tuning [DBZ-3880](https://issues.redhat.com/browse/DBZ-3880)
* Publish website-builder and tooling images once per week [DBZ-3907](https://issues.redhat.com/browse/DBZ-3907)
* Intermittent test failure on CI - RecordsStreamProducerIT#shouldReceiveHeartbeatAlsoWhenChangingNonWhitelistedTable() [DBZ-3919](https://issues.redhat.com/browse/DBZ-3919)
* Please fix vulnerabilites [DBZ-3926](https://issues.redhat.com/browse/DBZ-3926)
* Error processing binlog event [DBZ-3989](https://issues.redhat.com/browse/DBZ-3989)
* Upgrade Java version for GH actions [DBZ-3993](https://issues.redhat.com/browse/DBZ-3993)
* Replace hard-coded version of MySQL example image with getStableVersion()  [DBZ-4005](https://issues.redhat.com/browse/DBZ-4005)
* Handle SCN gap  [DBZ-4036](https://issues.redhat.com/browse/DBZ-4036)
* Upgrade to Apache Kafka 3.0 [DBZ-4045](https://issues.redhat.com/browse/DBZ-4045)
* Recreate webhook for linking PRs to JIRA issues [DBZ-4065](https://issues.redhat.com/browse/DBZ-4065)
* Recipient email address should be a variable in all Jenkins jobs [DBZ-4071](https://issues.redhat.com/browse/DBZ-4071)
* Allow [ci] tag as commit message prefix  [DBZ-4073](https://issues.redhat.com/browse/DBZ-4073)
* Debezium Docker build job fails on rate limiter [DBZ-4074](https://issues.redhat.com/browse/DBZ-4074)
* Add Postgresql 14 container image (Alpine) [DBZ-4075](https://issues.redhat.com/browse/DBZ-4075)
* Add Postgresql 14 container image [DBZ-4079](https://issues.redhat.com/browse/DBZ-4079)
* Fail Docker build scripts on error [DBZ-4084](https://issues.redhat.com/browse/DBZ-4084)
* Display commit SHA in page footer [DBZ-4110](https://issues.redhat.com/browse/DBZ-4110)
* Handle large comparisons results from GH API to address missing authors in release workflow [DBZ-4112](https://issues.redhat.com/browse/DBZ-4112)
* Add debezium-connect-rest-extension module to GH workflows  [DBZ-4113](https://issues.redhat.com/browse/DBZ-4113)
* Display commit SHA in documentation footer [DBZ-4123](https://issues.redhat.com/browse/DBZ-4123)
* Add Debezium Kafka Connect REST Extension to Debezium Kafka Connect NIGHTLY container image [DBZ-4128](https://issues.redhat.com/browse/DBZ-4128)
* Migrate from Gitter to Zulip [DBZ-4142](https://issues.redhat.com/browse/DBZ-4142)
* Postgres module build times out after 6h on CI [DBZ-4145](https://issues.redhat.com/browse/DBZ-4145)
* Misc. MongoDB connector docs fixes [DBZ-4149](https://issues.redhat.com/browse/DBZ-4149)
* Document Oracle buffering solutions [DBZ-4157](https://issues.redhat.com/browse/DBZ-4157)
* Close open file handle [DBZ-4164](https://issues.redhat.com/browse/DBZ-4164)
* Outreach jobs should test all connectors [DBZ-4165](https://issues.redhat.com/browse/DBZ-4165)
* Broken link in MySQL docs [DBZ-4199](https://issues.redhat.com/browse/DBZ-4199)
* Expose outbox event structure at level of Kafka Connect messages [DBZ-1297](https://issues.redhat.com/browse/DBZ-1297)



## 1.7.0.Final
September 30th 2021 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12374879)

### New features since 1.7.0.CR2

* DBZ-UI - Provide list of configurations [DBZ-3960](https://issues.jboss.org/browse/DBZ-3960)
* add ProtobufConverter for Cassandra CDC [DBZ-3906](https://issues.redhat.com/browse/DBZ-3906)


### Breaking changes since 1.7.0.CR2

* Cassandra UUID handling [DBZ-3885](https://issues.jboss.org/browse/DBZ-3885)


### Fixes since 1.7.0.CR2

* java.lang.RuntimeException: com.microsoft.sqlserver.jdbc.SQLServerException: The connection is closed [DBZ-3346](https://issues.jboss.org/browse/DBZ-3346)
* Oracle connector unable to start in archive only mode [DBZ-3712](https://issues.jboss.org/browse/DBZ-3712)
* DDL statement couldn't be parsed [DBZ-4026](https://issues.jboss.org/browse/DBZ-4026)
* Question about handling Raw column types [DBZ-4037](https://issues.jboss.org/browse/DBZ-4037)
* Fixing wrong log dir location in Kafka container image [DBZ-4048](https://issues.jboss.org/browse/DBZ-4048)
* Incremental snapshotting of a table can be prematurely terminated after restart [DBZ-4057](https://issues.jboss.org/browse/DBZ-4057)
* Documentation - Setting up Db2 - Step 10 (Start the ASN agent) is not accurate [DBZ-4044](https://issues.jboss.org/browse/DBZ-4044)
* Debezium Server uses MySQL driver version as defined in Quarkus not in Debezium [DBZ-4049](https://issues.jboss.org/browse/DBZ-4049)
* Events are missed with Oracle connector due to LGWR buffer not being flushed to redo logs [DBZ-4067](https://issues.jboss.org/browse/DBZ-4067)
* Postgres JDBC Driver version causes connection issues on some cloud Postgres instances [DBZ-4060](https://issues.jboss.org/browse/DBZ-4060)
* Postgres JDBC Driver version causes connection issues on some cloud Postgres instances [DBZ-4060](https://issues.redhat.com/browse/DBZ-4060)
* UI final connector configuration includes some default values [DBZ-3967](https://issues.redhat.com/browse/DBZ-3967)


### Other changes since 1.7.0.CR2

* Oracle IncrementalSnapshotIT invalid table test fails [DBZ-4040](https://issues.jboss.org/browse/DBZ-4040)
* Document how to enable schema for JSON messages [DBZ-4041](https://issues.jboss.org/browse/DBZ-4041)
* Trigger contributor check action only when PR is opened [DBZ-4058](https://issues.jboss.org/browse/DBZ-4058)
* Provide JMH benchmark for ChangeEventQueue [DBZ-4050](https://issues.jboss.org/browse/DBZ-4050)
* Commit message action fails for multi-line commit messages [DBZ-4047](https://issues.jboss.org/browse/DBZ-4047)



## 1.7.0.CR2
Spetember 23rd 2021 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12374333)

### New features since 1.7.0.CR1

* Support read-only MySQL connection in incremental snapshot [DBZ-3577](https://issues.jboss.org/browse/DBZ-3577)


### Breaking changes since 1.7.0.CR1

None


### Fixes since 1.7.0.CR1

* Connection failure after snapshot wasn't executed for a while [DBZ-3951](https://issues.jboss.org/browse/DBZ-3951)
* Oracle-Connector fails parsing a DDL statement [DBZ-3977](https://issues.jboss.org/browse/DBZ-3977)
* Oracle connector fails after error ORA-01327 [DBZ-4010](https://issues.jboss.org/browse/DBZ-4010)
* Incorrect incremental snapshot DDL triggers snapshot that generates unending* inserts against signalling table [DBZ-4013](https://issues.jboss.org/browse/DBZ-4013)
* Oracle-Connector fails parsing a DDL statement (truncate partition) [DBZ-4017](https://issues.jboss.org/browse/DBZ-4017)


### Other changes since 1.7.0.CR1

* Jenkins build node is based on RHEL 8.0 and requires upgrade [DBZ-3690](https://issues.jboss.org/browse/DBZ-3690)
* Remove `GRANT ALTER ANY TABLE` from Oracle documentation [DBZ-4007](https://issues.jboss.org/browse/DBZ-4007)
* Update deploy action configuration for v3 [DBZ-4009](https://issues.jboss.org/browse/DBZ-4009)
* Website preview via surge.sh [DBZ-4011](https://issues.jboss.org/browse/DBZ-4011)
* Automate contributor check in COPYRIGHT.txt  [DBZ-4023](https://issues.jboss.org/browse/DBZ-4023)
* Provide an example of usage of snapshot.select.statement.overrides [DBZ-3603](https://issues.jboss.org/browse/DBZ-3603)
* Throughput Bottleneck and Inefficient Batching in ChangeEventQueue [DBZ-3887](https://issues.jboss.org/browse/DBZ-3887)
* Performance Bottleneck in TableIdParser String Replacement [DBZ-4015](https://issues.jboss.org/browse/DBZ-4015)



## 1.7.0.CR1
September 16th 2021 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12373513)

### New features since 1.7.0.Beta1

None


### Breaking changes since 1.7.0.Beta1

* Migrate to Fedora base image [DBZ-3939](https://issues.jboss.org/browse/DBZ-3939)


### Fixes since 1.7.0.Beta1

* RedisStreamChangeConsumer - handleBatch - client.xadd should be wrapped with a try catch block [DBZ-3713](https://issues.jboss.org/browse/DBZ-3713)
* Incorrect information in documentation about supplemental logging [DBZ-3776](https://issues.jboss.org/browse/DBZ-3776)
* DML statement couldn't be parsed [DBZ-3892](https://issues.jboss.org/browse/DBZ-3892)
* DEBEZIUM producer stops unexpectedly trying to change column in table which does not exist [DBZ-3898](https://issues.jboss.org/browse/DBZ-3898)
* "binary.handling.mode": "hex" setting works incorrectly for values with trailing zeros [DBZ-3912](https://issues.jboss.org/browse/DBZ-3912)
* System test-suite is unable to work with unreleased Apicurio versions [DBZ-3924](https://issues.jboss.org/browse/DBZ-3924)
* CI support for running Apicurio registry tests [DBZ-3932](https://issues.jboss.org/browse/DBZ-3932)
* Incorrect validation of truncate handling mode [DBZ-3935](https://issues.jboss.org/browse/DBZ-3935)
* protobuf decoder has sends unsigned long as signed for Postgres 13 [DBZ-3937](https://issues.jboss.org/browse/DBZ-3937)
* Field#description() should return a proper java.lang.String when documentation/description is not set [DBZ-3943](https://issues.jboss.org/browse/DBZ-3943)
* MySQL example image not working after upgrade to 8.0 [DBZ-3944](https://issues.jboss.org/browse/DBZ-3944)
* Fix empty high watermark check [DBZ-3947](https://issues.jboss.org/browse/DBZ-3947)
* Oracle Connector replicating data from all PDBs. Missing PDB filter during replication.  [DBZ-3954](https://issues.jboss.org/browse/DBZ-3954)
* Oracle connector Parsing Exception: DDL statement couldn't be parsed [DBZ-3962](https://issues.jboss.org/browse/DBZ-3962)
* FormSwitchComponent not working correctly in case of duplicate STM form [DBZ-3968](https://issues.jboss.org/browse/DBZ-3968)
* Strings with binary collation shouldn't be parsed as Types.BINARY by MySqlAntlrDdlParser. [DBZ-3969](https://issues.jboss.org/browse/DBZ-3969)
* Openshift pods list image preview not found [DBZ-3970](https://issues.jboss.org/browse/DBZ-3970)
* MySqlValueConvertes.java has typo [DBZ-3976](https://issues.jboss.org/browse/DBZ-3976)
* Mysql-Connector fails parsing invalid decimal format DDL statement [DBZ-3984](https://issues.jboss.org/browse/DBZ-3984)
* Connection Factory is not used when validating SQL Server Connector [DBZ-4001](https://issues.jboss.org/browse/DBZ-4001)


### Other changes since 1.7.0.Beta1

* Promote Outbox SMT to GA [DBZ-3584](https://issues.jboss.org/browse/DBZ-3584)
* Clarify lifecycle of snapshot metrics [DBZ-3613](https://issues.jboss.org/browse/DBZ-3613)
* Explore on building non-core repos with corresponding PR branch of core repo and vice-versa [DBZ-3748](https://issues.jboss.org/browse/DBZ-3748)
* Upgrade to binlog-client 0.25.3 [DBZ-3787](https://issues.jboss.org/browse/DBZ-3787)
* RelationalSnapshotChangeEventSource should accept a RelationalDatabaseSchema [DBZ-3818](https://issues.jboss.org/browse/DBZ-3818)
* Create GH Action that flags "octocat" commits [DBZ-3822](https://issues.jboss.org/browse/DBZ-3822)
* Publish Maven repo with downstream artifacts [DBZ-3861](https://issues.jboss.org/browse/DBZ-3861)
* CI preparation for Apicurio Registry downstream [DBZ-3908](https://issues.jboss.org/browse/DBZ-3908)
* Specify branch name on push/pull_request step in all GH action workflows [DBZ-3913](https://issues.jboss.org/browse/DBZ-3913)
* Consistently order releases from new to old on the website [DBZ-3917](https://issues.jboss.org/browse/DBZ-3917)
* Update RELEASING.md [DBZ-3918](https://issues.jboss.org/browse/DBZ-3918)
* Update antora.yml file with new values for SMT attributes [DBZ-3922](https://issues.jboss.org/browse/DBZ-3922)
* Documentation update should not trigger staging workflow build  [DBZ-3923](https://issues.jboss.org/browse/DBZ-3923)
* Upgrade to Jackson Databind version 2.10.5.1 [DBZ-3927](https://issues.jboss.org/browse/DBZ-3927)
* Add top-level Transformation menu node for downstream docs [DBZ-3931](https://issues.jboss.org/browse/DBZ-3931)
* Docker image serving plugin artifacts over HTTP for new Strimzi deployment mechanism [DBZ-3934](https://issues.jboss.org/browse/DBZ-3934)
* Upgrade MySQL example image to 8.0 [DBZ-3936](https://issues.jboss.org/browse/DBZ-3936)
* Gracefully handle DB history file stored in a sym-linked directory [DBZ-3958](https://issues.jboss.org/browse/DBZ-3958)
* Update docs to specify that connectors track metadata only for transactions that occur after deployment  [DBZ-3961](https://issues.jboss.org/browse/DBZ-3961)
* Update and automate Jenkis Node setup [DBZ-3965](https://issues.jboss.org/browse/DBZ-3965)
* Hyper-link references between options in the Outbox SMT options table  [DBZ-3920](https://issues.jboss.org/browse/DBZ-3920)
* Generify exclusion of columns from snapshotting [DBZ-2525](https://issues.jboss.org/browse/DBZ-2525)
* PoC for adding transformations / SMT steps to the Debezium UI [DBZ-3698](https://issues.jboss.org/browse/DBZ-3698)
* Use No match found of pf Empty state component in filter page. [DBZ-3888](https://issues.jboss.org/browse/DBZ-3888)
* Update the "Skip to review" implementation as per PF new documented standard design pattern [DBZ-3916](https://issues.jboss.org/browse/DBZ-3916)
* Set up MongoDB 5.0 image [DBZ-3973](https://issues.jboss.org/browse/DBZ-3973)



## 1.7.0.Beta1
August 25th 2021 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12359667)

### New features since 1.7.0.Alpha1

* Sink adapter for Nats Streaming [DBZ-3815](https://issues.jboss.org/browse/DBZ-3815)
* Debezium Server's run.sh startup script fails on msys or cygwin bash [DBZ-3840](https://issues.jboss.org/browse/DBZ-3840)
* Upgrade Debezium Server Pravega sink to 0.9.1 [DBZ-3841](https://issues.jboss.org/browse/DBZ-3841)


### Breaking changes since 1.7.0.Alpha1

* Upgrade MySQL driver dependency to latest version [DBZ-3833](https://issues.jboss.org/browse/DBZ-3833)


### Fixes since 1.7.0.Alpha1

* Create example for using self-managed Debezium with MK [DBZ-2947](https://issues.jboss.org/browse/DBZ-2947)
* Exception when validating `field.exclude.list` for Mongo DB connectors [DBZ-3028](https://issues.jboss.org/browse/DBZ-3028)
* In case of `/api/connectors/1` takes longer time(more than pooling) to fail spinner keeps on loading. [DBZ-3313](https://issues.jboss.org/browse/DBZ-3313)
* SQL Server CDC event timestamps do not get converted to UTC [DBZ-3479](https://issues.jboss.org/browse/DBZ-3479)
* Debezium snapshot.select.statement.overrides overrides not used [DBZ-3760](https://issues.jboss.org/browse/DBZ-3760)
* Server name pattern is unnecessarily restrictive. [DBZ-3765](https://issues.jboss.org/browse/DBZ-3765)
* Crash when processing MySQL 5.7.28 TIME fields returns empty blob instead of null [DBZ-3773](https://issues.jboss.org/browse/DBZ-3773)
* Debezium UI and CDC   [DBZ-3781](https://issues.jboss.org/browse/DBZ-3781)
* Disable "Next" if any field value is changed after the validation. [DBZ-3783](https://issues.jboss.org/browse/DBZ-3783)
* Add DEFAULT to partition option engine [DBZ-3784](https://issues.jboss.org/browse/DBZ-3784)
* Initiating MongoDB connector causes oplog table scan [DBZ-3788](https://issues.jboss.org/browse/DBZ-3788)
* SRCFG00014: The config property debezium.sink.pravega.scope is required but it could not be found in any config source [DBZ-3792](https://issues.jboss.org/browse/DBZ-3792)
* LSN component of Postgres sequence numbers is not updated [DBZ-3801](https://issues.jboss.org/browse/DBZ-3801)
* Debezium 1.6.1 expecting database.port even when database.url is provided in config. [DBZ-3813](https://issues.jboss.org/browse/DBZ-3813)
* Postgres numeric default value throwing exception [DBZ-3816](https://issues.jboss.org/browse/DBZ-3816)
* SQL Server connector doesn't handle retriable errors during task start [DBZ-3823](https://issues.jboss.org/browse/DBZ-3823)
*  Debezium OpenShift integration test-suite failure [DBZ-3824](https://issues.jboss.org/browse/DBZ-3824)
* Debezium Server Kinesis Sink Cannot Handle Null Events [DBZ-3827](https://issues.jboss.org/browse/DBZ-3827)
* Timeout when reading from MongoDB oplog cannot be controlled [DBZ-3836](https://issues.jboss.org/browse/DBZ-3836)
* Snapshot locking mode "minimal_percona" incorrectly resets transaction & isolation state [DBZ-3838](https://issues.jboss.org/browse/DBZ-3838)
* Properly skip tests when minor/patch are not specified [DBZ-3839](https://issues.jboss.org/browse/DBZ-3839)
* Truncate validation should verify key schema is null and not value schema [DBZ-3842](https://issues.jboss.org/browse/DBZ-3842)
* System test-suite fails if CRD already exist within the cluster [DBZ-3846](https://issues.jboss.org/browse/DBZ-3846)
* Incorrect test-tags for OcpAvroDB2ConnectorIT [DBZ-3851](https://issues.jboss.org/browse/DBZ-3851)
* System  test-suite CI job does not have RHEL image parameter [DBZ-3852](https://issues.jboss.org/browse/DBZ-3852)
* Typo with prodname asciidoc attribute usage [DBZ-3856](https://issues.jboss.org/browse/DBZ-3856)
* SQL Server Connector finds tables for streaming but not snapshot [DBZ-3857](https://issues.jboss.org/browse/DBZ-3857)
* Signaling table id column too small in example [DBZ-3867](https://issues.jboss.org/browse/DBZ-3867)
* Oracle unparsable DDL issue [DBZ-3877](https://issues.jboss.org/browse/DBZ-3877)
* Support AS clause in GRANT statement [DBZ-3878](https://issues.jboss.org/browse/DBZ-3878)
* Error Parsing Oracle DDL dropping PK [DBZ-3886](https://issues.jboss.org/browse/DBZ-3886)
* Q3 docs referencing Service Registry 2.0 docs [DBZ-3891](https://issues.jboss.org/browse/DBZ-3891)
* EMPTY_CLOB() and EMPTY_BLOB() should be treated as empty LOB values [DBZ-3893](https://issues.jboss.org/browse/DBZ-3893)
* Oracle DDL parsing issue [DBZ-3896](https://issues.jboss.org/browse/DBZ-3896)


### Other changes since 1.7.0.Alpha1

* Debezium UI participating in upstream releases -- follow-up [DBZ-3169](https://issues.jboss.org/browse/DBZ-3169)
* Discuss SMT predicates in docs [DBZ-3227](https://issues.jboss.org/browse/DBZ-3227)
* Test failure for SqlServerConnectorIT#excludeColumnWhenCaptureInstanceExcludesColumns [DBZ-3228](https://issues.jboss.org/browse/DBZ-3228)
* Adjust to changed Strimzi CRDs [DBZ-3385](https://issues.jboss.org/browse/DBZ-3385)
* Create a smoke test for Debezium with Kafka on RHEL [DBZ-3387](https://issues.jboss.org/browse/DBZ-3387)
* Promote Debezium support on RHEL to GA [DBZ-3406](https://issues.jboss.org/browse/DBZ-3406)
* Oracle Docs for TP [DBZ-3407](https://issues.jboss.org/browse/DBZ-3407)
* Upgrade to Kafka 2.8 [DBZ-3444](https://issues.jboss.org/browse/DBZ-3444)
* Update Debezium on RHEL documentation for GA [DBZ-3462](https://issues.jboss.org/browse/DBZ-3462)
* Options in outbox router docs not linked [DBZ-3649](https://issues.jboss.org/browse/DBZ-3649)
* Create Kafka related images based on UBI-8 for RHEL certification [DBZ-3650](https://issues.jboss.org/browse/DBZ-3650)
* Error in description of the property column.mask.hash._hashAlgorithm_.with.salt._salt_ [DBZ-3802](https://issues.jboss.org/browse/DBZ-3802)
* Debezium does not provide up-to-date container images [DBZ-3809](https://issues.jboss.org/browse/DBZ-3809)
* Change DBZ kafka image , so its start script can be used on QA Rhel kafka [DBZ-3810](https://issues.jboss.org/browse/DBZ-3810)
* Test with Apicurio Registry 2.0 in system level test-suite [DBZ-3812](https://issues.jboss.org/browse/DBZ-3812)
* Upgrade commons-compress from 1.20 to 1.21 [DBZ-3819](https://issues.jboss.org/browse/DBZ-3819)
* Update jenkins job configuration to incorporate recent system-testsuite changes [DBZ-3825](https://issues.jboss.org/browse/DBZ-3825)
* Test Failure - RecordsStreamProducerIT#testEmptyChangesProducesHeartbeat [DBZ-3828](https://issues.jboss.org/browse/DBZ-3828)
* Upgrade UI proxy connectors to 1.6.1.Final [DBZ-3837](https://issues.jboss.org/browse/DBZ-3837)
* Improperly constructed links generating downstream build errors [DBZ-3858](https://issues.jboss.org/browse/DBZ-3858)
* CI Failure in VitessConnectorIT.shouldOutputRecordsInCloudEventsFormat [DBZ-3863](https://issues.jboss.org/browse/DBZ-3863)
* CI Failure for StreamingSourceIT.shouldFailOnSchemaInconsistency [DBZ-3869](https://issues.jboss.org/browse/DBZ-3869)
* Extract new top-level menu node for SMTs [DBZ-3873](https://issues.jboss.org/browse/DBZ-3873)
* Introduce documentation variables for AMQ [DBZ-3879](https://issues.jboss.org/browse/DBZ-3879)
* Don't log error when dropping non-existent replication slot in tests [DBZ-3889](https://issues.jboss.org/browse/DBZ-3889)
* Intermittent test failures on CI: VitessConnectorIT::shouldUseUniqueKeyAsRecordKey [DBZ-3900](https://issues.jboss.org/browse/DBZ-3900)
* Intermittent test failures on CI: IncrementalSnapshotIT#updatesWithRestart [DBZ-3901](https://issues.jboss.org/browse/DBZ-3901)
* Test shouldNotEmitDdlEventsForNonTableObjects randomly fails [DBZ-3902](https://issues.jboss.org/browse/DBZ-3902)
* VOLUME instruction causes issue with recent Docker versions [DBZ-3903](https://issues.jboss.org/browse/DBZ-3903)
* Provide ability to denote UI order in field metadata [DBZ-3904](https://issues.jboss.org/browse/DBZ-3904)
* Make relocation.dir and offset.dir configs required. [DBZ-2251](https://issues.jboss.org/browse/DBZ-2251)
* Create Debezium API Spec Generator and static API definitions for connectors [DBZ-3364](https://issues.jboss.org/browse/DBZ-3364)
* Improve incremental snapshot metrics [DBZ-3688](https://issues.jboss.org/browse/DBZ-3688)
* Import Pattern-fly CSS from @patternfly/patternfly [DBZ-3779](https://issues.jboss.org/browse/DBZ-3779)
* Allow system testsuite  to produce Strimzi image for arbitrary released version of Debezium [DBZ-3826](https://issues.jboss.org/browse/DBZ-3826)
* PostgreSQL - Minor Performance bottleneck in PostgresChangeRecordEmitter [DBZ-3870](https://issues.jboss.org/browse/DBZ-3870)
* Oracle - Provide a more user-friendly way to update SCN [DBZ-3876](https://issues.jboss.org/browse/DBZ-3876)
* Test failure on CI - SqlServerConnectorIT#readOnlyApplicationIntent [DBZ-2398](https://issues.jboss.org/browse/DBZ-2398)
* Test failure for SqlServerConnectorIT#EventProcessingFailureHandlingIT [DBZ-3229](https://issues.jboss.org/browse/DBZ-3229)
* Remove underscore from Debezium Server NATS sink Java package name [DBZ-3910](https://issues.jboss.org/browse/DBZ-3910)
* LogMinerDatabaseStateWriter causes a SQLException [DBZ-3911](https://issues.jboss.org/browse/DBZ-3911)
* Maven release fails due to debezium-testing version handling [DBZ-3909](https://issues.jboss.org/browse/DBZ-3909)
* Zookeeper image should not use archive.apache.org [DBZ-3914](https://issues.jboss.org/browse/DBZ-3914)



## 1.7.0.Alpha1
July 30th 2021 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12354171)

### New features since 1.6.0.Final

* Implement incremental snapshotting for Oracle [DBZ-3692](https://issues.jboss.org/browse/DBZ-3692)
* Implement a LogMiner event buffer SPI [DBZ-3752](https://issues.jboss.org/browse/DBZ-3752)
* Remove `artifacts.url` property from UI config.js [DBZ-3209](https://issues.jboss.org/browse/DBZ-3209)
* Do not mark offset for commit log files with error [DBZ-3366](https://issues.jboss.org/browse/DBZ-3366)
* Support read-only MySQL connection in incremental snapshot [DBZ-3577](https://issues.jboss.org/browse/DBZ-3577)
* CloudEventsConverter does not support Oracle, Db2, or Vitess [DBZ-3668](https://issues.jboss.org/browse/DBZ-3668)
* Allow usernames to be excluded in logminer query [DBZ-3671](https://issues.jboss.org/browse/DBZ-3671)
* Track Oracle session PGA memory consumption [DBZ-3756](https://issues.jboss.org/browse/DBZ-3756)
* Performance issue due to inefficient ObjectMapper initialization [DBZ-3770](https://issues.jboss.org/browse/DBZ-3770)
* Add more smoke tests [DBZ-3789](https://issues.jboss.org/browse/DBZ-3789)


### Breaking changes since 1.6.0.Final

None


### Fixes since 1.6.0.Final

* UI frontend build fails for exported checkout which has no .git dir [DBZ-3265](https://issues.jboss.org/browse/DBZ-3265)
* Broken links in Avro and Outbox Event Router documentation [DBZ-3430](https://issues.jboss.org/browse/DBZ-3430)
* Cassandra connector generates invalid schema name for its CDC records [DBZ-3590](https://issues.jboss.org/browse/DBZ-3590)
* Support invisible columns with MySql 8.0.23+ [DBZ-3623](https://issues.jboss.org/browse/DBZ-3623)
* Db2Connector is unable to establish validation connection [DBZ-3632](https://issues.jboss.org/browse/DBZ-3632)
* Status stays in RUNNING for Postgres Connector after Postgres is stopped [DBZ-3655](https://issues.jboss.org/browse/DBZ-3655)
* Change connection validation log level for better visibility [DBZ-3677](https://issues.jboss.org/browse/DBZ-3677)
* OracleSchemaMigrationIT can throw false positive test failures if test artifacts remain [DBZ-3684](https://issues.jboss.org/browse/DBZ-3684)
* MySQL Connector error after execute a "create role" statement [DBZ-3686](https://issues.jboss.org/browse/DBZ-3686)
* ERROR in Entry module not found: Error: Can't resolve './src' [DBZ-3716](https://issues.jboss.org/browse/DBZ-3716)
* Error parsing query, even with database.history.skip.unparseable.ddl [DBZ-3717](https://issues.jboss.org/browse/DBZ-3717)
* Support for TABLE_TYPE missing form MySQL grammar [DBZ-3718](https://issues.jboss.org/browse/DBZ-3718)
* Oracle LogMiner DdlParser Error [DBZ-3723](https://issues.jboss.org/browse/DBZ-3723)
* Debezium mysql connector plugin throws SQL syntax error during incremental snapshot [DBZ-3725](https://issues.jboss.org/browse/DBZ-3725)
* DDL statement couldn't be parsed [DBZ-3755](https://issues.jboss.org/browse/DBZ-3755)
* Debezium Oracle connector stops with DDL parsing error [DBZ-3759](https://issues.jboss.org/browse/DBZ-3759)
* Exception thrown from getTableColumnsFromDatabase [DBZ-3769](https://issues.jboss.org/browse/DBZ-3769)
* Incorrect regex parsing in start script of kafka image [DBZ-3791](https://issues.jboss.org/browse/DBZ-3791)
* Dropdown items list visibility blocked by wizard footer  [DBZ-3794](https://issues.jboss.org/browse/DBZ-3794)
* Permission issues with DB2 example image [DBZ-3795](https://issues.jboss.org/browse/DBZ-3795)


### Other changes since 1.6.0.Final

* Make consumer of outbox example more resilient [DBZ-1709](https://issues.jboss.org/browse/DBZ-1709)
* Set up CI for debezium-examples repo [DBZ-1749](https://issues.jboss.org/browse/DBZ-1749)
* Refactor LogMinerHelper and SqlUtils [DBZ-2552](https://issues.jboss.org/browse/DBZ-2552)
* Implement tests for UI components [DBZ-3050](https://issues.jboss.org/browse/DBZ-3050)
* Add documentation about new capturing implementation for the MySQL connector to downstream product [DBZ-3140](https://issues.jboss.org/browse/DBZ-3140)
* Remove JSimpleParser [DBZ-3155](https://issues.jboss.org/browse/DBZ-3155)
* Ability to build KC image with Apicurio converters [DBZ-3433](https://issues.jboss.org/browse/DBZ-3433)
* Remove `log.mining.history.xxx` deprecated options  [DBZ-3581](https://issues.jboss.org/browse/DBZ-3581)
* Un-document deprecated options and metrics [DBZ-3681](https://issues.jboss.org/browse/DBZ-3681)
* Capture changes made by connector user & document that SYS/SYSTEM changes are not captured [DBZ-3683](https://issues.jboss.org/browse/DBZ-3683)
* Use Debezium thread factory for PG keep-alive [DBZ-3685](https://issues.jboss.org/browse/DBZ-3685)
* Time for another community newsletter [DBZ-3695](https://issues.jboss.org/browse/DBZ-3695)
* Improve signalling documentation [DBZ-3699](https://issues.jboss.org/browse/DBZ-3699)
* Example end-to-end fails due to an API incompatibility with Maven 3.6+ [DBZ-3705](https://issues.jboss.org/browse/DBZ-3705)
* Example debezium-server-name-mapper fails due to an API incompatibility with Maven 3.6+ [DBZ-3706](https://issues.jboss.org/browse/DBZ-3706)
* Doc clarification on connector rewrite [DBZ-3711](https://issues.jboss.org/browse/DBZ-3711)
* Support RHEL deployments in system-test tooling [DBZ-3724](https://issues.jboss.org/browse/DBZ-3724)
* Misc. tutorial updates [DBZ-3747](https://issues.jboss.org/browse/DBZ-3747)
* Update Oracle connector deployment instructions for consistency [DBZ-3772](https://issues.jboss.org/browse/DBZ-3772)


## 1.6.0.Final
June 30th 2021 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12358966)

### New features since 1.6.0.CR1

* Allow specifying of Oracle archive log location [DBZ-3661](https://issues.redhat.com/browse/DBZ-3661)


### Breaking changes since 1.6.0.CR1

None


### Fixes since 1.6.0.CR1

* Fix connect container build to be compatible with Oracle Instant Client [DBZ-2547](https://issues.redhat.com/browse/DBZ-2547)
* Schema change events of excluded databases are discarded  [DBZ-3622](https://issues.redhat.com/browse/DBZ-3622)
* Provide a descriptive error when enabling log.mining.archive.log.only.mode with an offset SCN that isn't yet in an archive log. [DBZ-3665](https://issues.redhat.com/browse/DBZ-3665)
* When LOB support is disabled, use legacy SCN mining algorithm [DBZ-3676](https://issues.redhat.com/browse/DBZ-3676)


### Other changes since 1.6.0.CR1

* Oracle connector error with tables using unique index keys: "key must not be null"  [DBZ-1211](https://issues.redhat.com/browse/DBZ-1211)
* Database history properties missing in connector docs [DBZ-3459](https://issues.redhat.com/browse/DBZ-3459)
* Oracle connector doc fixes [DBZ-3662](https://issues.redhat.com/browse/DBZ-3662)
* Change the reached max batch size log message to DEBUG level [DBZ-3664](https://issues.redhat.com/browse/DBZ-3664)
* Remove unused code [DBZ-3672](https://issues.redhat.com/browse/DBZ-3672)
* Update deprecated config for debezium smt [DBZ-3673](https://issues.redhat.com/browse/DBZ-3673)
* Align Antlr versions used during testing [DBZ-3675](https://issues.redhat.com/browse/DBZ-3675)



## 1.6.0.CR1
June 24th 2021 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12358695)

### New features since 1.6.0.Beta2

* Implement SKIPPED_OPERATIONS for SQLServer [DBZ-2697](https://issues.redhat.com/browse/DBZ-2697)
* Handling database connection timeout during schema recovery [DBZ-3615](https://issues.redhat.com/browse/DBZ-3615)
* Scope mined DDL events to include/exclude lists if provided [DBZ-3634](https://issues.redhat.com/browse/DBZ-3634)
* Support heartbeats during periods of low change event activity [DBZ-3639](https://issues.redhat.com/browse/DBZ-3639)


### Breaking changes since 1.6.0.Beta2

None


### Fixes since 1.6.0.Beta2

* Fix exception on not found table [DBZ-3523](https://issues.redhat.com/browse/DBZ-3523)
* Transaction commit event dispatch fails if no active transaction in progress. [DBZ-3593](https://issues.redhat.com/browse/DBZ-3593)
* Additional unique index referencing columns not exposed by CDC causes exception [DBZ-3597](https://issues.redhat.com/browse/DBZ-3597)
* GRANT/REVOKE for roles is not working [DBZ-3610](https://issues.redhat.com/browse/DBZ-3610)
* ParsingException for ALTER TABLE against a table that is unknown to the connector. [DBZ-3612](https://issues.redhat.com/browse/DBZ-3612)
* Oracle connector continually logging warnings about already processed transactions. [DBZ-3616](https://issues.redhat.com/browse/DBZ-3616)
* StringIndexOutOfBoundsException thrown while handling UTF-8 characters [DBZ-3618](https://issues.redhat.com/browse/DBZ-3618)
* DDL ParsingException - "SUPPLEMENTAL LOG DATA (UNIQUE INDEX) COLUMNS" [DBZ-3619](https://issues.redhat.com/browse/DBZ-3619)
* Oracle transaction reconciliation fails to lookup primary key columns if UPDATE sets columns to only NULL [DBZ-3631](https://issues.redhat.com/browse/DBZ-3631)
* Oracle DDL parser fails on CREATE TABLE: mismatched input 'maxtrans' expecting {'AS', ';'} [DBZ-3641](https://issues.redhat.com/browse/DBZ-3641)
* Antlr version mismatch [DBZ-3646](https://issues.redhat.com/browse/DBZ-3646)
* SQL Agent does not start in SqlServer  image when deployed to openshift [DBZ-3648](https://issues.redhat.com/browse/DBZ-3648)
* Java UBI image is lacking gzip utility [DBZ-3659](https://isssues.redhat.com/browse/DBZ-3659)

### Other changes since 1.6.0.Beta2

* Upgrade to Apicurio Registry 2.0 [DBZ-3171](https://issues.redhat.com/browse/DBZ-3171)
* Vitess: rename "master" branch to "main" [DBZ-3275](https://issues.redhat.com/browse/DBZ-3275)
* Formatting updates to correct errors in documentation builds [DBZ-3518](https://issues.redhat.com/browse/DBZ-3518)
* Prepare test-suite for Kafka on RHEL [DBZ-3566](https://issues.redhat.com/browse/DBZ-3566)
* Upgrade to Quarkus 2.0.0.Final [DBZ-3602](https://issues.redhat.com/browse/DBZ-3602)
* Some dependencies are broken in ocp testsuite after BOM introduction [DBZ-3625](https://issues.redhat.com/browse/DBZ-3625)
* Handle updated json schema for connector passwords [DBZ-3637](https://issues.redhat.com/browse/DBZ-3637)
* MySQL SourceInfo should be public [DBZ-3638](https://issues.redhat.com/browse/DBZ-3638)
* Change CLOB/BLOB data type support to an opt-in feature [DBZ-3645](https://issues.redhat.com/browse/DBZ-3645)
* Denote BLOB support as incubating [DBZ-3651](https://issues.redhat.com/browse/DBZ-3651)



## 1.6.0.Beta2
June 10th 2021 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12358021)

### New features since 1.6.0.Beta1

* Clarification on MySQL vs MariaDb Usage [DBZ-1145](https://issues.jboss.org/browse/DBZ-1145)
* Pravega sink for Debezium Server [DBZ-3546](https://issues.jboss.org/browse/DBZ-3546)
* Postgres - Column default values are not extracted [DBZ-2790](https://issues.jboss.org/browse/DBZ-2790)
* Add support for snapshot.include.collection.list [DBZ-3062](https://issues.jboss.org/browse/DBZ-3062)
* Apply filters with empty filter changes 'Exclude' selection to 'Include' [DBZ-3102](https://issues.jboss.org/browse/DBZ-3102)
* Adjust OpenShift tests to support new version of Strimzi CRDs [DBZ-3475](https://issues.jboss.org/browse/DBZ-3475)
* Remove SchemaProcessor From Cassandra Connector [DBZ-3506](https://issues.jboss.org/browse/DBZ-3506)
* Provide a `snapshot.locking.mode` option for Oracle [DBZ-3557](https://issues.jboss.org/browse/DBZ-3557)
* Implement support for JSON function in MySQL parser [DBZ-3559](https://issues.jboss.org/browse/DBZ-3559)


### Breaking changes since 1.6.0.Beta1

None


### Fixes since 1.6.0.Beta1

* AbstractConnectorTest should work in environment with longer latency [DBZ-400](https://issues.jboss.org/browse/DBZ-400)
* PostgreSQL connector task fails to resume streaming because replication slot is active [DBZ-3068](https://issues.jboss.org/browse/DBZ-3068)
* SQL Server connector buffers all CDC events in memory if more than one table is captured [DBZ-3486](https://issues.jboss.org/browse/DBZ-3486)
* SQLServer low throughput tables increase usage of TempDB [DBZ-3515](https://issues.jboss.org/browse/DBZ-3515)
* Incorrectly identifies primary member of replica set [DBZ-3522](https://issues.jboss.org/browse/DBZ-3522)
* Cannot enable binlog streaming when INITIAL_ONLY snapshot mode configured [DBZ-3529](https://issues.jboss.org/browse/DBZ-3529)
* Connector CRD name and database.server.name cannot use the same value in OCP test-suite [DBZ-3538](https://issues.jboss.org/browse/DBZ-3538)
* SelectLobParser checks for lowercase "is null" instead of uppercase "IS NULL" [DBZ-3545](https://issues.jboss.org/browse/DBZ-3545)
* DDL ParsingException "mismatched input 'sharing'" for create table syntax. [DBZ-3549](https://issues.jboss.org/browse/DBZ-3549)
* DDL ParsingException on alter table [DBZ-3554](https://issues.jboss.org/browse/DBZ-3554)
* ORA-00310 when online redo log is archived and replaced by redo log with new sequence [DBZ-3561](https://issues.jboss.org/browse/DBZ-3561)
* Server name pattern is unnecessarily restrictive [DBZ-3562](https://issues.jboss.org/browse/DBZ-3562)
* ORA-01289 error encountered on Oracle RAC when multiple logs are mined with same sequence number [DBZ-3563](https://issues.jboss.org/browse/DBZ-3563)
* MySQL metrics documentation refers to legacy implementation [DBZ-3572](https://issues.jboss.org/browse/DBZ-3572)
* Update downstream MySQL doc to reference streaming metrics vs. binlog metrics  [DBZ-3582](https://issues.jboss.org/browse/DBZ-3582)
* No viable alternative at input "add COLUMN optional" [DBZ-3586](https://issues.jboss.org/browse/DBZ-3586)
* NPE when OracleValueConverters get unsupported jdbc type [DBZ-3587](https://issues.jboss.org/browse/DBZ-3587)
* SelectLobParser throws NullPointerException when parsing SQL for an unknown table [DBZ-3591](https://issues.jboss.org/browse/DBZ-3591)
* Pulsar sink tries to convert null key to string [DBZ-3595](https://issues.jboss.org/browse/DBZ-3595)
* Oracle RAC URL does not correctly substitute node IP addresses [DBZ-3599](https://issues.jboss.org/browse/DBZ-3599)
* Oracle Connector - got InputMismatchException mismatched input 'CASCADE' expecting {'AS', 'PURGE', ';'} [DBZ-3606](https://issues.jboss.org/browse/DBZ-3606)


### Other changes since 1.6.0.Beta1

* Unsupported column types should be ignored as with other connectors [DBZ-814](https://issues.jboss.org/browse/DBZ-814)
* Make outbox extensions dependency on tracing extension optional [DBZ-2834](https://issues.jboss.org/browse/DBZ-2834)
* Avoid copying in DML handling [DBZ-3328](https://issues.jboss.org/browse/DBZ-3328)
* Document impact of using --hostname when starting Connect container [DBZ-3466](https://issues.jboss.org/browse/DBZ-3466)
* Update external link to AMQ Streams documentation [DBZ-3502](https://issues.jboss.org/browse/DBZ-3502)
* Update external links in downstream docs to AMQ Streams deployment information  [DBZ-3525](https://issues.jboss.org/browse/DBZ-3525)
* Debezium Server Core builds plugin artifact [DBZ-3542](https://issues.jboss.org/browse/DBZ-3542)
* List contributors script fails when name contains a "/" character [DBZ-3544](https://issues.jboss.org/browse/DBZ-3544)
* Upgrade to Quarkus 2.0.0.CR3 [DBZ-3550](https://issues.jboss.org/browse/DBZ-3550)
* Reduce DB round-trips for LOB handling [DBZ-3556](https://issues.jboss.org/browse/DBZ-3556)
* Oracle benchmark does not execute LogMiner parser performance tests [DBZ-3560](https://issues.jboss.org/browse/DBZ-3560)
* Clarify purpose of database.history.retention.hours [DBZ-3565](https://issues.jboss.org/browse/DBZ-3565)
* Improve documentation related to signalling table DDL [DBZ-3568](https://issues.jboss.org/browse/DBZ-3568)
* cassandra-driver-core 3.5.0 managed in Debezium BOM too old for testcontainers 1.15.3 [DBZ-3589](https://issues.jboss.org/browse/DBZ-3589)
* Remove some dead code in Postgres connector [DBZ-3596](https://issues.jboss.org/browse/DBZ-3596)
* Debezium server sink oracle database to pulsar without default namespace "public/default" [DBZ-3601](https://issues.jboss.org/browse/DBZ-3601)
* Document OffsetContext.incrementalSnapshotEvents() [DBZ-3607](https://issues.jboss.org/browse/DBZ-3607)
* Database skipping logic isn't correct [DBZ-3608](https://issues.jboss.org/browse/DBZ-3608)



## 1.6.0.Beta1
May 20th 2021 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12357565)

### New features since 1.6.0.Alpha1

* Support ad hoc snapshots on MySQL connector [DBZ-66](https://issues.jboss.org/browse/DBZ-66)
* Support DDL operations [DBZ-2916](https://issues.jboss.org/browse/DBZ-2916)
* Add support for RAW, LONG, LONG RAW, BLOB, and CLOB data types [DBZ-2948](https://issues.jboss.org/browse/DBZ-2948)
* Update Doc For Cassandra Connector [DBZ-3092](https://issues.jboss.org/browse/DBZ-3092)
* Document log.mining.strategy for Oracle connector [DBZ-3393](https://issues.jboss.org/browse/DBZ-3393)
* Update DOC with the new NUM_OF_CHANGE_EVENT_QUEUES parameter [DBZ-3480](https://issues.jboss.org/browse/DBZ-3480)
* Use date format model that does not depend on client NLS settings in integration tests [DBZ-3482](https://issues.jboss.org/browse/DBZ-3482)
* Provide Japanese translation of README.md  [DBZ-3503](https://issues.jboss.org/browse/DBZ-3503)
* Better handling of invalid SQL Server connector configuration [DBZ-3505](https://issues.jboss.org/browse/DBZ-3505)
* Allow table.include.list and table.exclude.list to be updated after a connector is created [DBZ-1263](https://issues.jboss.org/browse/DBZ-1263)
* Allow retry when SQL Server is down temporarily [DBZ-3339](https://issues.jboss.org/browse/DBZ-3339)


### Breaking changes since 1.6.0.Alpha1

* Rename table stores only a fragment of DDL in schema history [DBZ-3399](https://issues.jboss.org/browse/DBZ-3399)


### Fixes since 1.6.0.Alpha1

* Database name should not be converted to lower case if tablenameCaseInsensitive=True in Oracle Connector [DBZ-2203](https://issues.jboss.org/browse/DBZ-2203)
* Not able to configure Debezium Server via smallrye/microprofile environment variables [DBZ-2622](https://issues.jboss.org/browse/DBZ-2622)
* Upgrading from debezium 1.2.2 to 1.4.0 stopped snapshotting new tables [DBZ-2944](https://issues.jboss.org/browse/DBZ-2944)
* oracle logminer cannot add duplicate logfile [DBZ-3266](https://issues.jboss.org/browse/DBZ-3266)
* Oracle connector does not correctly handle partially committed transactions [DBZ-3322](https://issues.jboss.org/browse/DBZ-3322)
* Data loss when MongoDB snapshot take longer than the Oplog Window [DBZ-3331](https://issues.jboss.org/browse/DBZ-3331)
* First online log query does not limit results to those that are available. [DBZ-3332](https://issues.jboss.org/browse/DBZ-3332)
* Connector crashing after running for some time [DBZ-3377](https://issues.jboss.org/browse/DBZ-3377)
* Broken links in downstream Monitoring chapter [DBZ-3408](https://issues.jboss.org/browse/DBZ-3408)
* Broken links in User guide table of routing SMT configuration options [DBZ-3410](https://issues.jboss.org/browse/DBZ-3410)
* Broken link to basic configuration example in downstream content-based routing topic [DBZ-3412](https://issues.jboss.org/browse/DBZ-3412)
* Cassandra connector does not react on schema changes properly [DBZ-3417](https://issues.jboss.org/browse/DBZ-3417)
* Debezium mapped diagnostic contexts doesn't work [DBZ-3438](https://issues.jboss.org/browse/DBZ-3438)
* source.timestamp.mode=commit imposes a significant performance penalty [DBZ-3452](https://issues.jboss.org/browse/DBZ-3452)
* Timezone difference not considered in `LagFromSourceInMilliseconds` calculation [DBZ-3456](https://issues.jboss.org/browse/DBZ-3456)
* "Found null value for non-optional schema" error when issuing TRUNCATE from Postgres on a table with a PK [DBZ-3469](https://issues.jboss.org/browse/DBZ-3469)
* Connector crashes when table name contains '-' character [DBZ-3485](https://issues.jboss.org/browse/DBZ-3485)
* Kafka Clients in Debezium Server is not aligned with Debezium Kafka version [DBZ-3498](https://issues.jboss.org/browse/DBZ-3498)
* ReadToInsertEvent SMT needs to set ConfigDef [DBZ-3508](https://issues.jboss.org/browse/DBZ-3508)
* Debezium configuration can be modified after instantiation [DBZ-3514](https://issues.jboss.org/browse/DBZ-3514)
* Oracle redo log switch not detected when using multiple archiver process threads [DBZ-3516](https://issues.jboss.org/browse/DBZ-3516)
* Cannot enable binlog streaming when INITIAL_ONLY snapshot mode configured [DBZ-3529](https://issues.jboss.org/browse/DBZ-3529)
* Missing schema function in DDL Parser [DBZ-3543](https://issues.jboss.org/browse/DBZ-3543)
* Retry logic for "No more data to read from socket" is too strict [DBZ-3472](https://issues.jboss.org/browse/DBZ-3472)


### Other changes since 1.6.0.Alpha1

* Document new source block and fix formatting issues [DBZ-1614](https://issues.jboss.org/browse/DBZ-1614)
* Re-connect after "too many connections" [DBZ-2300](https://issues.jboss.org/browse/DBZ-2300)
* Modularize doc for MongoDB component [DBZ-2334](https://issues.jboss.org/browse/DBZ-2334)
* Rebase Postgres snapshot modes on exported snapshots [DBZ-2337](https://issues.jboss.org/browse/DBZ-2337)
* Enable continuous JFR recording [DBZ-3082](https://issues.jboss.org/browse/DBZ-3082)
* Remove deprecated Oracle connector option "database.tablename.case.insensitive" [DBZ-3240](https://issues.jboss.org/browse/DBZ-3240)
* Improve Oracle redo logs query to avoid de-duplication step [DBZ-3256](https://issues.jboss.org/browse/DBZ-3256)
* Migrate Jenkins CI to OCP 4.0 in  PSI cloud  [DBZ-3396](https://issues.jboss.org/browse/DBZ-3396)
* Remove Antlr-based DML Parser [DBZ-3400](https://issues.jboss.org/browse/DBZ-3400)
* Update Oracle driver version [DBZ-3460](https://issues.jboss.org/browse/DBZ-3460)
* Incremental snapshot follow-up tasks [DBZ-3500](https://issues.jboss.org/browse/DBZ-3500)
* Unnecessary NPE due to autoboxing [DBZ-3519](https://issues.jboss.org/browse/DBZ-3519)
* Upgrade actions/cache to v2 version for formatting check [DBZ-3520](https://issues.jboss.org/browse/DBZ-3520)
* Improve documentation for Oracle supplemental logging requirements [DBZ-3521](https://issues.jboss.org/browse/DBZ-3521)
* SignalsIT leave table artifacts that cause other tests to fail [DBZ-3533](https://issues.jboss.org/browse/DBZ-3533)
* Mark xstream dependency as provided [DBZ-3539](https://issues.jboss.org/browse/DBZ-3539)
* Add test for Oracle table without PK [DBZ-832](https://issues.jboss.org/browse/DBZ-832)



## 1.6.0.Alpha1
May 6th 2021 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12353176)

### New features since 1.5.0.Final

* Sink adapter for Apache Kafka [DBZ-3382](https://issues.jboss.org/browse/DBZ-3382)
* Optimisation on MongoDB and MySQL connector for skipped.operations [DBZ-3403](https://issues.jboss.org/browse/DBZ-3403)
* Incremental snapshotting [DBZ-3473](https://issues.jboss.org/browse/DBZ-3473)


### Breaking changes since 1.5.0.Final

* Build Debezium with Java 11 [DBZ-2870](https://issues.jboss.org/browse/DBZ-2870)


### Fixes since 1.5.0.Final

* io.debezium.text.ParsingException: no viable alternative at input 'IDNUMBER(4)GENERATEDBY' [DBZ-1721](https://issues.jboss.org/browse/DBZ-1721)
* SKIPPED_OPERATIONS is added to CommonConnectorConfig.CONFIG_DEFINITION although it's not implemented in all connectors [DBZ-2699](https://issues.jboss.org/browse/DBZ-2699)
* Snapshot fails when reading TIME, DATE, DATETIME fields in mysql from ResultSet [DBZ-3238](https://issues.jboss.org/browse/DBZ-3238)
* Update to fabric8 kube client 5.x [DBZ-3349](https://issues.jboss.org/browse/DBZ-3349)
* An exception in resolveOracleDatabaseVersion if system language is not English [DBZ-3397](https://issues.jboss.org/browse/DBZ-3397)
* Change strimzi branch in jenkins openshift-test job to main [DBZ-3404](https://issues.jboss.org/browse/DBZ-3404)
* Broken link in downstream Monitoring chapter 7.3 [DBZ-3409](https://issues.jboss.org/browse/DBZ-3409)
* Broken link in content-based routing chapter to page for downloading the SMT scripting archive  [DBZ-3411](https://issues.jboss.org/browse/DBZ-3411)
* LogMinerDmlParser mishandles double single quotes in WHERE clauses [DBZ-3413](https://issues.jboss.org/browse/DBZ-3413)
* Incorrectly formatted links in downstream automatic topic creation doc [DBZ-3414](https://issues.jboss.org/browse/DBZ-3414)
* SMT acronym incorrectly expanded in Debezium User Guide [DBZ-3415](https://issues.jboss.org/browse/DBZ-3415)
* MariaDB -- support privilege DDL in parser [DBZ-3422](https://issues.jboss.org/browse/DBZ-3422)
* Change oc apply in jenkins openshift-test job to oc create [DBZ-3423](https://issues.jboss.org/browse/DBZ-3423)
* SQL Server property (snapshot.select.statement.overrides) only matches 1st entry if comma-separated list also contains spaces [DBZ-3429](https://issues.jboss.org/browse/DBZ-3429)
* Permission issue when running docker-compose or docker build as user not having uid 1001 [DBZ-3453](https://issues.jboss.org/browse/DBZ-3453)
* no viable alternative at input 'DROP TABLE IF EXISTS group' (Galera and MariaDB) [DBZ-3467](https://issues.jboss.org/browse/DBZ-3467)
* Debezium MySQL connector does not process tables with partitions [DBZ-3468](https://issues.jboss.org/browse/DBZ-3468)
* The building tools' version in README doc is outdated [DBZ-3478](https://issues.jboss.org/browse/DBZ-3478)
* MySQL DATE default value parser rejects timestamp [DBZ-3497](https://issues.jboss.org/browse/DBZ-3497)
* MySQL8 GRANT statement not parsable [DBZ-3499](https://issues.jboss.org/browse/DBZ-3499)


### Other changes since 1.5.0.Final

* Config validation for Db2 [DBZ-3118](https://issues.jboss.org/browse/DBZ-3118)
* Add smoke test for UI [DBZ-3133](https://issues.jboss.org/browse/DBZ-3133)
* Create new metric "CapturedTables" [DBZ-3161](https://issues.jboss.org/browse/DBZ-3161)
* Handle deadlock issue for MySql build stuck for 6h [DBZ-3233](https://issues.jboss.org/browse/DBZ-3233)
* Document using Connect REST API for log level changes [DBZ-3270](https://issues.jboss.org/browse/DBZ-3270)
* User Guide corrections for SQL Server connector [DBZ-3297](https://issues.jboss.org/browse/DBZ-3297)
* User Guide corrections for Db2 connector [DBZ-3298](https://issues.jboss.org/browse/DBZ-3298)
* User Guide corrections for MySQL connector [DBZ-3299](https://issues.jboss.org/browse/DBZ-3299)
* User Guide corrections for MongoDB connector [DBZ-3300](https://issues.jboss.org/browse/DBZ-3300)
* Allow building the Oracle connector on CI [DBZ-3365](https://issues.jboss.org/browse/DBZ-3365)
* Add tests for Protobuf Converter [DBZ-3369](https://issues.jboss.org/browse/DBZ-3369)
* Use current SQL Server container image for testing and examples [DBZ-3379](https://issues.jboss.org/browse/DBZ-3379)
* Reword prereq in downstream SQL Server connector doc  [DBZ-3392](https://issues.jboss.org/browse/DBZ-3392)
* Duplicate entry in MySQL connector properties table for `mysql-property-skipped-operations`  [DBZ-3402](https://issues.jboss.org/browse/DBZ-3402)
* Docs clarification around tombstone events [DBZ-3416](https://issues.jboss.org/browse/DBZ-3416)
* Validate logical server name contains only alpha-numerical characters [DBZ-3427](https://issues.jboss.org/browse/DBZ-3427)
* Provide a "quick" build profile [DBZ-3449](https://issues.jboss.org/browse/DBZ-3449)
* Avoid warning about superfluous exclusion during packaging [DBZ-3458](https://issues.jboss.org/browse/DBZ-3458)
* Upgrade binlog client [DBZ-3463](https://issues.jboss.org/browse/DBZ-3463)



## 1.5.0.Final
April 7th 2021 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12354718)

### New features since 1.5.0.CR1

* Add support for Redis Streams target in Debezium Server [DBZ-2879](https://issues.jboss.org/browse/DBZ-2879)
* Provide LSN coordinates as standardized sequence field [DBZ-2911](https://issues.jboss.org/browse/DBZ-2911)


### Breaking changes since 1.5.0.CR1

None

### Fixes since 1.5.0.CR1

* Do not mine Data Guard archive log entries [DBZ-3341](https://issues.jboss.org/browse/DBZ-3341)
* Debezium stuck in an infinite loop on boot [DBZ-3343](https://issues.jboss.org/browse/DBZ-3343)
* Schema change SourceRecords have null partition [DBZ-3347](https://issues.jboss.org/browse/DBZ-3347)
* LogMiner can incorrectly resolve that SCN is available [DBZ-3348](https://issues.jboss.org/browse/DBZ-3348)
* The event.deserialization.failure.handling.mode is documented incorrectly [DBZ-3353](https://issues.jboss.org/browse/DBZ-3353)
* DB2 Function wrong [DBZ-3362](https://issues.jboss.org/browse/DBZ-3362)
* LogMiner parser incorrectly parses UNISTR function [DBZ-3367](https://issues.jboss.org/browse/DBZ-3367)
* Invalid Decimal schema: scale parameter not found [DBZ-3371](https://issues.jboss.org/browse/DBZ-3371)


### Other changes since 1.5.0.Beta2

* Allow Debezium Server to be used with Apicurio converters [DBZ-2388](https://issues.jboss.org/browse/DBZ-2388)
* Remove connector properties from descriptors on the /connector-types response  [DBZ-3316](https://issues.jboss.org/browse/DBZ-3316)
* Literal attribute rendered in deployment instructions for the downstream PostgreSQL connector  [DBZ-3338](https://issues.jboss.org/browse/DBZ-3338)
* Fix test failures due to existing database object artifacts [DBZ-3344](https://issues.jboss.org/browse/DBZ-3344)
* Use correct repository level PAT for building debezium website  [DBZ-3345](https://issues.jboss.org/browse/DBZ-3345)
* Document configuration of max.request.size  [DBZ-3355](https://issues.jboss.org/browse/DBZ-3355)
* Use Java 8 for Cassandra workflow [DBZ-3357](https://issues.jboss.org/browse/DBZ-3357)
* Trigger workflow on workflow definition update [DBZ-3358](https://issues.jboss.org/browse/DBZ-3358)
* Prefer DDL before logical schema in history recovery [DBZ-3361](https://issues.jboss.org/browse/DBZ-3361)
* Add missing space and omitted command to PostgreSQL connector doc  [DBZ-3372](https://issues.jboss.org/browse/DBZ-3372)
* Wrong badge on Docker Hub  [DBZ-3383](https://issues.jboss.org/browse/DBZ-3383)



## 1.5.0.CR1
March 24th 2021 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12354265)

### New features since 1.5.0.Beta2

* Promote Oracle connector from "Incubating" to "Stable" [DBZ-3290](https://issues.jboss.org/browse/DBZ-3290)
* Handle large SCN values in Offsets and SourceInfo block [DBZ-2994](https://issues.jboss.org/browse/DBZ-2994)


### Breaking changes since 1.5.0.Beta2

* Upgrade to Apache Kafka 2.7.0 [DBZ-2872](https://issues.jboss.org/browse/DBZ-2872)
* Add more parameters to TLS support [DBZ-3262](https://issues.jboss.org/browse/DBZ-3262)


### Fixes since 1.5.0.Beta2

* Debezium logs "is not a valid Avro schema name" can be too verbose [DBZ-2511](https://issues.jboss.org/browse/DBZ-2511)
* message.key.columns Regex Validation Time Complexity [DBZ-2957](https://issues.jboss.org/browse/DBZ-2957)
* OID values don't fit to INT32 schema [DBZ-3033](https://issues.jboss.org/browse/DBZ-3033)
* Connector automatically restart on ORA-26653 [DBZ-3236](https://issues.jboss.org/browse/DBZ-3236)
* UI container has no assets (JS artifacts, fonts, etc) and randomly fails building [DBZ-3247](https://issues.jboss.org/browse/DBZ-3247)
* Revert Clob behavior for Oracle LogMiner to avoid null values [DBZ-3257](https://issues.jboss.org/browse/DBZ-3257)
* SQL Server misses description for decimal.handling.mode [DBZ-3267](https://issues.jboss.org/browse/DBZ-3267)
* Oracle connector ignores time.precision.mode and just uses adaptive mode [DBZ-3268](https://issues.jboss.org/browse/DBZ-3268)
* commons-logging JAR is missing from Debezium Server distro [DBZ-3277](https://issues.jboss.org/browse/DBZ-3277)
* MongoDB timeouts crash the whole connector [DBZ-3278](https://issues.jboss.org/browse/DBZ-3278)
* Prefer archive logs over redo logs of the same SCN range [DBZ-3292](https://issues.jboss.org/browse/DBZ-3292)
* LogMiner mining query may unintentionally skip records [DBZ-3295](https://issues.jboss.org/browse/DBZ-3295)
* IndexOutOfBoundsException when LogMiner DML update statement contains a function as last column's value [DBZ-3305](https://issues.jboss.org/browse/DBZ-3305)
* Out of memory with mysql snapshots (regression of DBZ-94) [DBZ-3309](https://issues.jboss.org/browse/DBZ-3309)
* Keyword ORDER is a valid identifier in MySQL grammar [DBZ-3310](https://issues.jboss.org/browse/DBZ-3310)
* DDL statement couldn't be parsed for ROW_FORMAT=TOKUDB_QUICKLZ [DBZ-3311](https://issues.jboss.org/browse/DBZ-3311)
* LogMiner can miss a log switch event if too many switches occur. [DBZ-3319](https://issues.jboss.org/browse/DBZ-3319)
* Function MOD is missing from MySQL grammar [DBZ-3333](https://issues.jboss.org/browse/DBZ-3333)
* Incorrect SR label names in OCP testusite [DBZ-3336](https://issues.jboss.org/browse/DBZ-3336)
* DB2 upstream tests are still using master as the default branch [DBZ-3337](https://issues.jboss.org/browse/DBZ-3337)


### Other changes since 1.5.0.Beta2

* Demo: Exploring non-key joins of Kafka Streams 2.4 [DBZ-2100](https://issues.jboss.org/browse/DBZ-2100)
* Publish Debezium BOM POM [DBZ-2145](https://issues.jboss.org/browse/DBZ-2145)
* Use BigInteger as SCN rather than BigDecimal [DBZ-2457](https://issues.jboss.org/browse/DBZ-2457)
* Document ChangeConsumer usage for Debezium Engine [DBZ-2520](https://issues.jboss.org/browse/DBZ-2520)
* Add check that target release is set [DBZ-2536](https://issues.jboss.org/browse/DBZ-2536)
* Consolidate multiple JMX beans during Oracle streaming with LogMiner [DBZ-2537](https://issues.jboss.org/browse/DBZ-2537)
* Create script for listing all contributors of a release [DBZ-2592](https://issues.jboss.org/browse/DBZ-2592)
* Explicitly mention Debezium Engine database history config for different connectors [DBZ-2665](https://issues.jboss.org/browse/DBZ-2665)
* Cleanup by restructuring Debezium UI REST API structure [DBZ-3031](https://issues.jboss.org/browse/DBZ-3031)
* Make Debezium main repo build checks artifacts for CI/CD checks in sibling repositories available on Maven Central  [DBZ-3142](https://issues.jboss.org/browse/DBZ-3142)
* Handle duplicate warnings for deprecated options [DBZ-3218](https://issues.jboss.org/browse/DBZ-3218)
* Upgrade Jackson as per AK 2.7 [DBZ-3221](https://issues.jboss.org/browse/DBZ-3221)
* Document the need of qualified names in snapshot.include.collection.list [DBZ-3244](https://issues.jboss.org/browse/DBZ-3244)
* Add snapshot.select.statement.override options to Oracle documentation [DBZ-3250](https://issues.jboss.org/browse/DBZ-3250)
* Remove all possible backend calls from non-validation mode [DBZ-3255](https://issues.jboss.org/browse/DBZ-3255)
* Document delayed TX END markers [DBZ-3261](https://issues.jboss.org/browse/DBZ-3261)
* Extended scripting SMT docs with handling of non-data events [DBZ-3269](https://issues.jboss.org/browse/DBZ-3269)
* Unify column inclusion/exclusion handling [DBZ-3271](https://issues.jboss.org/browse/DBZ-3271)
* Downstream conditional spans topic boundary in db2 doc [DBZ-3272](https://issues.jboss.org/browse/DBZ-3272)
* Add info about languge dependencies into scripting SMTs [DBZ-3280](https://issues.jboss.org/browse/DBZ-3280)
* Copyright check script should take additional connector repos into consideration [DBZ-3281](https://issues.jboss.org/browse/DBZ-3281)
* Intermittent failure of MyMetricsIT.testStreamingOnlyMetrics [DBZ-3304](https://issues.jboss.org/browse/DBZ-3304)
* Remove references to supported configurations from Db2 connector documentation [DBZ-3308](https://issues.jboss.org/browse/DBZ-3308)
* Use separate API calls to get the connector info(name, id etc) and details(Properties) [DBZ-3314](https://issues.jboss.org/browse/DBZ-3314)
* Documentation updates should trigger a website build [DBZ-3320](https://issues.jboss.org/browse/DBZ-3320)
* Cassandra connector is not part of core CI build [DBZ-3335](https://issues.jboss.org/browse/DBZ-3335)



## 1.5.0.Beta2
March 12th 2021 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12354047)

### New features since 1.5.0.Beta1

* Detect and skip non-parent index-organized tables [DBZ-3036](https://issues.jboss.org/browse/DBZ-3036)
* Capture additional JMX metrics for LogMiner [DBZ-3038](https://issues.jboss.org/browse/DBZ-3038)
* Incorrect information in Debezium connector for Postgres documentation [DBZ-3197](https://issues.jboss.org/browse/DBZ-3197)
* Add support for SET column type [DBZ-3199](https://issues.jboss.org/browse/DBZ-3199)
* Improve relocation logic for processed commitLog files  [DBZ-3224](https://issues.jboss.org/browse/DBZ-3224)
* Disable log.mining.transaction.retention.hours logic by default [DBZ-3242](https://issues.jboss.org/browse/DBZ-3242)
* Provide a signalling table [DBZ-3141](https://issues.jboss.org/browse/DBZ-3141)
* Update sensitive env vars for connect-base image [DBZ-3223](https://issues.jboss.org/browse/DBZ-3223)
* Support specifying kinesis endpoint in debezium server [DBZ-3246](https://issues.jboss.org/browse/DBZ-3246)
* Add log4j.properties file [DBZ-3248](https://issues.jboss.org/browse/DBZ-3248)


### Breaking changes since 1.5.0.Beta1

* LogMiner does not process NUMBER(1) data [DBZ-3208](https://issues.jboss.org/browse/DBZ-3208)
* Use LogMiner adapter by default for Oracle connector [DBZ-3241](https://issues.jboss.org/browse/DBZ-3241)


### Fixes since 1.5.0.Beta1

* Error in LSN [DBZ-2417](https://issues.jboss.org/browse/DBZ-2417)
* Connector restarts with an SCN that was previously processed. [DBZ-2875](https://issues.jboss.org/browse/DBZ-2875)
* Misleading error message for filtered publication with misconfigured filters [DBZ-2885](https://issues.jboss.org/browse/DBZ-2885)
* There are still important problems with Oracle LogMiner [DBZ-2976](https://issues.jboss.org/browse/DBZ-2976)
* Don't execute initial statements upon connector validation [DBZ-3030](https://issues.jboss.org/browse/DBZ-3030)
* Forever stuck with new binlog parser (1.3 and later) when processing big JSON column data  [DBZ-3106](https://issues.jboss.org/browse/DBZ-3106)
* Change Events are not captured after initial load [DBZ-3128](https://issues.jboss.org/browse/DBZ-3128)
* Repeating Unknown schema error even after recent schema_recovery [DBZ-3146](https://issues.jboss.org/browse/DBZ-3146)
* CloudEvent value id field is not unique [DBZ-3157](https://issues.jboss.org/browse/DBZ-3157)
* Oracle connector fails when using database.tablename.case.insensitive=true [DBZ-3190](https://issues.jboss.org/browse/DBZ-3190)
* DML parser IndexOutOfRangeException with where-clause using "IS NULL" [DBZ-3193](https://issues.jboss.org/browse/DBZ-3193)
* ORA-01284 file cannot be opened error when file locked by another process [DBZ-3194](https://issues.jboss.org/browse/DBZ-3194)
* CommitThroughput metrics can raise division by zero error [DBZ-3200](https://issues.jboss.org/browse/DBZ-3200)
* Update MongoDB driver version [DBZ-3212](https://issues.jboss.org/browse/DBZ-3212)
* Extra connectors are not buildable unless main Debezium is built locally [DBZ-3213](https://issues.jboss.org/browse/DBZ-3213)
* Docker image debezium/server:1.5 won't start [DBZ-3217](https://issues.jboss.org/browse/DBZ-3217)
* Debezium Oracle Connector not excluding table columns [DBZ-3219](https://issues.jboss.org/browse/DBZ-3219)
* LogMiner parse failure with Update DML with no where condition [DBZ-3235](https://issues.jboss.org/browse/DBZ-3235)
* Debezium 1.4.2.Final and onwards unable to parse sasl.jaas.config from env var [DBZ-3245](https://issues.jboss.org/browse/DBZ-3245)
* Debezium engine should call stop on task even when start fails [DBZ-3251](https://issues.jboss.org/browse/DBZ-3251)
* No meaningful message provided when oracle driver is missing [DBZ-3254](https://issues.jboss.org/browse/DBZ-3254)


### Other changes since 1.5.0.Beta1

* Discuss capture job configuration as a tuning option for SQL Server and Db2 [DBZ-2122](https://issues.jboss.org/browse/DBZ-2122)
* Prepare customizing auto-created topics doc for downstream [DBZ-2654](https://issues.jboss.org/browse/DBZ-2654)
* Wrong warning about deprecated options [DBZ-3084](https://issues.jboss.org/browse/DBZ-3084)
* Have non-validating mode in the UI [DBZ-3088](https://issues.jboss.org/browse/DBZ-3088)
* Move container image builds to GH Actions [DBZ-3131](https://issues.jboss.org/browse/DBZ-3131)
* Exclude CommonConnectorConfig.PROVIDE_TRANSACTION_METADATA from connectors not supporting it [DBZ-3132](https://issues.jboss.org/browse/DBZ-3132)
* Add example for Debezium UI to debezium-examples repo [DBZ-3134](https://issues.jboss.org/browse/DBZ-3134)
* Clarify required privileges for using pgoutput [DBZ-3138](https://issues.jboss.org/browse/DBZ-3138)
* Do not rely on Max SCN seed value w/LogMiner [DBZ-3145](https://issues.jboss.org/browse/DBZ-3145)
* Postgres documentation improvements [DBZ-3149](https://issues.jboss.org/browse/DBZ-3149)
* Support running Oracle test suite in non-CDB (no PDB name) mode [DBZ-3154](https://issues.jboss.org/browse/DBZ-3154)
* Update Oracle documentation [DBZ-3156](https://issues.jboss.org/browse/DBZ-3156)
* Move the Oracle connector to the main repostory [DBZ-3166](https://issues.jboss.org/browse/DBZ-3166)
* Minor editorial update to PostgreSQL connector documentation [DBZ-3192](https://issues.jboss.org/browse/DBZ-3192)
* Incorrect link/anchor pair for truncate.handling.mode property in PG properties documentation [DBZ-3195](https://issues.jboss.org/browse/DBZ-3195)
* Update oracle-vagrant-box [DBZ-3206](https://issues.jboss.org/browse/DBZ-3206)
* Update Oracle versions tested [DBZ-3215](https://issues.jboss.org/browse/DBZ-3215)
* Oracle test suite does not always clean-up tables after tests [DBZ-3237](https://issues.jboss.org/browse/DBZ-3237)
* Update Oracle tutorial example [DBZ-3239](https://issues.jboss.org/browse/DBZ-3239)
* Avoid reference to upstream Docker set-up [DBZ-3259](https://issues.jboss.org/browse/DBZ-3259)



## 1.5.0.Beta1
February 23rd 2021 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12353830)

### New features since 1.5.0.Alpha1

* Make field descriptions consistent for time values (milliseconds, ms, sec, seconds, etc) [DBZ-2858](https://issues.jboss.org/browse/DBZ-2858)
* DebeziumEngine RecordChangeEvents cannot be modified [DBZ-2897](https://issues.jboss.org/browse/DBZ-2897)
* Add license headers and related checkstyle checks for Debezium UI files [DBZ-2985](https://issues.jboss.org/browse/DBZ-2985)
* Display commit SHA of UI frontend/backend somewhere in the footer [DBZ-3052](https://issues.jboss.org/browse/DBZ-3052)
* Implement UX suggestions for display of connector type [DBZ-3054](https://issues.jboss.org/browse/DBZ-3054)
* SqlServerConnector does not implement validate [DBZ-3056](https://issues.jboss.org/browse/DBZ-3056)
* Database History Producer does not close with a timeout [DBZ-3075](https://issues.jboss.org/browse/DBZ-3075)
* Improve DML parser performance [DBZ-3078](https://issues.jboss.org/browse/DBZ-3078)
* Connector list table UI improvement desktop/mobile [DBZ-3079](https://issues.jboss.org/browse/DBZ-3079)
* Vitess Connector adds support for Vitess 9.0.0 GA [DBZ-3100](https://issues.jboss.org/browse/DBZ-3100)
* Improve layout for Column Truncate - Mask Component [DBZ-3101](https://issues.jboss.org/browse/DBZ-3101)
* Improve layout for Data options component and main wizard nav [DBZ-3105](https://issues.jboss.org/browse/DBZ-3105)
* Add ability to skip tests based on available database options [DBZ-3110](https://issues.jboss.org/browse/DBZ-3110)
* Support for Transaction Metadata in MySql connector [DBZ-3114](https://issues.jboss.org/browse/DBZ-3114)
* Add support for JSON column type [DBZ-3115](https://issues.jboss.org/browse/DBZ-3115)
* Add support for ENUM column type [DBZ-3124](https://issues.jboss.org/browse/DBZ-3124)
* Enable easy downloading of Camel Kafka Connectors [DBZ-3136](https://issues.jboss.org/browse/DBZ-3136)
* Capture LogMiner session parameters when session fails to start [DBZ-3153](https://issues.jboss.org/browse/DBZ-3153)
* Process special values in temporal datatypes [DBZ-2614](https://issues.jboss.org/browse/DBZ-2614)


### Breaking changes since 1.5.0.Alpha1

* Document JSON column parsing regression for MySQL connector [DBZ-3130](https://issues.jboss.org/browse/DBZ-3130)
* Replace MySQL connector option with SMT for mitigating wrong op flag [DBZ-2788](https://issues.jboss.org/browse/DBZ-2788)
* Avoid dependency to JAXB classes [DBZ-3165](https://issues.jboss.org/browse/DBZ-3165)
* Remove build deprecation warnings [DBZ-3034](https://issues.jboss.org/browse/DBZ-3034)


### Fixes since 1.5.0.Alpha1

* Negative timestamps are converted to positive during snapshot [DBZ-2616](https://issues.jboss.org/browse/DBZ-2616)
* Wrong reference to KafkaConnector in setting up Debezium [DBZ-2745](https://issues.jboss.org/browse/DBZ-2745)
* Oracle Connector(Using Logminer) with Oracle RDS (v12) does not capture changes [DBZ-2754](https://issues.jboss.org/browse/DBZ-2754)
* Oracle connector causes ORA-65090 when connecting to an Oracle instance running in non-CDB mode [DBZ-2795](https://issues.jboss.org/browse/DBZ-2795)
* Warnings and notifications from PostgreSQL are ignored by the connector until the connection is closed [DBZ-2865](https://issues.jboss.org/browse/DBZ-2865)
* Add support for MySQL to UI Backend  [DBZ-2950](https://issues.jboss.org/browse/DBZ-2950)
* ExtractNewRecord SMT incorrectly extracts ts_ms from source info [DBZ-2984](https://issues.jboss.org/browse/DBZ-2984)
* Replication terminates with ORA-01291: missing log file [DBZ-3001](https://issues.jboss.org/browse/DBZ-3001)
* Kafka Docker image the HEAP_OPTS variable is not used [DBZ-3006](https://issues.jboss.org/browse/DBZ-3006)
* Support multiple schemas with Oracle LogMiner [DBZ-3009](https://issues.jboss.org/browse/DBZ-3009)
* Function calls does not allow parentheses for functions with non-mandatory parentheses [DBZ-3017](https://issues.jboss.org/browse/DBZ-3017)
* Complete support for properties that contain hyphens [DBZ-3019](https://issues.jboss.org/browse/DBZ-3019)
* UI issues with connectors table row expansion state [DBZ-3049](https://issues.jboss.org/browse/DBZ-3049)
* SQLException for Global temp tables  from OracleDatabaseMetaData.getIndexInfo() makes Debezium snapshotting fail [DBZ-3057](https://issues.jboss.org/browse/DBZ-3057)
* Cassandra Connector doesn't support Cassandra version >=3.11.5 [DBZ-3060](https://issues.jboss.org/browse/DBZ-3060)
* Make Cassandra Connector work with CommitLogTransfer better [DBZ-3063](https://issues.jboss.org/browse/DBZ-3063)
* no viable alternative at input 'create or replace index' [DBZ-3067](https://issues.jboss.org/browse/DBZ-3067)
* Connect image propagates  env vars starting with CONNECT prefix [DBZ-3070](https://issues.jboss.org/browse/DBZ-3070)
* PgOutputMessageDecoder doesn't order primary keys [DBZ-3074](https://issues.jboss.org/browse/DBZ-3074)
* Strange transaction metadata for Oracle logminer connector [DBZ-3090](https://issues.jboss.org/browse/DBZ-3090)
* Getting RejectedExecutionException when checking topic settings from KafkaDatabaseHistory.checkTopicSettings [DBZ-3096](https://issues.jboss.org/browse/DBZ-3096)
* Environment Variables with spaces are truncated when written to properties file [DBZ-3103](https://issues.jboss.org/browse/DBZ-3103)
* Error: Supplemental logging not configured for table. Use command: ALTER TABLE  [DBZ-3109](https://issues.jboss.org/browse/DBZ-3109)
* Uncaught (in promise) TypeError: Cannot read property 'call' of undefined [DBZ-3125](https://issues.jboss.org/browse/DBZ-3125)
* Final stage of snapshot analyzes tables not present in table.include.list thus stumbles upon unsupported XMLTYPE table [DBZ-3151](https://issues.jboss.org/browse/DBZ-3151)
* Missing Prometheus port in kafka network policy  [DBZ-3170](https://issues.jboss.org/browse/DBZ-3170)
* XStream does not process NUMER(1) data [DBZ-3172](https://issues.jboss.org/browse/DBZ-3172)


### Other changes since 1.5.0.Alpha1

* Setup CI job for DB2  [DBZ-2235](https://issues.jboss.org/browse/DBZ-2235)
* Integration with Service Registry promoted to GA [DBZ-2815](https://issues.jboss.org/browse/DBZ-2815)
* Remove DECIMAL string sanitisation once Vitess upstream bug is fixed [DBZ-2908](https://issues.jboss.org/browse/DBZ-2908)
* Review format and configuration options for Db2 for GA [DBZ-2977](https://issues.jboss.org/browse/DBZ-2977)
* Test with Postgres 13 [DBZ-3022](https://issues.jboss.org/browse/DBZ-3022)
* Prepare Debezium UI to participate in upstream releases [DBZ-3027](https://issues.jboss.org/browse/DBZ-3027)
* Upgrade testcontainers to 1.15.1  [DBZ-3066](https://issues.jboss.org/browse/DBZ-3066)
* Use new deployment endpoint for releases to Maven Central [DBZ-3069](https://issues.jboss.org/browse/DBZ-3069)
* Remove obsolete Awestruct container image [DBZ-3072](https://issues.jboss.org/browse/DBZ-3072)
* "JDBC driver" doesn't make sense for non-relational connectors [DBZ-3076](https://issues.jboss.org/browse/DBZ-3076)
* Replace RecordMakers with MySqlChangeRecordEmitter [DBZ-3077](https://issues.jboss.org/browse/DBZ-3077)
* Make CI builds resilient against disconnects on GH Actions infrastructure [DBZ-3083](https://issues.jboss.org/browse/DBZ-3083)
* Separate SourceInfo and MySQL offset context [DBZ-3086](https://issues.jboss.org/browse/DBZ-3086)
* Remove zero-width whitespace from option names [DBZ-3087](https://issues.jboss.org/browse/DBZ-3087)
* Adapt UI for MySQL connector type [DBZ-3091](https://issues.jboss.org/browse/DBZ-3091)
* Change MySQL database schema contract to support separate parsing and processing phase [DBZ-3093](https://issues.jboss.org/browse/DBZ-3093)
* MySQL build stuck for 6h [DBZ-3095](https://issues.jboss.org/browse/DBZ-3095)
* Rewrite legacy reader tests [DBZ-3099](https://issues.jboss.org/browse/DBZ-3099)
* Intermittent test failure in Postgres PostgresConnectorIT#customSnapshotterSkipsTablesOnRestart [DBZ-3107](https://issues.jboss.org/browse/DBZ-3107)
* Remove duplicate anchor links in Connector properties [DBZ-3111](https://issues.jboss.org/browse/DBZ-3111)
* Upgrade to Quarkus 1.12.0.Final [DBZ-3116](https://issues.jboss.org/browse/DBZ-3116)
* Config validation for Vitess [DBZ-3117](https://issues.jboss.org/browse/DBZ-3117)
* Config validation for Oracle [DBZ-3119](https://issues.jboss.org/browse/DBZ-3119)
* Avoid naming conflict between connection classes [DBZ-3147](https://issues.jboss.org/browse/DBZ-3147)
* Set up commit message check for Vitess [DBZ-3152](https://issues.jboss.org/browse/DBZ-3152)
* Put IIDR license requirement into NOTE box [DBZ-3163](https://issues.jboss.org/browse/DBZ-3163)
* Consistent logging of connection validation failure [DBZ-3164](https://issues.jboss.org/browse/DBZ-3164)
* Remove COLUMN_BLACK_LIST option in Oracle connector [DBZ-3167](https://issues.jboss.org/browse/DBZ-3167)



## 1.5.0.Alpha1
February 4th 2021 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12351487)

### New features since 1.4.1.Final

* Support emitting TRUNCATE events in PostgreSQL pgoutput plugin [DBZ-2382](https://issues.jboss.org/browse/DBZ-2382)
* Migrate DebeziumContainer enhancements for DBZ-2950 and DBZ-2952 into main repository [DBZ-3024](https://issues.jboss.org/browse/DBZ-3024)
* Implement meta tags [DBZ-2620](https://issues.jboss.org/browse/DBZ-2620)
* Improve performance for very large postgres schemas [DBZ-2575](https://issues.jboss.org/browse/DBZ-2575)


### Breaking changes since 1.4.1.Final

* Move MySQL connector to base framework [DBZ-1865](https://issues.jboss.org/browse/DBZ-1865)


### Fixes since 1.4.1.Final

* Extra connectors are not buildable unless main Debezium is built locally [DBZ-2901](https://issues.jboss.org/browse/DBZ-2901)
* java.sql.SQLException: ORA-01333: failed to establish Logminer Dictionary [DBZ-2939](https://issues.jboss.org/browse/DBZ-2939)
* Add support for connector/task lifecycle ops to UI backend [DBZ-2951](https://issues.jboss.org/browse/DBZ-2951)
* Cassandra CDC failed to deserialize list<UserType> column correct [DBZ-2974](https://issues.jboss.org/browse/DBZ-2974)
* Debezium Oracle Connector will appear stuck on large SCN jumps [DBZ-2982](https://issues.jboss.org/browse/DBZ-2982)
* Invalid regex patterns should fail validation when validation database.include/exclude.list properties for MySQL connector [DBZ-3008](https://issues.jboss.org/browse/DBZ-3008)
* Fix repository config for Jenkis snapshot deployment [DBZ-3011](https://issues.jboss.org/browse/DBZ-3011)
* Unable to parse non-constant SIGNAL option value [DBZ-3018](https://issues.jboss.org/browse/DBZ-3018)
* Cannot parse expression in DEFAULT column definition [DBZ-3020](https://issues.jboss.org/browse/DBZ-3020)
* Key being used as value in pubsub batch handler [DBZ-3037](https://issues.jboss.org/browse/DBZ-3037)
* Table creation DDL with `CHARACTER SET = DEFAULT` causes MySQL connector failure [DBZ-3023](https://issues.jboss.org/browse/DBZ-3023)
* Missing some MariaDB existence predicates in ALTER TABLE [DBZ-3039](https://issues.jboss.org/browse/DBZ-3039)


### Other changes since 1.4.1.Final

* Improved resiliency of release process against OSS failures [DBZ-2274](https://issues.jboss.org/browse/DBZ-2274)
* Pull up HOSTNAME, PORT, DATABASE_NAME, USER and PASSWORD to RelationalDatabaseConnectorConfig [DBZ-2420](https://issues.jboss.org/browse/DBZ-2420)
* Db2 Connector doesn't declare database related config options [DBZ-2424](https://issues.jboss.org/browse/DBZ-2424)
* Fix build status badge in README files [DBZ-2802](https://issues.jboss.org/browse/DBZ-2802)
* Merge and complete web components PR [DBZ-2804](https://issues.jboss.org/browse/DBZ-2804)
* IBM Db2 Connector promoted to GA [DBZ-2814](https://issues.jboss.org/browse/DBZ-2814)
* Document several Oracle frequently encountered problems [DBZ-2970](https://issues.jboss.org/browse/DBZ-2970)
* No syntax highlighting on website listings [DBZ-2978](https://issues.jboss.org/browse/DBZ-2978)
* Admonition icons missing [DBZ-2986](https://issues.jboss.org/browse/DBZ-2986)
* Improve logging for Logminer adapter [DBZ-2999](https://issues.jboss.org/browse/DBZ-2999)
* CI build not required for changes in README files [DBZ-3012](https://issues.jboss.org/browse/DBZ-3012)
* Execute ZZZGtidSetIT as the last test [DBZ-3047](https://issues.jboss.org/browse/DBZ-3047)
* Capture and report LogMiner state when mining session fails to start [DBZ-3055](https://issues.jboss.org/browse/DBZ-3055)



## 1.4.1.Final
January 28th 2021 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12353181)

### New features since 1.4.0.Final

* Clarify information in Debezium connector for SQL Server doc [DBZ-2675](https://issues.jboss.org/browse/DBZ-2675)
* Add support for binary.handling.mode to the SQL Server connector [DBZ-2912](https://issues.jboss.org/browse/DBZ-2912)
* Use collation to get charset when charset is not set [DBZ-2922](https://issues.jboss.org/browse/DBZ-2922)
* Additional logging for number and type of sql operations [DBZ-2980](https://issues.jboss.org/browse/DBZ-2980)
* Retry on "The server failed to resume the transaction" [DBZ-2959](https://issues.jboss.org/browse/DBZ-2959)




### Breaking changes since 1.4.0.Final

None


### Fixes since 1.4.0.Final

* Debezium Connectors are failing while reading binlog: Unknown event type 100 [DBZ-2499](https://issues.jboss.org/browse/DBZ-2499)
* Some column default values are not extracted correctly while reading table structure [DBZ-2698](https://issues.jboss.org/browse/DBZ-2698)
* Supplemental logging is required for entire database rather than per monitored table [DBZ-2711](https://issues.jboss.org/browse/DBZ-2711)
* Missing log file error when current SCN differs from snapshotted in Oracle connector and Logminer [DBZ-2855](https://issues.jboss.org/browse/DBZ-2855)
* GitHub action for "Build Testing Workflow" is using old artifacts and not building missing dependencies [DBZ-2861](https://issues.jboss.org/browse/DBZ-2861)
* Deadlock in the XStream handler and offset commiter call concurrently [DBZ-2891](https://issues.jboss.org/browse/DBZ-2891)
* Sanitise DECIMAL string from VStream [DBZ-2906](https://issues.jboss.org/browse/DBZ-2906)
* Vitess Connector download link missing on website [DBZ-2907](https://issues.jboss.org/browse/DBZ-2907)
* DML statements longer than 4000 characters are incorrectly combined from V$LOGMNR_CONTENTS [DBZ-2920](https://issues.jboss.org/browse/DBZ-2920)
* Default database charset is not recorded [DBZ-2921](https://issues.jboss.org/browse/DBZ-2921)
* Instable test: PostgresConnectorIT#testCustomSnapshotterSnapshotCompleteLifecycleHook() [DBZ-2938](https://issues.jboss.org/browse/DBZ-2938)
* Snapshot causes ORA-08181 exception [DBZ-2949](https://issues.jboss.org/browse/DBZ-2949)
* Postgres connector config validation fails because current connector is occupying replication slot [DBZ-2952](https://issues.jboss.org/browse/DBZ-2952)
* Labeled create procedure's body is not parsed [DBZ-2972](https://issues.jboss.org/browse/DBZ-2972)
* Debezium swallows DML exception in certain cases [DBZ-2981](https://issues.jboss.org/browse/DBZ-2981)


### Other changes since 1.4.0.Final

* Migrate website build to Hugo [DBZ-575](https://issues.jboss.org/browse/DBZ-575)
* Test binary/varbinary datatypes [DBZ-2174](https://issues.jboss.org/browse/DBZ-2174)
* Implement Scn as a domain type [DBZ-2518](https://issues.jboss.org/browse/DBZ-2518)
* Fix docs for message.key.columns and skipped.operations [DBZ-2572](https://issues.jboss.org/browse/DBZ-2572)
* Upgrade to Apache Kafka Connect 2.6.1 [DBZ-2630](https://issues.jboss.org/browse/DBZ-2630)
* Centralize postgres image name for test container tests [DBZ-2764](https://issues.jboss.org/browse/DBZ-2764)
* Add missing connector options for Postgres connector [DBZ-2807](https://issues.jboss.org/browse/DBZ-2807)
* Importing TestDatabase as QuarkusTestResource for IT tests [DBZ-2868](https://issues.jboss.org/browse/DBZ-2868)
* Set up Pulsar via Testcontainers in PulsarIT [DBZ-2915](https://issues.jboss.org/browse/DBZ-2915)
* Remove blacklist and whitelist from anchor link text in documentation [DBZ-2918](https://issues.jboss.org/browse/DBZ-2918)
* Instable test: PostgresShutdownIT#shouldStopOnPostgresFastShutdown() [DBZ-2923](https://issues.jboss.org/browse/DBZ-2923)
* Rename whitelist/blacklist configs in examples to include/exclude [DBZ-2925](https://issues.jboss.org/browse/DBZ-2925)
* Misspelling in readme for db2 connector [DBZ-2940](https://issues.jboss.org/browse/DBZ-2940)
* Fetch correct Apicurio version for ApicurioRegistryTest [DBZ-2945](https://issues.jboss.org/browse/DBZ-2945)
* Incorrect link IDs in SQL Server connector snapshot metrics table [DBZ-2958](https://issues.jboss.org/browse/DBZ-2958)



## 1.4.0.Final
January 7th 2021 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12352766)

### New features since 1.4.0.CR1

* Improve error reporting from DDL parser [DBZ-2366](https://issues.jboss.org/browse/DBZ-2366)
* Support TNS Names and full RAC connection strings [DBZ-2859](https://issues.jboss.org/browse/DBZ-2859)
* Add more comprehensible logs to FIELD event [DBZ-2873](https://issues.jboss.org/browse/DBZ-2873)


### Breaking changes since 1.4.0.CR1

None


### Fixes since 1.4.0.CR1

* AWS RDS has different role names which make connector validation fail [DBZ-2800](https://issues.jboss.org/browse/DBZ-2800)
* Archive Log mining does not work with Logminer [DBZ-2825](https://issues.jboss.org/browse/DBZ-2825)
* MySQL parser error for comments starting with tab [DBZ-2840](https://issues.jboss.org/browse/DBZ-2840)
* Connector fails when using '$' sign in column name. [DBZ-2849](https://issues.jboss.org/browse/DBZ-2849)
* Connection adapter not passed to Surefire tests [DBZ-2856](https://issues.jboss.org/browse/DBZ-2856)
* Unsupported MariaDB syntax for generated columns [DBZ-2882](https://issues.jboss.org/browse/DBZ-2882)
* SLF4J API should not be included in Oracle distirbution [DBZ-2890](https://issues.jboss.org/browse/DBZ-2890)
* Vitess distro contains unaligned deps [DBZ-2892](https://issues.jboss.org/browse/DBZ-2892)
* Changing base packages does not always trigger full builds [DBZ-2896](https://issues.jboss.org/browse/DBZ-2896)
* LogMiner causes DataException when DATE field is specified as NOT NULL [DBZ-2784](https://issues.jboss.org/browse/DBZ-2784)


### Other changes since 1.4.0.CR1

* Remove LegacyDdlParser and related code [DBZ-2167](https://issues.jboss.org/browse/DBZ-2167)
* Add MongoDB connector interface [DBZ-2808](https://issues.jboss.org/browse/DBZ-2808)
* `sanitize.field.names` support for Vitess Connector [DBZ-2851](https://issues.jboss.org/browse/DBZ-2851)
* Explicitly declare to Quarkus that ORM XML mapping is required for the outbox extension [DBZ-2860](https://issues.jboss.org/browse/DBZ-2860)
* Upgrade MySQL JDBC driver to 8.0.21 [DBZ-2887](https://issues.jboss.org/browse/DBZ-2887)
* Upgrade Guava library to 30.0 [DBZ-2888](https://issues.jboss.org/browse/DBZ-2888)
* Avoid exception when payload id field not present [DBZ-2889](https://issues.jboss.org/browse/DBZ-2889)



## 1.4.0.CR1
December 16th 2020 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12352696)

### New features since 1.4.0.Beta1

* Documentation of the Logminer implementation needs improvement [DBZ-2799](https://issues.jboss.org/browse/DBZ-2799)
* Update Vitess Connector documentation [DBZ-2854](https://issues.jboss.org/browse/DBZ-2854)
* Add Cassandra to tutorial Compose set-up [DBZ-1463](https://issues.jboss.org/browse/DBZ-1463)
* Add support for Vitess gRPC static authentication [DBZ-2852](https://issues.jboss.org/browse/DBZ-2852)


### Breaking changes since 1.4.0.Beta1

None


### Fixes since 1.4.0.Beta1

* Document "database.oracle.version" option [DBZ-2603](https://issues.jboss.org/browse/DBZ-2603)
* Remove link in MySQL docs section that points to the same section [DBZ-2710](https://issues.jboss.org/browse/DBZ-2710)
* Oracle schema history events fail on partitioned table [DBZ-2841](https://issues.jboss.org/browse/DBZ-2841)
* outbox extension emits UPDATE events when delete is disabled [DBZ-2847](https://issues.jboss.org/browse/DBZ-2847)


### Other changes since 1.4.0.Beta1

* Move Cassandra connector to separate repository [DBZ-2636](https://issues.jboss.org/browse/DBZ-2636)
* Invalid column name should fail connector with meaningful message [DBZ-2836](https://issues.jboss.org/browse/DBZ-2836)
* Fix typos in downstream ModuleID declarations in monitoring.adoc [DBZ-2838](https://issues.jboss.org/browse/DBZ-2838)
* Duplicate anchor ID in partials/ref-connector-monitoring-snapshot-metrics.adoc [DBZ-2839](https://issues.jboss.org/browse/DBZ-2839)
* Fix additional typo in ModuleID declaration in monitoring.adoc [DBZ-2843](https://issues.jboss.org/browse/DBZ-2843)
* Edit modularization annotations in logging.adoc [DBZ-2846](https://issues.jboss.org/browse/DBZ-2846)
* Update Groovy version to 3.0.7 [DBZ-2850](https://issues.jboss.org/browse/DBZ-2850)



## 1.4.0.Beta1
December 9th 2020 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12352306)

### New features since 1.4.0.Alpha2

* Add support for distributed tracing [DBZ-559](https://issues.jboss.org/browse/DBZ-559)
* Outbox Quarkus extension: Support OpenTracing [DBZ-1818](https://issues.jboss.org/browse/DBZ-1818)
* Upgrade MongoDB driver to 4.x to run in native mode in GraalVM (for Quarkus extension) [DBZ-2138](https://issues.jboss.org/browse/DBZ-2138)
* Allow snapshot records be generated either as create or read for MySQL connector [DBZ-2775](https://issues.jboss.org/browse/DBZ-2775)
* Support in Db2 connector for lowercase table and schema names [DBZ-2796](https://issues.jboss.org/browse/DBZ-2796)
* option to kill process when engine run crashes [DBZ-2785](https://issues.jboss.org/browse/DBZ-2785)
* Add support for using Vitess primary key as Kafka message key [DBZ-2578](https://issues.jboss.org/browse/DBZ-2578)
* Add support for Nullable columns [DBZ-2579](https://issues.jboss.org/browse/DBZ-2579)
* Tablespace name LOGMINER_TBS should not be hardcoded in the Java code [DBZ-2797](https://issues.jboss.org/browse/DBZ-2797)


### Breaking changes since 1.4.0.Alpha2

None


### Fixes since 1.4.0.Alpha2

* DDL parser: Allow stored procedure variables in LIMIT clause [DBZ-2692](https://issues.jboss.org/browse/DBZ-2692)
* Wrong mysql command in openshift dpeloyment docs [DBZ-2746](https://issues.jboss.org/browse/DBZ-2746)
* long running transaction will be abandoned and ignored [DBZ-2759](https://issues.jboss.org/browse/DBZ-2759)
* MS SQL Decimal with default value not matching the scale of the column definition cause exception [DBZ-2767](https://issues.jboss.org/browse/DBZ-2767)
* Cassandra Connector doesn't shut down completely [DBZ-2768](https://issues.jboss.org/browse/DBZ-2768)
* MySQL Parser fails for BINARY collation shortcut [DBZ-2771](https://issues.jboss.org/browse/DBZ-2771)
* PostgresConnectorIT.shouldResumeStreamingFromSlotPositionForCustomSnapshot is failing for wal2json on CI [DBZ-2772](https://issues.jboss.org/browse/DBZ-2772)
* Connector configuration property "database.out.server.name" is not relevant for Logminer implementation but cannot be omitted [DBZ-2801](https://issues.jboss.org/browse/DBZ-2801)
* CHARACTER VARYING mysql identifier for varchar is not supported in debezium [DBZ-2821](https://issues.jboss.org/browse/DBZ-2821)
* try-with-resources should not be used when OkHttp Response object is returned [DBZ-2827](https://issues.jboss.org/browse/DBZ-2827)
* EmbeddedEngine does not shutdown when commitOffsets is interrupted [DBZ-2830](https://issues.jboss.org/browse/DBZ-2830)
* Rename user command parsing fails [DBZ-2743](https://issues.jboss.org/browse/DBZ-2743)


### Other changes since 1.4.0.Alpha2

* Fix splitter annotations that control how content is modularized downstream [DBZ-2824](https://issues.jboss.org/browse/DBZ-2824)
* VerifyRecord#isValid() compares JSON schema twice instead of Avro [DBZ-735](https://issues.jboss.org/browse/DBZ-735)
* Don't rely on deprecated JSON serialization functionality of MongoDB driver [DBZ-1322](https://issues.jboss.org/browse/DBZ-1322)
* Move website build to GitHub Actions [DBZ-1984](https://issues.jboss.org/browse/DBZ-1984)
* Move Db2 connector to separate repository [DBZ-2001](https://issues.jboss.org/browse/DBZ-2001)
* Modularize doc for SQL Server component [DBZ-2335](https://issues.jboss.org/browse/DBZ-2335)
* Upgrade apicurio to 1.3.2.Final [DBZ-2561](https://issues.jboss.org/browse/DBZ-2561)
* Remove obsolete logging files from /partials directory [DBZ-2740](https://issues.jboss.org/browse/DBZ-2740)
* Remove obsolete monitoring files from /partials directory [DBZ-2741](https://issues.jboss.org/browse/DBZ-2741)
* Increase Oracle CI frequency [DBZ-2744](https://issues.jboss.org/browse/DBZ-2744)
* Make Debezium example work with Podman instead of Docker [DBZ-2753](https://issues.jboss.org/browse/DBZ-2753)
* Disable log mining history by default [DBZ-2763](https://issues.jboss.org/browse/DBZ-2763)
* Upgrade -setup-java action to the latest 1.4.3 [DBZ-2770](https://issues.jboss.org/browse/DBZ-2770)
* Trigger non-core connector tests when core or DDL parser module are changed [DBZ-2773](https://issues.jboss.org/browse/DBZ-2773)
* Add support for unsigned integer types [DBZ-2776](https://issues.jboss.org/browse/DBZ-2776)
* Update JDK action workflow matrix with JDK 16.0.0-ea.24 [DBZ-2777](https://issues.jboss.org/browse/DBZ-2777)
* Auto resolve latest JDK EA release number  [DBZ-2781](https://issues.jboss.org/browse/DBZ-2781)
* Update content in modularized SQL Server connector doc [DBZ-2782](https://issues.jboss.org/browse/DBZ-2782)



## 1.4.0.Alpha2
November 16th 2020 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12351542)

### New features since 1.4.0.Alpha1

* Move testcontainers changes on DebeziumContainer from UI PoC backend to Debezium main repo [DBZ-2602](https://issues.jboss.org/browse/DBZ-2602)
* Add ability to map new name for the fields and headers [DBZ-2606](https://issues.jboss.org/browse/DBZ-2606)
* Add close call to the Snapshotter interface [DBZ-2608](https://issues.jboss.org/browse/DBZ-2608)
* Overriding Character Set Mapping [DBZ-2673](https://issues.jboss.org/browse/DBZ-2673)
* Support PostgreSQL connector retry when database is restarted [DBZ-2685](https://issues.jboss.org/browse/DBZ-2685)
* Cassandra connector documentation typos [DBZ-2701](https://issues.jboss.org/browse/DBZ-2701)
* Fix typo in converters doc [DBZ-2717](https://issues.jboss.org/browse/DBZ-2717)
* Add tests for DBZ-2617: PG connector does not enter FAILED state on failing heartbeats [DBZ-2724](https://issues.jboss.org/browse/DBZ-2724)
* DBZ-2662 Control ChangeEventQueue by the size in bytes [DBZ-2662](https://issues.jboss.org/browse/DBZ-2662)


### Breaking changes since 1.4.0.Alpha1

None


### Fixes since 1.4.0.Alpha1

* Oracle throw "no snapshot found based on specified time" when running flashback query [DBZ-1446](https://issues.jboss.org/browse/DBZ-1446)
* Exception when PK definition precedes column definition [DBZ-2580](https://issues.jboss.org/browse/DBZ-2580)
* Patroni can't stop PostgreSQL when Debezium is streaming [DBZ-2617](https://issues.jboss.org/browse/DBZ-2617)
* ChangeRecord informations don't connect with the TableSchema [DBZ-2679](https://issues.jboss.org/browse/DBZ-2679)
* MySQL connector fails on a zero date [DBZ-2682](https://issues.jboss.org/browse/DBZ-2682)
* Oracle LogMiner doesn't support partition tables [DBZ-2683](https://issues.jboss.org/browse/DBZ-2683)
* DB2 doesn't start reliably in OCP  [DBZ-2693](https://issues.jboss.org/browse/DBZ-2693)
* Dropped columns cause NPE in SqlServerConnector [DBZ-2716](https://issues.jboss.org/browse/DBZ-2716)
* Timestamp default value in 'yyyy-mm-dd' format fails MySQL connector [DBZ-2726](https://issues.jboss.org/browse/DBZ-2726)
* Connection timeout on write should retry [DBZ-2727](https://issues.jboss.org/browse/DBZ-2727)
* No viable alternative at input error on "min" column [DBZ-2738](https://issues.jboss.org/browse/DBZ-2738)
* SQLServer CI error in SqlServerConnectorIT.whenCaptureInstanceExcludesColumnsAndColumnsRenamedExpectNoErrors:1473 [DBZ-2747](https://issues.jboss.org/browse/DBZ-2747)
* debezium-connector-db2: DB2 SQL Error: SQLCODE=-206 on DB2 for z/OS [DBZ-2755](https://issues.jboss.org/browse/DBZ-2755)
* no viable alternative at input 'alter table `order` drop CONSTRAINT' [DBZ-2760](https://issues.jboss.org/browse/DBZ-2760)
* Tests are failing on macos [DBZ-2762](https://issues.jboss.org/browse/DBZ-2762)


### Other changes since 1.4.0.Alpha1

* Move CI to Github Actions for all repositories [DBZ-1720](https://issues.jboss.org/browse/DBZ-1720)
* Privileges missing from setup in documentation - Oracle LogMiner connector [DBZ-2628](https://issues.jboss.org/browse/DBZ-2628)
* Add validation that replication slot doesn't exist [DBZ-2637](https://issues.jboss.org/browse/DBZ-2637)
* Update OpenJDK Quality Outreach jobs [DBZ-2638](https://issues.jboss.org/browse/DBZ-2638)
* Re-unify monitoring content in the operations/monitoring.adoc file [DBZ-2659](https://issues.jboss.org/browse/DBZ-2659)
* Pull oracle specific changes for reading table column metadata into debezium-core [DBZ-2690](https://issues.jboss.org/browse/DBZ-2690)
* Intermittent test failure on CI - PostgresConnectorIT#shouldRegularlyFlushLsnWithTxMonitoring [DBZ-2704](https://issues.jboss.org/browse/DBZ-2704)
* Topic routing doc formatting fix [DBZ-2708](https://issues.jboss.org/browse/DBZ-2708)
* Re-unify logging content in the operations/logging.adoc file [DBZ-2721](https://issues.jboss.org/browse/DBZ-2721)
* Incorporate Oracle LogMiner implementation updates [DBZ-2729](https://issues.jboss.org/browse/DBZ-2729)
* Upgrade Vitess docker image to Vitess 8.0.0 [DBZ-2749](https://issues.jboss.org/browse/DBZ-2749)
* Intermittent SQL Server test failure on CI - SqlServerConnectorIT [DBZ-2625](https://issues.jboss.org/browse/DBZ-2625)
* Change initial.sync.max.threads to snapshot.max.threads [DBZ-2742](https://issues.jboss.org/browse/DBZ-2742)



## 1.4.0.Alpha1
October 22nd, 2020 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12350728)

### New features since 1.3.0.Final

* Allow to specify subset of captured tables to be snapshotted [DBZ-2456](https://issues.jboss.org/browse/DBZ-2456)
* Implement snapshot select override behavior for MongoDB [DBZ-2496](https://issues.jboss.org/browse/DBZ-2496)
* Asciidoc block titles are rendered the same as regular text [DBZ-2631](https://issues.jboss.org/browse/DBZ-2631)
* Allow closing of hung JDBC connection [DBZ-2632](https://issues.jboss.org/browse/DBZ-2632)
* Hide stacktrace when default value for SQL Server cannot be parsed [DBZ-2642](https://issues.jboss.org/browse/DBZ-2642)
* Implement a CDC connector for Vitess [DBZ-2463](https://issues.jboss.org/browse/DBZ-2463)
* SqlServer - Skip processing of LSNs not associated with change table entries. [DBZ-2582](https://issues.jboss.org/browse/DBZ-2582)


### Breaking changes since 1.3.0.Final

None


### Fixes since 1.3.0.Final

* Cant override environment variables [DBZ-2559](https://issues.jboss.org/browse/DBZ-2559)
* Inconsistencies in PostgreSQL Connector Docs [DBZ-2584](https://issues.jboss.org/browse/DBZ-2584)
* ConcurrentModificationException during exporting data for a mongodb collection in a sharded cluster [DBZ-2597](https://issues.jboss.org/browse/DBZ-2597)
* Mysql connector didn't pass the default db charset to the column definition [DBZ-2604](https://issues.jboss.org/browse/DBZ-2604)
* [Doc] "registry.redhat.io/amq7/amq-streams-kafka-25: unknown: Not Found" error occurs [DBZ-2609](https://issues.jboss.org/browse/DBZ-2609)
* [Doc] "Error: no context directory and no Containerfile specified" error occurs [DBZ-2610](https://issues.jboss.org/browse/DBZ-2610)
* SqlExceptions using dbz with Oracle on RDS online logs and logminer [DBZ-2624](https://issues.jboss.org/browse/DBZ-2624)
* Mining session stopped - task killed/SQL operation cancelled - Oracle LogMiner [DBZ-2629](https://issues.jboss.org/browse/DBZ-2629)
* Unparseable DDL: Using 'trigger' as table alias in view creation [DBZ-2639](https://issues.jboss.org/browse/DBZ-2639)
* Antlr DDL parser fails to interpret BLOB([size]) [DBZ-2641](https://issues.jboss.org/browse/DBZ-2641)
* MySQL Connector keeps stale offset metadata after snapshot.new.tables is changed [DBZ-2643](https://issues.jboss.org/browse/DBZ-2643)
* WAL logs are not flushed in Postgres Connector [DBZ-2653](https://issues.jboss.org/browse/DBZ-2653)
* Debezium server Event Hubs plugin support in v1.3 [DBZ-2660](https://issues.jboss.org/browse/DBZ-2660)
* Cassandra Connector doesn't use log4j for logging correctly [DBZ-2661](https://issues.jboss.org/browse/DBZ-2661)
* Should Allow NonAsciiCharacter in SQL [DBZ-2670](https://issues.jboss.org/browse/DBZ-2670)
* MariaDB nextval function is not supported in grammar [DBZ-2671](https://issues.jboss.org/browse/DBZ-2671)
* Sanitize field name do not santize sub struct field [DBZ-2680](https://issues.jboss.org/browse/DBZ-2680)
* Debezium fails if a non-existing view with the same name as existing table is dropped [DBZ-2688](https://issues.jboss.org/browse/DBZ-2688)


### Other changes since 1.3.0.Final

* Merge MySQL doc source files into one again [DBZ-2127](https://issues.jboss.org/browse/DBZ-2127)
* Metrics links duplicate anchor IDs [DBZ-2497](https://issues.jboss.org/browse/DBZ-2497)
* Slim down Vitess container image [DBZ-2551](https://issues.jboss.org/browse/DBZ-2551)
* Modify release peipeline to support per-connector repos e.g. Vitess [DBZ-2611](https://issues.jboss.org/browse/DBZ-2611)
* Add Vitess connector to Kafka Connect container image [DBZ-2618](https://issues.jboss.org/browse/DBZ-2618)
* User Guide Documentation corrections for PostgreSQL  [DBZ-2621](https://issues.jboss.org/browse/DBZ-2621)
* Checkstyle should be built as a part of GH check formatting action [DBZ-2623](https://issues.jboss.org/browse/DBZ-2623)
* Upgrade MySQL JDBC driver to version 8.0.19 [DBZ-2626](https://issues.jboss.org/browse/DBZ-2626)
* Add support for multiple shard GTIDs in VGTID [DBZ-2635](https://issues.jboss.org/browse/DBZ-2635)
* Add documentation for Vitess connector [DBZ-2645](https://issues.jboss.org/browse/DBZ-2645)
* Restrict matrix job configurations to run only on Slaves [DBZ-2648](https://issues.jboss.org/browse/DBZ-2648)
* Upgrade JUnit to 4.13.1 [DBZ-2658](https://issues.jboss.org/browse/DBZ-2658)
* Avoid parsing generated files in Checkstyle [DBZ-2669](https://issues.jboss.org/browse/DBZ-2669)
* Update debezium/awestruct image to use Antora 2.3.4 [DBZ-2674](https://issues.jboss.org/browse/DBZ-2674)
* Fix doc typos and minor format glitches for downstream rendering [DBZ-2681](https://issues.jboss.org/browse/DBZ-2681)
* Intermittent test failure on CI - RecordsStreamProducerIT#shouldReceiveHeartbeatAlsoWhenChangingNonWhitelistedTable() [DBZ-2344](https://issues.jboss.org/browse/DBZ-2344)



## 1.3.0.Final
October 1st, 2020 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12350725)

### New features since 1.3.0.CR1

* Allow configurable CONNECT_LOG4J_LOGGERS in connect images [DBZ-2541](https://issues.jboss.org/browse/DBZ-2541)
* MySQL connector - ignore statement-based logs [DBZ-2583](https://issues.jboss.org/browse/DBZ-2583)
* Add a configuration which sanitizes values in mongodb [DBZ-2585](https://issues.jboss.org/browse/DBZ-2585)


### Breaking changes since 1.3.0.CR1

None


### Fixes since 1.3.0.CR1

* Sqlserver connector block cdc cleanup job [DBZ-1285](https://issues.jboss.org/browse/DBZ-1285)
* Upgrade Guava library due to GuavaCompatibility errors [DBZ-2008](https://issues.jboss.org/browse/DBZ-2008)
* mongodb-connector NPE in process of  MongoDataConverter  [DBZ-2316](https://issues.jboss.org/browse/DBZ-2316)
* Error with UUID-typed collection column [DBZ-2512](https://issues.jboss.org/browse/DBZ-2512)
* event.processing.failure.handling.mode doesn't skip unparseable data events [DBZ-2563](https://issues.jboss.org/browse/DBZ-2563)
* decoderbufs Segmentation fault on timestamp with infinity [DBZ-2565](https://issues.jboss.org/browse/DBZ-2565)
* MongoDB ExtractNewDocumentState can not extract array of array [DBZ-2569](https://issues.jboss.org/browse/DBZ-2569)
* New MySQL 8 ALTER USER password options not supported [DBZ-2576](https://issues.jboss.org/browse/DBZ-2576)
* MariaDB ANTLR parser issue for grant syntax [DBZ-2586](https://issues.jboss.org/browse/DBZ-2586)
* Debezium Db2 connector fails with tables using BOOLEAN type [DBZ-2587](https://issues.jboss.org/browse/DBZ-2587)
* db2 connector doesn't allow to reprocess messages [DBZ-2591](https://issues.jboss.org/browse/DBZ-2591)
* Missing links in filter and content-based SMT doc [DBZ-2593](https://issues.jboss.org/browse/DBZ-2593)
* Format error in doc for topic routing and event flattening SMTs [DBZ-2596](https://issues.jboss.org/browse/DBZ-2596)
* Debezium refers to database instead of schema in Postgres config [DBZ-2605](https://issues.jboss.org/browse/DBZ-2605)
* NullPointerException thrown when calling getAllTableIds [DBZ-2607](https://issues.jboss.org/browse/DBZ-2607)


### Other changes since 1.3.0.CR1

* Coordinate docs work for downstream 1.2 release [DBZ-2272](https://issues.jboss.org/browse/DBZ-2272)
* Gracefully handle server-side filtered columns [DBZ-2495](https://issues.jboss.org/browse/DBZ-2495)
* Schema change events fail to be dispatched due to inconsistent case [DBZ-2555](https://issues.jboss.org/browse/DBZ-2555)
* Use dedicated functional interface for struct generators [DBZ-2588](https://issues.jboss.org/browse/DBZ-2588)
* Remove obsolete note from docs [DBZ-2590](https://issues.jboss.org/browse/DBZ-2590)
* Intermittent test failure on CI - ReplicationConnectionIT#shouldResumeFromLastReceivedLSN [DBZ-2435](https://issues.jboss.org/browse/DBZ-2435)
* Intermittent test failure on CI - PostgresConnectorIT#shouldExecuteOnConnectStatements [DBZ-2468](https://issues.jboss.org/browse/DBZ-2468)
* Intermittent test failure on CI - AbstractSqlServerDatatypesTest#stringTypes() [DBZ-2474](https://issues.jboss.org/browse/DBZ-2474)
* Intermittent test failure on CI - PostgresConnectorIT#customSnapshotterSkipsTablesOnRestart() [DBZ-2544](https://issues.jboss.org/browse/DBZ-2544)
* Intermittent test failure on CI - SQLServerConnectorIT#verifyOffsets [DBZ-2599](https://issues.jboss.org/browse/DBZ-2599)



## 1.3.0.CR1
September 24th, 2020 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12350459)

### New features since 1.3.0.Beta2

* Describe configurations options for auto-created change data topics [DBZ-78](https://issues.jboss.org/browse/DBZ-78)


### Breaking changes since 1.3.0.Beta2

* Extract scripting SMTs into a separate module with separate installation package [DBZ-2549](https://issues.jboss.org/browse/DBZ-2549)


### Fixes since 1.3.0.Beta2

* Outbox | Heartbeat not working when using ByteBufferConverter [DBZ-2396](https://issues.jboss.org/browse/DBZ-2396)
* Catch up streaming before snapshot may duplicate messages upon resuming streaming [DBZ-2550](https://issues.jboss.org/browse/DBZ-2550)
* Fix Quarkus datasource configuration for Quarkus 1.9 [DBZ-2558](https://issues.jboss.org/browse/DBZ-2558)


### Other changes since 1.3.0.Beta2

* Show custom images instead of S2I in docs [DBZ-2236](https://issues.jboss.org/browse/DBZ-2236)
* Add Db2 tests to OpenShift test-suite and CI  [DBZ-2383](https://issues.jboss.org/browse/DBZ-2383)
* Implement connection retry support for Oracle [DBZ-2531](https://issues.jboss.org/browse/DBZ-2531)
* Format updates in doc for topic routing and event flattening SMTs [DBZ-2554](https://issues.jboss.org/browse/DBZ-2554)
* Coordinate docs work for downstream 1.3 release [DBZ-2557](https://issues.jboss.org/browse/DBZ-2557)
* Extend connect image build script with ability to add extra libraries [DBZ-2560](https://issues.jboss.org/browse/DBZ-2560)
* Invalid use of AppProtocol instead of protocol field in OpenShiftUtils service creation method [DBZ-2562](https://issues.jboss.org/browse/DBZ-2562)
* Doc format updates for better downstream rendering [DBZ-2564](https://issues.jboss.org/browse/DBZ-2564)
* Prepare revised SMT docs (filter and content-based routing) for downstream  [DBZ-2567](https://issues.jboss.org/browse/DBZ-2567)
* Swap closing square bracket for curly brace in downstream title annotations [DBZ-2577](https://issues.jboss.org/browse/DBZ-2577)



## 1.3.0.Beta2
September 16th, 2020 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12347109)

### New features since 1.3.0.Beta1

* Ingest change data from Oracle databases using LogMiner [DBZ-137](https://issues.redhat.com/browse/DBZ-137)
* Server-side column filtering in SQL Server connector [DBZ-1068](https://issues.redhat.com/browse/DBZ-1068)
* Introduce column.include.list for MySQL Connector [DBZ-2508](https://issues.redhat.com/browse/DBZ-2508)


### Breaking changes since 1.3.0.Beta1

* Avoid divisive language in docs and option names in incubator connectors [DBZ-2462](https://issues.redhat.com/browse/DBZ-2462)


### Fixes since 1.3.0.Beta1

* Increase Maven version in enforcer plugin [DBZ-2281](https://issues.redhat.com/browse/DBZ-2281)
* JSON functions in MySQL grammar unsupported [DBZ-2453](https://issues.redhat.com/browse/DBZ-2453)
* PostgresStreamingChangeEventSource's replicationStream flushLsn after closed [DBZ-2461](https://issues.redhat.com/browse/DBZ-2461)
* Fix link rendering for include.list and exclude.list properties [DBZ-2476](https://issues.redhat.com/browse/DBZ-2476)
* CVE-2019-10172 - security vulnerability [DBZ-2509](https://issues.redhat.com/browse/DBZ-2509)
* ArrayIndexOutOfBoundsException with excluded column from CDC table [DBZ-2522](https://issues.redhat.com/browse/DBZ-2522)
* maven-surefire-plugin versions defined twice in parent pom [DBZ-2523](https://issues.redhat.com/browse/DBZ-2523)
* Connector Type properties has missing displayName property [DBZ-2526](https://issues.redhat.com/browse/DBZ-2526)


### Other changes since 1.3.0.Beta1

* Allow Postgres snapshotter to set streaming start position [DBZ-2094](https://issues.redhat.com/browse/DBZ-2094)
* Ability to include Db2 driver in downstream image [DBZ-2191](https://issues.redhat.com/browse/DBZ-2191)
* Unify representation of events in the documentation [DBZ-2226](https://issues.redhat.com/browse/DBZ-2226)
* CloudEvents remains TP but has avro support downstream [DBZ-2245](https://issues.redhat.com/browse/DBZ-2245)
* Document new SMTs: content-based-routing and filtering [DBZ-2247](https://issues.redhat.com/browse/DBZ-2247)
* Document new Schema Change Topics [DBZ-2248](https://issues.redhat.com/browse/DBZ-2248)
* Change db2 version in Dockerfile from latest [DBZ-2257](https://issues.redhat.com/browse/DBZ-2257)
* Prepare DB2 connector doc for TP [DBZ-2403](https://issues.redhat.com/browse/DBZ-2403)
* Strimzi cluster operator no longer exposes service to access prometheus metrics endpoint [DBZ-2407](https://issues.redhat.com/browse/DBZ-2407)
* Clarify include/exclude filters for MongoDB are lists of regexps [DBZ-2429](https://issues.redhat.com/browse/DBZ-2429)
* Mongo SMT dose not support `add.fields=patch` [DBZ-2455](https://issues.redhat.com/browse/DBZ-2455)
* Prepare message filtering SMT doc for product release [DBZ-2460](https://issues.redhat.com/browse/DBZ-2460)
* Avoid divisive language in docs and option names in incubator connectors [DBZ-2462](https://issues.redhat.com/browse/DBZ-2462)
* Intermittent test failure on CI - FieldRenamesIT [DBZ-2464](https://issues.redhat.com/browse/DBZ-2464)
* Adjust outbox extension to updated Quarkus semantics [DBZ-2465](https://issues.redhat.com/browse/DBZ-2465)
* Add a locking mode which doesn't conflict with DML and existing reads on Percona Server [DBZ-2466](https://issues.redhat.com/browse/DBZ-2466)
* Ignore SSL issues during release job [DBZ-2467](https://issues.redhat.com/browse/DBZ-2467)
* Fix Debezium Server documentation for transformations and Google Pub/Sub [DBZ-2469](https://issues.redhat.com/browse/DBZ-2469)
* Remove unnecessary include/exclude database configuration in order to ensure backwards compatibility in OCP test-suite [DBZ-2470](https://issues.redhat.com/browse/DBZ-2470)
* Edit the features topic [DBZ-2477](https://issues.redhat.com/browse/DBZ-2477)
* False negatives by commit message format checker [DBZ-2479](https://issues.redhat.com/browse/DBZ-2479)
* Document outbox event router SMT [DBZ-2480](https://issues.redhat.com/browse/DBZ-2480)
* Error when processing commitLogs related to frozen type collections [DBZ-2498](https://issues.redhat.com/browse/DBZ-2498)
* Doc tweaks required to automatically build Db2 content in downstream user guide [DBZ-2500](https://issues.redhat.com/browse/DBZ-2500)
* Unify representation of events - part two - update other connector doc [DBZ-2501](https://issues.redhat.com/browse/DBZ-2501)
* Ability to specify kafka version for OCP ci job [DBZ-2502](https://issues.redhat.com/browse/DBZ-2502)
* Add ability to configure prefix for the add.fields and add.headers [DBZ-2504](https://issues.redhat.com/browse/DBZ-2504)
* Upgrade apicurio to 1.3.0.Final [DBZ-2507](https://issues.redhat.com/browse/DBZ-2507)
* Add more logs to Cassandra Connector [DBZ-2510](https://issues.redhat.com/browse/DBZ-2510)
* Create Configuration Fields for datatype.propagate.source.type and column.propagate.source.type [DBZ-2516](https://issues.redhat.com/browse/DBZ-2516)
* Prepare content-based router SMT doc for product release [DBZ-2519](https://issues.redhat.com/browse/DBZ-2519)
* Add missing ListOfRegex validator to all regex list fields and remove legacy whitelist/blacklist dependents [DBZ-2527](https://issues.redhat.com/browse/DBZ-2527)
* Add annotations to support splitting files for downstream docs  [DBZ-2539](https://issues.redhat.com/browse/DBZ-2539)



## 1.3.0.Beta1
August 28th, 2020 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12346874)

### New features since 1.3.0.Alpha1

* Improve error handling in Cassandra Connector [DBZ-2410](https://issues.jboss.org/browse/DBZ-2410)
* Add few MySql8 privileges support [DBZ-2413](https://issues.jboss.org/browse/DBZ-2413)
* Add support for MySql Dynamic Privileges [DBZ-2415](https://issues.jboss.org/browse/DBZ-2415)
* Support for MySql8 invisible / visible index [DBZ-2425](https://issues.jboss.org/browse/DBZ-2425)
* Hitting "Unable to unregister the MBean" when stopping an embedded engine [DBZ-2427](https://issues.jboss.org/browse/DBZ-2427)


### Breaking changes since 1.3.0.Alpha1

* Avoid divisive language in docs and option names in core connectors [DBZ-2171](https://issues.jboss.org/browse/DBZ-2171)


### Fixes since 1.3.0.Alpha1

* Adding new table to cdc causes the sqlconnector to fail [DBZ-2303](https://issues.jboss.org/browse/DBZ-2303)
* LSNs in replication slots are not monotonically increasing [DBZ-2338](https://issues.jboss.org/browse/DBZ-2338)
* Transaction data loss when process restarted [DBZ-2397](https://issues.jboss.org/browse/DBZ-2397)
* java.lang.NullPointerException in ByLogicalTableRouter.java [DBZ-2412](https://issues.jboss.org/browse/DBZ-2412)
* Snapshot fails if table or schema contain hyphens [DBZ-2452](https://issues.jboss.org/browse/DBZ-2452)


### Other changes since 1.3.0.Alpha1

* Upgrade OpenShift guide [DBZ-1908](https://issues.jboss.org/browse/DBZ-1908)
* Refactor: Add domain type for LSN [DBZ-2200](https://issues.jboss.org/browse/DBZ-2200)
* Entries in metrics tables should be linkable [DBZ-2375](https://issues.jboss.org/browse/DBZ-2375)
* Update some doc file names  [DBZ-2402](https://issues.jboss.org/browse/DBZ-2402)
* Asciidoc throw warnings while building documentation [DBZ-2408](https://issues.jboss.org/browse/DBZ-2408)
* Upgrade to Kafka 2.6.0 [DBZ-2411](https://issues.jboss.org/browse/DBZ-2411)
* Confusing way of reporting incorrect DB credentials [DBZ-2418](https://issues.jboss.org/browse/DBZ-2418)
* Default value for database port isn't honoured [DBZ-2423](https://issues.jboss.org/browse/DBZ-2423)
* Update to Quarkus 1.7.1.Final [DBZ-2454](https://issues.jboss.org/browse/DBZ-2454)



## 1.3.0.Alpha1
August 6th, 2020 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12345155)

### New features since 1.2.1.Final

* Postgres and possibly other DB connections are not properly shutdown when the task encounters thread interrupt [DBZ-2133](https://issues.redhat.com/browse/DBZ-2133)
* More flexible connection options for MongoDB [DBZ-2225](https://issues.redhat.com/browse/DBZ-2225)
* Sink adapter for Azure Event Hubs [DBZ-2282](https://issues.redhat.com/browse/DBZ-2282)
* Implement new snapshot mode - initial_only [DBZ-2379](https://issues.redhat.com/browse/DBZ-2379)


### Breaking changes since 1.2.1.Final

* Deprecate `mongodb.poll.interval.sec` and add `mongodb.poll.interval.ms`. [DBZ-2400](https://issues.redhat.com/browse/DBZ-2400)


### Fixes since 1.2.1.Final

* Ignore non-existing table reported on Aurora via SHOW TABLES [DBZ-1939](https://issues.redhat.com/browse/DBZ-1939)
* Cassandra connector not getting events [DBZ-2086](https://issues.redhat.com/browse/DBZ-2086)
* PubSub Sink sends empty records [DBZ-2277](https://issues.redhat.com/browse/DBZ-2277)
* Skipping LSN is inefficient and does not forward slot position [DBZ-2310](https://issues.redhat.com/browse/DBZ-2310)
* message size is at least 68x larger for changes with bit varying columns [DBZ-2315](https://issues.redhat.com/browse/DBZ-2315)
* Change events lost when connnector is restarted while processing transaction with PK update [DBZ-2329](https://issues.redhat.com/browse/DBZ-2329)
* Error when processing commitLogs related to list-type columns [DBZ-2345](https://issues.redhat.com/browse/DBZ-2345)
* Fix dependency groupId on Outbox Quarkus Extension documentation [DBZ-2367](https://issues.redhat.com/browse/DBZ-2367)
* Cannot detect Azure Sql Version [DBZ-2373](https://issues.redhat.com/browse/DBZ-2373)
* ParallelSnapshotReader sometimes throws NPE  [DBZ-2387](https://issues.redhat.com/browse/DBZ-2387)


### Other changes since 1.2.1.Final

* Column default values are not extracted while reading table structure [DBZ-1491](https://issues.redhat.com/browse/DBZ-1491)
* DataException("Struct schemas do not match.") when recording cellData  [DBZ-2103](https://issues.redhat.com/browse/DBZ-2103)
* Provide container image for Debezium Server [DBZ-2147](https://issues.redhat.com/browse/DBZ-2147)
* Update binlog client [DBZ-2173](https://issues.redhat.com/browse/DBZ-2173)
* PostgreSQL test matrix runs incorrect test-suite [DBZ-2279](https://issues.redhat.com/browse/DBZ-2279)
* Use ARG with defaults for Kafka's versions and sha when building Kafka Docker image [DBZ-2323](https://issues.redhat.com/browse/DBZ-2323)
* Test failures on Kafka 1.x CI job [DBZ-2332](https://issues.redhat.com/browse/DBZ-2332)
* Modularize doc for PostgreSQL component [DBZ-2333](https://issues.redhat.com/browse/DBZ-2333)
* Add configurable restart wait time and connection retires [DBZ-2362](https://issues.redhat.com/browse/DBZ-2362)
* Support data types from other database engines [DBZ-2365](https://issues.redhat.com/browse/DBZ-2365)
* Featured posts list broken [DBZ-2374](https://issues.redhat.com/browse/DBZ-2374)
* Add ProtoBuf support for Debezium Server [DBZ-2381](https://issues.redhat.com/browse/DBZ-2381)
* Intermittent test failure on CI - SqlServerChangeTableSetIT#addDefaultValue [DBZ-2389](https://issues.redhat.com/browse/DBZ-2389)
* Intermittent test failure on CI - TablesWithoutPrimaryKeyIT#shouldProcessFromStreaming [DBZ-2390](https://issues.redhat.com/browse/DBZ-2390)
* Include Azure PostgreSQL guidance in the docs [DBZ-2394](https://issues.redhat.com/browse/DBZ-2394)
* Update JSON Snippet on MongoDB Docs Page [DBZ-2395](https://issues.redhat.com/browse/DBZ-2395)


## 1.2.1.Final
July 16th, 2020 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12346704)

### New features since 1.2.0.Final

* Document content based routing and filtering for MongoDB [DBZ-2255](https://issues.jboss.org/browse/DBZ-2255)
* Handle MariaDB syntax add column IF EXISTS as part of alter table DDL [DBZ-2219](https://issues.jboss.org/browse/DBZ-2219)
* Add Apicurio converters to Connect container image [DBZ-2083](https://issues.jboss.org/browse/DBZ-2083)


### Breaking changes since 1.2.0.Final

None


### Fixes since 1.2.0.Final

* MongoDB connector is not resilient to Mongo connection errors [DBZ-2141](https://issues.jboss.org/browse/DBZ-2141)
* MySQL connector should filter additional DML binlog entries for RDS by default [DBZ-2275](https://issues.jboss.org/browse/DBZ-2275)
* Concurrent access to a thread map [DBZ-2278](https://issues.jboss.org/browse/DBZ-2278)
* Postgres connector may skip events during snapshot-streaming transition [DBZ-2288](https://issues.jboss.org/browse/DBZ-2288)
* MySQL connector emits false error while missing a required data [DBZ-2301](https://issues.jboss.org/browse/DBZ-2301)
* io.debezium.engine.spi.OffsetCommitPolicy.PeriodicCommitOffsetPolicy can't be initiated due to NoSuchMethod error   [DBZ-2302](https://issues.jboss.org/browse/DBZ-2302)
* Allow single dimension DECIMAL in CAST [DBZ-2305](https://issues.jboss.org/browse/DBZ-2305)
* MySQL JSON functions are missing from the grammar [DBZ-2318](https://issues.jboss.org/browse/DBZ-2318)
* Description in documentation metrics tables is bold and shouldn't be [DBZ-2326](https://issues.jboss.org/browse/DBZ-2326)
* ALTER TABLE with `timestamp default CURRENT_TIMESTAMP not null` fails the task [DBZ-2330](https://issues.jboss.org/browse/DBZ-2330)


### Other changes since 1.2.0.Final

* Unstable tests in SQL Server connector [DBZ-2217](https://issues.jboss.org/browse/DBZ-2217)
* Intermittent test failure on CI - SqlServerConnectorIT#verifyOffsets() [DBZ-2220](https://issues.jboss.org/browse/DBZ-2220)
* Intermittent test failure on CI - MySQL [DBZ-2229](https://issues.jboss.org/browse/DBZ-2229)
* Intermittent test failure on CI - SqlServerChangeTableSetIT#readHistoryAfterRestart() [DBZ-2231](https://issues.jboss.org/browse/DBZ-2231)
* Failing test MySqlSourceTypeInSchemaIT.shouldPropagateSourceTypeAsSchemaParameter [DBZ-2238](https://issues.jboss.org/browse/DBZ-2238)
* Intermittent test failure on CI - MySqlConnectorRegressionIT#shouldConsumeAllEventsFromDatabaseUsingBinlogAndNoSnapshot() [DBZ-2243](https://issues.jboss.org/browse/DBZ-2243)
* Use upstream image in ApicurioRegistryTest [DBZ-2256](https://issues.jboss.org/browse/DBZ-2256)
* Intermittent failure of MongoDbConnectorIT.shouldConsumeTransaction [DBZ-2264](https://issues.jboss.org/browse/DBZ-2264)
* Intermittent test failure on CI - MySqlSourceTypeInSchemaIT#shouldPropagateSourceTypeByDatatype() [DBZ-2269](https://issues.jboss.org/browse/DBZ-2269)
* Intermittent test failure on CI - MySqlConnectorIT#shouldNotParseQueryIfServerOptionDisabled [DBZ-2270](https://issues.jboss.org/browse/DBZ-2270)
* Intermittent test failure on CI - RecordsStreamProducerIT#testEmptyChangesProducesHeartbeat [DBZ-2271](https://issues.jboss.org/browse/DBZ-2271)
* Incorrect dependency from outbox to core module [DBZ-2276](https://issues.jboss.org/browse/DBZ-2276)
* Slowness in FieldRenamesTest [DBZ-2286](https://issues.jboss.org/browse/DBZ-2286)
* Create GitHub Action for verifying correct formatting [DBZ-2287](https://issues.jboss.org/browse/DBZ-2287)
* Clarify expectations for replica identity and key-less tables [DBZ-2307](https://issues.jboss.org/browse/DBZ-2307)
* Jenkins worker nodes must be logged in to Docker Hub [DBZ-2312](https://issues.jboss.org/browse/DBZ-2312)
* Upgrade PostgreSQL driver to 4.2.14 [DBZ-2317](https://issues.jboss.org/browse/DBZ-2317)
* Intermittent test failure on CI - PostgresConnectorIT#shouldOutputRecordsInCloudEventsFormat [DBZ-2319](https://issues.jboss.org/browse/DBZ-2319)
* Intermittent test failure on CI - TablesWithoutPrimaryKeyIT#shouldProcessFromStreaming [DBZ-2324](https://issues.jboss.org/browse/DBZ-2324)
* Intermittent test failure on CI - SqlServerConnectorIT#readOnlyApplicationIntent [DBZ-2325](https://issues.jboss.org/browse/DBZ-2325)
* Intermittent test failure on CI - SnapshotIT#takeSnapshotWithOldStructAndStartStreaming [DBZ-2331](https://issues.jboss.org/browse/DBZ-2331)



## 1.2.0.Final
June 24th, 2020 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12345052)

### New features since 1.2.0.CR2

None

### Breaking changes since 1.2.0.CR2

None


### Fixes since 1.2.0.CR2

* Test failure due to superfluous schema change event emitted on connector start [DBZ-2211](https://issues.jboss.org/browse/DBZ-2211)
* Intermittent test failures on CI [DBZ-2232](https://issues.jboss.org/browse/DBZ-2232)
* Test SimpleSourceConnectorOutputTest.shouldGenerateExpected blocked [DBZ-2241](https://issues.jboss.org/browse/DBZ-2241)
* CloudEventsConverter should use Apicurio converter for Avro [DBZ-2250](https://issues.jboss.org/browse/DBZ-2250)
* Default value is not properly set for non-optional columns [DBZ-2267](https://issues.jboss.org/browse/DBZ-2267)


### Other changes since 1.2.0.CR2

* Diff MySQL connector 0.10 and latest docs [DBZ-1997](https://issues.jboss.org/browse/DBZ-1997)
* Remove redundant property in antora.yml [DBZ-2223](https://issues.jboss.org/browse/DBZ-2223)
* Binary log client is not cleanly stopped in testsuite [DBZ-2221](https://issues.jboss.org/browse/DBZ-2221)
* Intermittent test failure on CI - Postgres [DBZ-2230](https://issues.jboss.org/browse/DBZ-2230)
* Build failure with Kafka 1.x [DBZ-2240](https://issues.jboss.org/browse/DBZ-2240)
* Intermittent test failure on CI - SqlServerConnectorIT#readOnlyApplicationIntent() [DBZ-2261](https://issues.jboss.org/browse/DBZ-2261)
* Test failure BinlogReaderIT#shouldFilterAllRecordsBasedOnDatabaseWhitelistFilter() [DBZ-2262](https://issues.jboss.org/browse/DBZ-2262)



## 1.2.0.CR2
June 18th, 2020 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12346173)

### New features since 1.2.0.CR1

* DB2 connector documentation ambiguous regarding licensing [DBZ-1835](https://issues.jboss.org/browse/DBZ-1835)
* Optimize SQLServer connector query [DBZ-2120](https://issues.jboss.org/browse/DBZ-2120)
* Documentation for implementing StreamNameMapper [DBZ-2163](https://issues.jboss.org/browse/DBZ-2163)
* Update architecture page [DBZ-2096](https://issues.jboss.org/browse/DBZ-2096)


### Breaking changes since 1.2.0.CR1

* Debezium server distro has been moved [DBZ-2212](https://issues.jboss.org/browse/DBZ-2212)


### Fixes since 1.2.0.CR1

* Encountered error when snapshotting collection type column [DBZ-2117](https://issues.jboss.org/browse/DBZ-2117)
* Missing dependencies for Debezium Server Pulsar sink [DBZ-2201](https://issues.jboss.org/browse/DBZ-2201)
* Intermittent test failure -- Multiple admin clients with same id [DBZ-2228](https://issues.jboss.org/browse/DBZ-2228)
* Adapt to changed TX representation in oplog in Mongo 4.2 [DBZ-2216](https://issues.jboss.org/browse/DBZ-2216)


### Other changes since 1.2.0.CR1

* Tests Asserting No Open Transactions Failing [DBZ-2176](https://issues.jboss.org/browse/DBZ-2176)
* General test harness for End-2-End Benchmarking [DBZ-1812](https://issues.jboss.org/browse/DBZ-1812)
* Add tests for datatype.propagate.source.type for all connectors [DBZ-1916](https://issues.jboss.org/browse/DBZ-1916)
* Productize CloudEvents support [DBZ-2019](https://issues.jboss.org/browse/DBZ-2019)
* [Doc] Add Debezium Architecture to downstream documentation [DBZ-2029](https://issues.jboss.org/browse/DBZ-2029)
* Transaction metadata documentation [DBZ-2069](https://issues.jboss.org/browse/DBZ-2069)
* Inconsistent test failures [DBZ-2177](https://issues.jboss.org/browse/DBZ-2177)
* Add Jandex plugin to Debezium Server connectors [DBZ-2192](https://issues.jboss.org/browse/DBZ-2192)
* Ability to scale wait times in OCP test-suite [DBZ-2194](https://issues.jboss.org/browse/DBZ-2194)
* CI doesn't delete mongo and sql server projects on successful runs [DBZ-2195](https://issues.jboss.org/browse/DBZ-2195)
* Document database history and web server port for Debezium Server [DBZ-2198](https://issues.jboss.org/browse/DBZ-2198)
* Do not throw IndexOutOfBoundsException when no task configuration is available [DBZ-2199](https://issues.jboss.org/browse/DBZ-2199)
* Upgrade Apicurio to 1.2.2.Final [DBZ-2206](https://issues.jboss.org/browse/DBZ-2206)
* Intermitent test failures [DBZ-2207](https://issues.jboss.org/browse/DBZ-2207)
* Increase Pulsar Server timeouts [DBZ-2210](https://issues.jboss.org/browse/DBZ-2210)
* Drop distribution from Debezium Server artifact name [DBZ-2214](https://issues.jboss.org/browse/DBZ-2214)



## 1.2.0.CR1
June 10th, 2020 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12345858)

### New features since 1.2.0.Beta2

* Restrict the set of tables with a publication when using pgoutput [DBZ-1813](https://issues.jboss.org/browse/DBZ-1813)
* Support configuring different encodings for binary source data [DBZ-1814](https://issues.jboss.org/browse/DBZ-1814)
* Add API for not registering metrics MBean into the platform MBean server [DBZ-2089](https://issues.jboss.org/browse/DBZ-2089)
* Unable to handle UDT data [DBZ-2091](https://issues.jboss.org/browse/DBZ-2091)
* Improve SQL Server reconnect during shutdown and connection resets [DBZ-2106](https://issues.jboss.org/browse/DBZ-2106)
* OpenShift tests for SQL Server connector before GA [DBZ-2113](https://issues.jboss.org/browse/DBZ-2113)
* OpenShift tests for MongoDB Connector before GA [DBZ-2114](https://issues.jboss.org/browse/DBZ-2114)
* Log begin/end of schema recovery on INFO level [DBZ-2149](https://issues.jboss.org/browse/DBZ-2149)
* Allow outbox EventRouter to pass non-String based Keys [DBZ-2152](https://issues.jboss.org/browse/DBZ-2152)
* Introduce API  checks [DBZ-2159](https://issues.jboss.org/browse/DBZ-2159)
* Bump mysql binlog version  [DBZ-2160](https://issues.jboss.org/browse/DBZ-2160)
* Postgresql - Allow for include.unknown.datatypes to return string instead of hash [DBZ-1266](https://issues.jboss.org/browse/DBZ-1266)
* Consider Apicurio registry [DBZ-1639](https://issues.jboss.org/browse/DBZ-1639)
* Debezium Server should support Google Cloud PubSub [DBZ-2092](https://issues.jboss.org/browse/DBZ-2092)
* Sink adapter for Apache Pulsar [DBZ-2112](https://issues.jboss.org/browse/DBZ-2112)


### Breaking changes since 1.2.0.Beta2

* Change table.whitelist/table.blacklist format [DBZ-1312](https://issues.jboss.org/browse/DBZ-1312)


### Fixes since 1.2.0.Beta2

* Transaction opened by Debezium is left idle and never committed [DBZ-2118](https://issues.jboss.org/browse/DBZ-2118)
* Don't call markBatchFinished() in finally block [DBZ-2124](https://issues.jboss.org/browse/DBZ-2124)
* kafka SSL passwords need to be added to the Sensitive Properties list [DBZ-2125](https://issues.jboss.org/browse/DBZ-2125)
* Intermittent test failure on CI - SQL Server [DBZ-2126](https://issues.jboss.org/browse/DBZ-2126)
* CREATE TABLE query is giving parsing exception [DBZ-2130](https://issues.jboss.org/browse/DBZ-2130)
* Misc. Javadoc and docs fixes [DBZ-2136](https://issues.jboss.org/browse/DBZ-2136)
* Avro schema doesn't change if a column default value is dropped [DBZ-2140](https://issues.jboss.org/browse/DBZ-2140)
* Multiple SETs not supported in trigger [DBZ-2142](https://issues.jboss.org/browse/DBZ-2142)
* Don't validate internal database.history.connector.* config parameters [DBZ-2144](https://issues.jboss.org/browse/DBZ-2144)
* ANTLR parser doesn't handle MariaDB syntax drop index IF EXISTS as part of alter table DDL [DBZ-2151](https://issues.jboss.org/browse/DBZ-2151)
* Casting as INT causes a ParsingError [DBZ-2153](https://issues.jboss.org/browse/DBZ-2153)
* Calling function UTC_TIMESTAMP without parenthesis causes a parsing error [DBZ-2154](https://issues.jboss.org/browse/DBZ-2154)
* Could not find or load main class io.debezium.server.Main [DBZ-2170](https://issues.jboss.org/browse/DBZ-2170)
* MongoDB connector snapshot NPE in case of document field named "op" [DBZ-2116](https://issues.jboss.org/browse/DBZ-2116)


### Other changes since 1.2.0.Beta2

* Adding tests and doc updates around column masking and truncating [DBZ-775](https://issues.jboss.org/browse/DBZ-775)
* Refactor/use common configuration parameters [DBZ-1657](https://issues.jboss.org/browse/DBZ-1657)
* Develop sizing recommendations, load tests etc. [DBZ-1662](https://issues.jboss.org/browse/DBZ-1662)
* Add performance test for SMTs like filters [DBZ-1929](https://issues.jboss.org/browse/DBZ-1929)
* Add banner to older doc versions about them being outdated [DBZ-1951](https://issues.jboss.org/browse/DBZ-1951)
* SMT Documentation [DBZ-2021](https://issues.jboss.org/browse/DBZ-2021)
* Instable integration test with Testcontainers [DBZ-2033](https://issues.jboss.org/browse/DBZ-2033)
* Add test for schema history topic for Oracle connector [DBZ-2056](https://issues.jboss.org/browse/DBZ-2056)
* Random test failures [DBZ-2060](https://issues.jboss.org/browse/DBZ-2060)
* Set up CI jobs for JDK 14/15 [DBZ-2065](https://issues.jboss.org/browse/DBZ-2065)
* Introduce Any type for server to seamlessly integrate with Debezium API [DBZ-2104](https://issues.jboss.org/browse/DBZ-2104)
* Update AsciiDoc markup in doc files for downstream reuse [DBZ-2105](https://issues.jboss.org/browse/DBZ-2105)
* Upgrade to Quarkus 1.5.0.Final [DBZ-2119](https://issues.jboss.org/browse/DBZ-2119)
* Additional AsciiDoc markup updates needed in doc files for downstream reuse [DBZ-2129](https://issues.jboss.org/browse/DBZ-2129)
* Refactor & Extend OpenShift test-suite tooling to prepare for MongoDB and SQL Server [DBZ-2132](https://issues.jboss.org/browse/DBZ-2132)
* OpenShift tests are failing  when waiting for Connect metrics to be exposed [DBZ-2135](https://issues.jboss.org/browse/DBZ-2135)
* Support incubator build in product release jobs [DBZ-2137](https://issues.jboss.org/browse/DBZ-2137)
* Rebase MySQL grammar on the latest upstream version [DBZ-2143](https://issues.jboss.org/browse/DBZ-2143)
* Await coordinator shutdown in embedded engine [DBZ-2150](https://issues.jboss.org/browse/DBZ-2150)
* More meaningful exception in case of replication slot conflict [DBZ-2156](https://issues.jboss.org/browse/DBZ-2156)
* Intermittent test failure on CI - Postgres [DBZ-2157](https://issues.jboss.org/browse/DBZ-2157)
* OpenShift pipeline uses incorrect projects for Mongo and Sql Server deployment [DBZ-2164](https://issues.jboss.org/browse/DBZ-2164)
* Incorrect polling timeout in AbstractReader [DBZ-2169](https://issues.jboss.org/browse/DBZ-2169)



## 1.2.0.Beta2
May 19th, 2020 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12345708)

### New features since 1.2.0.Beta1

* Add JDBC driver versions to docs [DBZ-2031](https://issues.jboss.org/browse/DBZ-2031)
* Add a few more loggings for Cassandra Connector [DBZ-2066](https://issues.jboss.org/browse/DBZ-2066)
* Provide ready-to-use standalone application based on the embedded engine [DBZ-651](https://issues.jboss.org/browse/DBZ-651)
* Add option to skip LSN timestamp queries [DBZ-1988](https://issues.jboss.org/browse/DBZ-1988)
* Add option to logical topic router for controlling placement of table information [DBZ-2034](https://issues.jboss.org/browse/DBZ-2034)
* Add headers and topic name into scripting transforms [DBZ-2074](https://issues.jboss.org/browse/DBZ-2074)
* Filter and content-based router SMTs should be restrictable to certain topics [DBZ-2024](https://issues.jboss.org/browse/DBZ-2024)


### Breaking changes since 1.2.0.Beta1

* Remove deprecated features [DBZ-1828](https://issues.jboss.org/browse/DBZ-1828)
* Db2: Replace `initial_schema_only` with `schema_only` [DBZ-2051](https://issues.jboss.org/browse/DBZ-2051)
* DebeziumContainer should allow for custom container images [DBZ-2070](https://issues.jboss.org/browse/DBZ-2070)


### Fixes since 1.2.0.Beta1

* Avro schema doesn't change if a column default value changes from 'foo' to 'bar' [DBZ-2061](https://issues.jboss.org/browse/DBZ-2061)
* DDL statement throws error if compression keyword contains backticks (``) [DBZ-2062](https://issues.jboss.org/browse/DBZ-2062)
* Error and connector stops when DDL contains algorithm=instant [DBZ-2067](https://issues.jboss.org/browse/DBZ-2067)
* Debezium Engine advanced record consuming example broken [DBZ-2073](https://issues.jboss.org/browse/DBZ-2073)
* Unable to parse MySQL ALTER statement with named primary key [DBZ-2080](https://issues.jboss.org/browse/DBZ-2080)
* Missing schema-serializer dependency for Avro [DBZ-2082](https://issues.jboss.org/browse/DBZ-2082)
* TinyIntOneToBooleanConverter doesn't seem to work with columns having a default value. [DBZ-2085](https://issues.jboss.org/browse/DBZ-2085)


### Other changes since 1.2.0.Beta1

* Add ability to insert fields from op field in ExtractNewDocumentState [DBZ-1791](https://issues.jboss.org/browse/DBZ-1791)
* Test with MySQL 8.0.20 [DBZ-2041](https://issues.jboss.org/browse/DBZ-2041)
* Update debezium-examples/tutorial README docker-compose file is missing [DBZ-2059](https://issues.jboss.org/browse/DBZ-2059)
* Skip tests that are no longer compatible with Kafka 1.x [DBZ-2068](https://issues.jboss.org/browse/DBZ-2068)
* Remove additional Jackson dependencies as of AK 2.5 [DBZ-2076](https://issues.jboss.org/browse/DBZ-2076)
* Make EventProcessingFailureHandlingIT resilient against timing issues [DBZ-2078](https://issues.jboss.org/browse/DBZ-2078)
* Tar packages must use posix format [DBZ-2088](https://issues.jboss.org/browse/DBZ-2088)
* Remove unused sourceInfo variable [DBZ-2090](https://issues.jboss.org/browse/DBZ-2090)



## 1.2.0.Beta1
May 7th, 2020 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12345561)

### New features since 1.2.0.Alpha1

* Don't try to database history topic if it exists already [DBZ-1886](https://issues.jboss.org/browse/DBZ-1886)
* Deleted database history should be detected for all connectors [DBZ-1923](https://issues.jboss.org/browse/DBZ-1923)
* Provide anchors to connector parameters [DBZ-1933](https://issues.jboss.org/browse/DBZ-1933)
* move static methods TRUNCATE_COLUMN and MASK_COLUMN as attributes to RelationalDatabaseConnectorConfig [DBZ-1972](https://issues.jboss.org/browse/DBZ-1972)
* Implement SKIPPED_OPERATIONS for mysql [DBZ-1895](https://issues.jboss.org/browse/DBZ-1895)
* User facing schema history topic for SQL Server [DBZ-1904](https://issues.jboss.org/browse/DBZ-1904)
* Multiline stack traces can be collapsed into a single log event  [DBZ-1913](https://issues.jboss.org/browse/DBZ-1913)
* Introduce column.whitelist for Postgres Connector [DBZ-1962](https://issues.jboss.org/browse/DBZ-1962)
* Add support for Postgres time, timestamp array columns [DBZ-1969](https://issues.jboss.org/browse/DBZ-1969)
* Add support for Postgres Json and Jsonb array columns [DBZ-1990](https://issues.jboss.org/browse/DBZ-1990)
* Content-based topic routing based on scripting languages [DBZ-2000](https://issues.jboss.org/browse/DBZ-2000)


### Breaking changes since 1.2.0.Alpha1

* Remove obsolete metrics from downstream docs [DBZ-1947](https://issues.jboss.org/browse/DBZ-1947)
* Outbox: Remove eventType field [DBZ-2014](https://issues.jboss.org/browse/DBZ-2014)
* Upgrade Postgres driver to 42.2.12 [DBZ-2027](https://issues.jboss.org/browse/DBZ-2027)
* Support different converters for key/value in embedded engine [DBZ-1970](https://issues.jboss.org/browse/DBZ-1970)


### Fixes since 1.2.0.Alpha1

* bit varying column has value that is too large to be cast to a long [DBZ-1949](https://issues.jboss.org/browse/DBZ-1949)
* PostgreSQL Sink connector with outbox event router and Avro uses wrong default io.confluent schema namespace [DBZ-1963](https://issues.jboss.org/browse/DBZ-1963)
* Stop processing new commitlogs in cdc folder [DBZ-1985](https://issues.jboss.org/browse/DBZ-1985)
* [Doc] Debezium User Guide should provide example of DB connector yaml and deployment instructions [DBZ-2011](https://issues.jboss.org/browse/DBZ-2011)
* ExtractNewRecordState SMT spamming logs for heartbeat messages [DBZ-2036](https://issues.jboss.org/browse/DBZ-2036)
* MySQL alias `FLUSH TABLE` not handled [DBZ-2047](https://issues.jboss.org/browse/DBZ-2047)
* Embedded engine not compatible with Kafka 1.x [DBZ-2054](https://issues.jboss.org/browse/DBZ-2054)


### Other changes since 1.2.0.Alpha1

* Blog post and demo about Debezium + Camel [DBZ-1656](https://issues.jboss.org/browse/DBZ-1656)
* Refactor connector config code to share the configuration definition [DBZ-1750](https://issues.jboss.org/browse/DBZ-1750)
* DB2 connector follow-up refactorings [DBZ-1753](https://issues.jboss.org/browse/DBZ-1753)
* Oracle JDBC driver available in Maven Central [DBZ-1878](https://issues.jboss.org/browse/DBZ-1878)
* Align snapshot/streaming semantics in MongoDB documentation [DBZ-1901](https://issues.jboss.org/browse/DBZ-1901)
* Add MySQL 5.5 and 5.6 to test matrix. [DBZ-1953](https://issues.jboss.org/browse/DBZ-1953)
* Upgrade to Quarkus to 1.4.1 release [DBZ-1975](https://issues.jboss.org/browse/DBZ-1975)
* Version selector on releases page should show all versions [DBZ-1979](https://issues.jboss.org/browse/DBZ-1979)
* Upgrade to Apache Kafka 2.5.0 and Confluent Platform 5.5.0 [DBZ-1981](https://issues.jboss.org/browse/DBZ-1981)
* Fix broken link [DBZ-1983](https://issues.jboss.org/browse/DBZ-1983)
* Update Outbox Quarkus extension yaml [DBZ-1991](https://issues.jboss.org/browse/DBZ-1991)
* Allow for simplified property references in filter SMT with graal.js [DBZ-1993](https://issues.jboss.org/browse/DBZ-1993)
* Avoid broken cross-book references in downstream docs [DBZ-1999](https://issues.jboss.org/browse/DBZ-1999)
* Fix wrong attribute name in MongoDB connector [DBZ-2006](https://issues.jboss.org/browse/DBZ-2006)
* Upgrade formatter and Impsort plugins [DBZ-2007](https://issues.jboss.org/browse/DBZ-2007)
* Clarify support for non-primary key tables in PostgreSQL documentation [DBZ-2010](https://issues.jboss.org/browse/DBZ-2010)
* Intermittent test failure on CI [DBZ-2030](https://issues.jboss.org/browse/DBZ-2030)
* Cleanup Postgres TypeRegistry [DBZ-2038](https://issues.jboss.org/browse/DBZ-2038)
* Upgrade to latest parent pom and checkstyle [DBZ-2039](https://issues.jboss.org/browse/DBZ-2039)
* Reduce build output to avoid maximum log length problems on CI [DBZ-2043](https://issues.jboss.org/browse/DBZ-2043)
* Postgres TypeRegistry makes one query per enum type at startup [DBZ-2044](https://issues.jboss.org/browse/DBZ-2044)



## 1.2.0.Alpha1
April 16th, 2020 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12344691)

### New features since 1.1.0.Final

* Expose original value for PK updates [DBZ-1531](https://issues.redhat.com/browse/DBZ-1531)
* New column masking mode: consistent hashing [DBZ-1692](https://issues.redhat.com/browse/DBZ-1692)
* Provide a filtering SMT [DBZ-1782](https://issues.redhat.com/browse/DBZ-1782)
* Support converters for embedded engine [DBZ-1807](https://issues.redhat.com/browse/DBZ-1807)
* Enhance MongoDB connector metrics [DBZ-1859](https://issues.redhat.com/browse/DBZ-1859)
* SQL Server connector: support reconnect after the database connection is broken [DBZ-1882](https://issues.redhat.com/browse/DBZ-1882)
* Support SMTs in embedded engine [DBZ-1930](https://issues.redhat.com/browse/DBZ-1930)
* Snapshot metrics shows TotalNumberOfEventsSeen as zero [DBZ-1932](https://issues.redhat.com/browse/DBZ-1932)


### Breaking changes since 1.1.0.Final

* Remove deprecated connector option value "initial_schema_only" [DBZ-1945](https://issues.redhat.com/browse/DBZ-1945)
* Remove deprecated unwrap SMTs [DBZ-1968](https://issues.redhat.com/browse/DBZ-1968)


### Fixes since 1.1.0.Final

* java.lang.IllegalArgumentException: Timestamp format must be yyyy-mm-dd hh:mm:ss[.fffffffff] [DBZ-1744](https://issues.redhat.com/browse/DBZ-1744)
* Snapshot lock timeout setting is not documented [DBZ-1914](https://issues.redhat.com/browse/DBZ-1914)
* AvroRuntimeException when publishing transaction metadata [DBZ-1915](https://issues.redhat.com/browse/DBZ-1915)
* Connector restart logic throttles for the first 2 seconds [DBZ-1918](https://issues.redhat.com/browse/DBZ-1918)
* Wal2json empty change event could cause NPE above version 1.0.3.final [DBZ-1922](https://issues.redhat.com/browse/DBZ-1922)
* Misleading error message on lost database connection [DBZ-1926](https://issues.redhat.com/browse/DBZ-1926)
* Cassandra CDC should not move and delete processed commitLog file under testing mode [DBZ-1927](https://issues.redhat.com/browse/DBZ-1927)
* Broken internal links and anchors in documentation [DBZ-1935](https://issues.redhat.com/browse/DBZ-1935)
* Dokumentation files in modules create separate pages, should be partials instead [DBZ-1944](https://issues.redhat.com/browse/DBZ-1944)
* Validation of binlog_row_image is not compatible with MySQL 5.5 [DBZ-1950](https://issues.redhat.com/browse/DBZ-1950)
* High CPU usage when idle [DBZ-1960](https://issues.redhat.com/browse/DBZ-1960)
* Outbox Quarkus Extension throws NPE in quarkus:dev mode [DBZ-1966](https://issues.redhat.com/browse/DBZ-1966)
* Cassandra Connector: unable to deserialize column mutation with reversed type [DBZ-1967](https://issues.redhat.com/browse/DBZ-1967)


### Other changes since 1.1.0.Final

* Replace Custom CassandraTopicSelector with DBZ's TopicSelector class in Cassandra Connector [DBZ-1407](https://issues.redhat.com/browse/DBZ-1407)
* Improve documentation on WAL disk space usage for Postgres connector [DBZ-1732](https://issues.redhat.com/browse/DBZ-1732)
* Outbox Quarkus Extension: Update version of extension used by demo [DBZ-1786](https://issues.redhat.com/browse/DBZ-1786)
* Community newsletter 1/2020 [DBZ-1806](https://issues.redhat.com/browse/DBZ-1806)
* Remove obsolete SnapshotChangeRecordEmitter [DBZ-1898](https://issues.redhat.com/browse/DBZ-1898)
* Fix typo in Quarkus Outbox extension documentation [DBZ-1902](https://issues.redhat.com/browse/DBZ-1902)
* Update schema change topic section of SQL Server connector doc [DBZ-1903](https://issues.redhat.com/browse/DBZ-1903)
* Log warning about insufficient retention time for DB history topic [DBZ-1905](https://issues.redhat.com/browse/DBZ-1905)
* Documentation should link to Apache Kafka upstream docs [DBZ-1906](https://issues.redhat.com/browse/DBZ-1906)
* The error messaging around binlog configuration is missleading [DBZ-1911](https://issues.redhat.com/browse/DBZ-1911)
* Restore documentation of MySQL event structures [DBZ-1919](https://issues.redhat.com/browse/DBZ-1919)
* Link from monitoring page to connector-specific metrics [DBZ-1920](https://issues.redhat.com/browse/DBZ-1920)
* Update snapshot.mode options in SQL Server documentation [DBZ-1924](https://issues.redhat.com/browse/DBZ-1924)
* Update build and container images to Apache Kafka 2.4.1 [DBZ-1925](https://issues.redhat.com/browse/DBZ-1925)
* Avoid Thread#sleep() calls in Oracle connector tests [DBZ-1942](https://issues.redhat.com/browse/DBZ-1942)
* Different versions of Jackson components pulled in as dependencies [DBZ-1943](https://issues.redhat.com/browse/DBZ-1943)
* Add docs for mask column and truncate column features [DBZ-1954](https://issues.redhat.com/browse/DBZ-1954)
* Upgrade MongoDB driver to 3.12.3 [DBZ-1958](https://issues.redhat.com/browse/DBZ-1958)



## 1.1.0.Final
March 23rd, 2020 [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=12344981)

### New features since 1.1.0.CR1

 * The Postgres connector heartbeat should optionally write back a heartbeat change to the DB [DBZ-1815](https://issues.redhat.com/browse/DBZ-1815)


### Breaking changes since 1.1.0.CR1

None


### Fixes since 1.1.0.CR1

 * Postgres Connector ignoring confirmed_flush_lsn and skipping ahead to latest txn [DBZ-1730](https://issues.redhat.com/browse/DBZ-1730)
 * Postgresql money error handling [DBZ-1755](https://issues.redhat.com/browse/DBZ-1755)
 * MongoDB tests not working correctly [DBZ-1867](https://issues.redhat.com/browse/DBZ-1867)
 * MongoDB transaction metadata topic generates extra events [DBZ-1874](https://issues.redhat.com/browse/DBZ-1874)
 * NullPointerException on delete in ExtractNewRecordState class [DBZ-1876](https://issues.redhat.com/browse/DBZ-1876)
 * MongoDB connector unrecoverable exception [DBZ-1880](https://issues.redhat.com/browse/DBZ-1880)
 * High log volume from: "Awaiting end of restart backoff period" logs [DBZ-1889](https://issues.redhat.com/browse/DBZ-1889)
 * Kafka records from one Cassandra table get published to the kafka queue of another Cassandra table [DBZ-1892](https://issues.redhat.com/browse/DBZ-1892)


### Other changes since 1.1.0.CR1

 * Use snapshot versions in master branch documentation [DBZ-1793](https://issues.redhat.com/browse/DBZ-1793)
 * Misc docs issues [DBZ-1798](https://issues.redhat.com/browse/DBZ-1798)
 * Outbox Quarkus Extension: Clarify default column types when using defaults. [DBZ-1804](https://issues.redhat.com/browse/DBZ-1804)
 * Create CI job to run OpenShift test [DBZ-1817](https://issues.redhat.com/browse/DBZ-1817)
 * Failing test jobs for Mongo and SQL Server due to insecure maven registry [DBZ-1837](https://issues.redhat.com/browse/DBZ-1837)
 * Support retriable exceptions with embedded engine [DBZ-1857](https://issues.redhat.com/browse/DBZ-1857)
 * Modularize Debezium logging doc [DBZ-1861](https://issues.redhat.com/browse/DBZ-1861)
 * Centralize closing of coordinator [DBZ-1863](https://issues.redhat.com/browse/DBZ-1863)
 * Assert format of commit messages [DBZ-1868](https://issues.redhat.com/browse/DBZ-1868)
 * Bump MongoDB java driver to the latest version 3.12.2 [DBZ-1869](https://issues.redhat.com/browse/DBZ-1869)
 * Add Travis CI task for MongoDB 3.2 [DBZ-1871](https://issues.redhat.com/browse/DBZ-1871)
 * Unstable tests for PostgreSQL [DBZ-1875](https://issues.redhat.com/browse/DBZ-1875)
 * Add MongoDB JMX integration tests [DBZ-1879](https://issues.redhat.com/browse/DBZ-1879)
    
    

## 1.1.0.CR1
March 11th, 2020 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12344727)

### New features since 1.1.0.Beta2

* Distinguish between public (API) and internal parts of Debezium [DBZ-234](https://issues.jboss.org/browse/DBZ-234)
* Add option to skip unprocesseable event [DBZ-1760](https://issues.jboss.org/browse/DBZ-1760)
* ExtractNewRecordState - add.source.fields should strip spaces from comma-separated list of fields [DBZ-1772](https://issues.jboss.org/browse/DBZ-1772)
* Add support for update events for sharded MongoDB collections [DBZ-1781](https://issues.jboss.org/browse/DBZ-1781)
* Useless/meaningless parameter in function [DBZ-1805](https://issues.jboss.org/browse/DBZ-1805)
* Replace BlockEventQueue with Debezium ChangeEventQueue  [DBZ-1820](https://issues.jboss.org/browse/DBZ-1820)
* Option to configure column.propagate.source.type on a per-type basis, not per column-name basis [DBZ-1830](https://issues.jboss.org/browse/DBZ-1830)
* Support MongoDB Oplog operations as config [DBZ-1831](https://issues.jboss.org/browse/DBZ-1831)
* Add app metrics for mongodb connector to jmx [DBZ-845](https://issues.jboss.org/browse/DBZ-845)
* Provide SPI to override schema and value conversion for specific columns [DBZ-1134](https://issues.jboss.org/browse/DBZ-1134)
* Retry polling on configured exceptions [DBZ-1723](https://issues.jboss.org/browse/DBZ-1723)


### Breaking changes since 1.1.0.Beta2

* Default `gtid.new.channel.position` to earliest [DBZ-1705](https://issues.jboss.org/browse/DBZ-1705)
* Mongodb field.renames will add renamed field even when source field is missing [DBZ-1848](https://issues.jboss.org/browse/DBZ-1848)
* MySQL: Rename event.deserialization.failure.handling.mode to event.processing.failure.handling.mode [DBZ-1826](https://issues.jboss.org/browse/DBZ-1826)


### Fixes since 1.1.0.Beta2

* CDC Event Schema Doesn't Change After 2 Fields Switch Names and Places [DBZ-1694](https://issues.jboss.org/browse/DBZ-1694)
* TINYINT(1) value range restricted on snapshot. [DBZ-1773](https://issues.jboss.org/browse/DBZ-1773)
* MySQL source connector fails while parsing new AWS RDS internal event [DBZ-1775](https://issues.jboss.org/browse/DBZ-1775)
* Connector fails when performing a Hot Schema Update in SQLServer (Data row is smaller than a column index). [DBZ-1778](https://issues.jboss.org/browse/DBZ-1778)
* Incosistency in MySQL TINYINT mapping definition [DBZ-1800](https://issues.jboss.org/browse/DBZ-1800)
* Debezium skips messages after restart [DBZ-1824](https://issues.jboss.org/browse/DBZ-1824)
* Supply of message.key.columns disables primary keys. [DBZ-1825](https://issues.jboss.org/browse/DBZ-1825)
* MySql connector fails after CREATE TABLE IF NOT EXISTS table_A, given table_A does exist already [DBZ-1833](https://issues.jboss.org/browse/DBZ-1833)
* Unable to listen to binlogs for tables with a period in the table names [DBZ-1834](https://issues.jboss.org/browse/DBZ-1834)
* Redundant calls to refresh schema when using user defined types in PostgreSQL [DBZ-1849](https://issues.jboss.org/browse/DBZ-1849)
* postgres oid is too large to cast to integer [DBZ-1850](https://issues.jboss.org/browse/DBZ-1850)


### Other changes since 1.1.0.Beta2

* Verify correctness of JMX metrics [DBZ-1664](https://issues.jboss.org/browse/DBZ-1664)
* Document that server name option must not use hyphen in name [DBZ-1704](https://issues.jboss.org/browse/DBZ-1704)
* Move MongoDB connector to base framework [DBZ-1726](https://issues.jboss.org/browse/DBZ-1726)
* hstore.handling.mode docs seem inaccurate (and map shows null values) [DBZ-1758](https://issues.jboss.org/browse/DBZ-1758)
* Document transaction metadata topic name [DBZ-1779](https://issues.jboss.org/browse/DBZ-1779)
* Remove Microsoft references in Db2 connector comments [DBZ-1794](https://issues.jboss.org/browse/DBZ-1794)
* Fix link to CONTRIBUTE.md in debezium-incubator repository README.md [DBZ-1795](https://issues.jboss.org/browse/DBZ-1795)
* Invalid dependency definition in Quarkus ITs [DBZ-1799](https://issues.jboss.org/browse/DBZ-1799)
* Document MySQL boolean handling [DBZ-1801](https://issues.jboss.org/browse/DBZ-1801)
* Jackson dependency shouldn't be optional in Testcontainers module [DBZ-1803](https://issues.jboss.org/browse/DBZ-1803)
* Change Db2 configuration for faster test execution [DBZ-1809](https://issues.jboss.org/browse/DBZ-1809)
* Misleading warning message about uncommitted offsets [DBZ-1840](https://issues.jboss.org/browse/DBZ-1840)
* Missing info on DB2 connector in incubator README file [DBZ-1842](https://issues.jboss.org/browse/DBZ-1842)
* Only replace log levels if LOG_LEVEL var is set [DBZ-1843](https://issues.jboss.org/browse/DBZ-1843)
* Modularize tutorial [DBZ-1845](https://issues.jboss.org/browse/DBZ-1845)
* Modularize the monitoring doc [DBZ-1851](https://issues.jboss.org/browse/DBZ-1851)
* Remove deprecated methods from SnapshotProgressListener [DBZ-1856](https://issues.jboss.org/browse/DBZ-1856)
* Document PostgreSQL connector metrics [DBZ-1858](https://issues.jboss.org/browse/DBZ-1858)



## 1.1.0.Beta2
February 13th, 2020 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12344682)

### New features since 1.1.0.Beta1

* Add ability to insert fields from op field in ExtractNewRecordState SMT [DBZ-1452](https://issues.jboss.org/browse/DBZ-1452)
* Integrates with TestContainers project [DBZ-1722](https://issues.jboss.org/browse/DBZ-1722)


### Breaking changes since 1.1.0.Beta1

None


### Fixes since 1.1.0.Beta1

* Postgres Connector losing data on restart due to commit() being called before events produced to Kafka [DBZ-1766](https://issues.jboss.org/browse/DBZ-1766)
* DBREF fields causes SchemaParseException using New Record State Extraction SMT and Avro converter [DBZ-1767](https://issues.jboss.org/browse/DBZ-1767)


### Other changes since 1.1.0.Beta1

* Superfluous whitespace in intra-level ToC sidebar [DBZ-1668](https://issues.jboss.org/browse/DBZ-1668)
* Outbox Quarkus Extension follow-up tasks [DBZ-1711](https://issues.jboss.org/browse/DBZ-1711)
* DB2 connector follow-up tasks [DBZ-1752](https://issues.jboss.org/browse/DBZ-1752)
* Unwrap SMT demo not compatible with ES 6.1+ [DBZ-1756](https://issues.jboss.org/browse/DBZ-1756)
* Instable SQL Server test [DBZ-1764](https://issues.jboss.org/browse/DBZ-1764)
* Remove Db2 JDBC driver from assembly package [DBZ-1776](https://issues.jboss.org/browse/DBZ-1776)
* Fix PostgresConnectorIT.shouldOutputRecordsInCloudEventsFormat test [DBZ-1783](https://issues.jboss.org/browse/DBZ-1783)
* Use "application/avro" as data content type in CloudEvents [DBZ-1784](https://issues.jboss.org/browse/DBZ-1784)
* Update Standard Tutorials/Examples with DB2 [DBZ-1558](https://issues.jboss.org/browse/DBZ-1558)



## 1.1.0.Beta1
February 5th, 2020 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12344479)

### New features since 1.1.0.Alpha1

* Create a plug-in for DB2 streaming [DBZ-695](https://issues.jboss.org/browse/DBZ-695)
* Add topic routing by field option for New Record State Extraction [DBZ-1715](https://issues.jboss.org/browse/DBZ-1715)
* Generate date(time) field types in the Kafka Connect data structure [DBZ-1717](https://issues.jboss.org/browse/DBZ-1717)
* Publish TX boundary markers on a TX metadata topic [DBZ-1052](https://issues.jboss.org/browse/DBZ-1052)
* Replace connectorName with kafkaTopicPrefix in kafka key/value schema [DBZ-1763](https://issues.jboss.org/browse/DBZ-1763)


### Breaking changes since 1.1.0.Alpha1

* Generate date(time) field types in the Kafka Connect data structure [DBZ-1717](https://issues.jboss.org/browse/DBZ-1717)
* Publish TX boundary markers on a TX metadata topic [DBZ-1052](https://issues.jboss.org/browse/DBZ-1052)


### Fixes since 1.1.0.Alpha1

* Connector error after adding a new not null column to table in Postgres [DBZ-1698](https://issues.jboss.org/browse/DBZ-1698)
* MySQL connector doesn't use default value of connector.port [DBZ-1712](https://issues.jboss.org/browse/DBZ-1712)
* Fix broken images in Antora and brush up AsciiDoc  [DBZ-1725](https://issues.jboss.org/browse/DBZ-1725)
* ANTLR parser cannot parse MariaDB Table DDL with TRANSACTIONAL attribute [DBZ-1733](https://issues.jboss.org/browse/DBZ-1733)
* Postgres connector does not support proxied connections [DBZ-1738](https://issues.jboss.org/browse/DBZ-1738)
* GET DIAGNOSTICS statement not parseable [DBZ-1740](https://issues.jboss.org/browse/DBZ-1740)
* Examples use http access to Maven repos which is no longer available [DBZ-1741](https://issues.jboss.org/browse/DBZ-1741)
* MySql password logged out in debug log level [DBZ-1748](https://issues.jboss.org/browse/DBZ-1748)
* Cannot shutdown PostgreSQL if there is an active Debezium connector [DBZ-1727](https://issues.jboss.org/browse/DBZ-1727)


### Other changes since 1.1.0.Alpha1

* Add tests for using fallback values with default REPLICA IDENTITY [DBZ-1158](https://issues.jboss.org/browse/DBZ-1158)
* Migrate all attribute name/value pairs to Antora component descriptors [DBZ-1687](https://issues.jboss.org/browse/DBZ-1687)
* Upgrade to Awestruct 0.6.0 [DBZ-1719](https://issues.jboss.org/browse/DBZ-1719)
* Run CI tests for delivered non-connector modules (like Quarkus) [DBZ-1724](https://issues.jboss.org/browse/DBZ-1724)
* Remove overlap of different documentation config files [DBZ-1729](https://issues.jboss.org/browse/DBZ-1729)
* Don't fail upon receiving unkown operation events [DBZ-1747](https://issues.jboss.org/browse/DBZ-1747)
* Provide a method to identify an envelope schema [DBZ-1751](https://issues.jboss.org/browse/DBZ-1751)
* Upgrade to Mongo Java Driver version 3.12.1 [DBZ-1761](https://issues.jboss.org/browse/DBZ-1761)
* Create initial Proposal for DB2 Source Connector [DBZ-1509](https://issues.jboss.org/browse/DBZ-1509)
* Review Pull Request for DB2 Connector [DBZ-1527](https://issues.jboss.org/browse/DBZ-1527)
* Test Set up of the DB2 Test Instance [DBZ-1556](https://issues.jboss.org/browse/DBZ-1556)
* Create Documentation for the DB2 Connector [DBZ-1557](https://issues.jboss.org/browse/DBZ-1557)
* Verify support of all DB2 types [DBZ-1708](https://issues.jboss.org/browse/DBZ-1708)



## 1.1.0.Alpha1
January 16th, 2020 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12344080)

### New features since 1.0.0.Final

* MongoDB authentication against non-admin authsource [DBZ-1168](https://issues.jboss.org/browse/DBZ-1168)
* Oracle: Add support for different representations of "NUMBER" Data Type [DBZ-1552](https://issues.jboss.org/browse/DBZ-1552)
* Update Mongo Java driver to version 3.12.0 [DBZ-1690](https://issues.jboss.org/browse/DBZ-1690)
* Support exporting change events in "CloudEvents" format [DBZ-1292](https://issues.jboss.org/browse/DBZ-1292)
* Build Quarkus extension facilitating implementations of the outbox pattern [DBZ-1478](https://issues.jboss.org/browse/DBZ-1478)
* Support column masking option for Postgres [DBZ-1685](https://issues.jboss.org/browse/DBZ-1685)


### Breaking changes since 1.0.0.Final

* Remove "slot.drop_on_stop" option [DBZ-1600](https://issues.jboss.org/browse/DBZ-1600)
* Outbox event router should ensure record timestamp is always millis-since-epoch [DBZ-1707](https://issues.jboss.org/browse/DBZ-1707)


### Fixes since 1.0.0.Final

* Make slot creation in PostgreSQL more resilient [DBZ-1684](https://issues.jboss.org/browse/DBZ-1684)
* SQLserver type time(4)...time(7) lost nanoseconds [DBZ-1688](https://issues.jboss.org/browse/DBZ-1688)
* Support boolean as default for INT(1) column in MySQL [DBZ-1689](https://issues.jboss.org/browse/DBZ-1689)
* SIGNAL statement is not recognized by DDL parser [DBZ-1691](https://issues.jboss.org/browse/DBZ-1691)
* When using in embedded mode MYSQL connector fails [DBZ-1693](https://issues.jboss.org/browse/DBZ-1693)
* MySQL connector fails to parse trigger DDL [DBZ-1699](https://issues.jboss.org/browse/DBZ-1699)


### Other changes since 1.0.0.Final

* Update outbox routing example [DBZ-1673](https://issues.jboss.org/browse/DBZ-1673)
* Add option to JSON change event SerDe for ignoring unknown properties [DBZ-1703](https://issues.jboss.org/browse/DBZ-1703)
* Update debezium/awestruct image to use Antora 2.3 alpha 2 [DBZ-1713](https://issues.jboss.org/browse/DBZ-1713)



## 1.0.0.Final
December 18th, 2019 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12343667)

### New features since 1.0.0.CR1

* Support streaming changes from SQL Server "AlwaysOn" replica [DBZ-1642](https://issues.jboss.org/browse/DBZ-1642)


### Breaking changes since 1.0.0.CR1

* Rename Serdes to DebeziumSerdes [DBZ-1670](https://issues.jboss.org/browse/DBZ-1670)
* MySQL Connector should use  "snapshot.lock.timeout.ms" [DBZ-1671](https://issues.jboss.org/browse/DBZ-1671)


### Fixes since 1.0.0.CR1

* Interpret Sql Server timestamp timezone correctly [DBZ-1643](https://issues.jboss.org/browse/DBZ-1643)
* Sorting a HashSet only to put it back into a HashSet [DBZ-1650](https://issues.jboss.org/browse/DBZ-1650)
* Function with RETURN only statement cannot be parsed [DBZ-1659](https://issues.jboss.org/browse/DBZ-1659)
* Enum value resolution not working while streaming with wal2json or pgoutput [DBZ-1680](https://issues.jboss.org/browse/DBZ-1680)


### Other changes since 1.0.0.CR1

* Globally ensure in tests that records can be serialized [DBZ-824](https://issues.jboss.org/browse/DBZ-824)
* Allow upstream teststuite to run with productised dependencies [DBZ-1658](https://issues.jboss.org/browse/DBZ-1658)
* Upgrade to latest PostgreSQL driver 42.2.9 [DBZ-1660](https://issues.jboss.org/browse/DBZ-1660)
* Generate warning for connectors with automatically dropped slots [DBZ-1666](https://issues.jboss.org/browse/DBZ-1666)
* Regression test for MySQL dates in snapshot being off by one  [DBZ-1667](https://issues.jboss.org/browse/DBZ-1667)
* Build against Apache Kafka 2.4 [DBZ-1676](https://issues.jboss.org/browse/DBZ-1676)
* When PostgreSQL schema refresh fails, allow error to include root cause [DBZ-1677](https://issues.jboss.org/browse/DBZ-1677)
* Prepare testsuite for RHEL 8 protobuf plugin RPM [DBZ-1536](https://issues.jboss.org/browse/DBZ-1536)



## 1.0.0.CR1
December 14th, 2019 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12343169)

### New features since 1.0.0.Beta3

* Transaction level TRANSACTION_READ_COMMITTED not implemented [DBZ-1480](https://issues.jboss.org/browse/DBZ-1480)
* Provide change event JSON Serde for Kafka Streams [DBZ-1533](https://issues.jboss.org/browse/DBZ-1533)
* Provide MongoDB 4.2 image [DBZ-1626](https://issues.jboss.org/browse/DBZ-1626)
* Support PostgreSQL enum types [DBZ-920](https://issues.jboss.org/browse/DBZ-920)
* Upgrade container images to Java 11 [DBZ-969](https://issues.jboss.org/browse/DBZ-969)
* Support MongoDB 4.0 transaction [DBZ-1215](https://issues.jboss.org/browse/DBZ-1215)
* Make connection timeout configurable in MySQL connection URL [DBZ-1632](https://issues.jboss.org/browse/DBZ-1632)
* Support for arrays of uuid [DBZ-1637](https://issues.jboss.org/browse/DBZ-1637)
* Add test matrix for SQL Server [DBZ-1644](https://issues.jboss.org/browse/DBZ-1644)


### Breaking changes since 1.0.0.Beta3

* Consolidate configuration parameters [DBZ-585](https://issues.jboss.org/browse/DBZ-585)


### Fixes since 1.0.0.Beta3

* Empty history topic treated as not existing [DBZ-1201](https://issues.jboss.org/browse/DBZ-1201)
* Incorrect handling of type alias [DBZ-1413](https://issues.jboss.org/browse/DBZ-1413)
* Blacklisted columns are not being filtered out when generating a Kafka message from a CDC event [DBZ-1617](https://issues.jboss.org/browse/DBZ-1617)
* IoUtil Bugfix [DBZ-1621](https://issues.jboss.org/browse/DBZ-1621)
* VariableLatch Bugfix [DBZ-1622](https://issues.jboss.org/browse/DBZ-1622)
* The oracle connector scans too many objects while attempting to determine the most recent ddl time [DBZ-1631](https://issues.jboss.org/browse/DBZ-1631)
* Connector does not update its state correctly when processing compound ALTER statement [DBZ-1645](https://issues.jboss.org/browse/DBZ-1645)
* Outbox event router shouldn't lower-case topic names [DBZ-1648](https://issues.jboss.org/browse/DBZ-1648)


### Other changes since 1.0.0.Beta3

* Merge the code for upscaling decimal values with scale lower than defined [DBZ-825](https://issues.jboss.org/browse/DBZ-825)
* Make Debezium project Java 11 compatible [DBZ-1402](https://issues.jboss.org/browse/DBZ-1402)
* Run SourceClear [DBZ-1602](https://issues.jboss.org/browse/DBZ-1602)
* Extend MySQL to test Enum with column.propagate.source.type [DBZ-1636](https://issues.jboss.org/browse/DBZ-1636)
* Sticky ToC hides tables in PG connector docs [DBZ-1652](https://issues.jboss.org/browse/DBZ-1652)
* Antora generates build warning  [DBZ-1654](https://issues.jboss.org/browse/DBZ-1654)



## 1.0.0.Beta3
November 14th, 2019 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12343094)

### New features since 1.0.0.Beta2

* Standardize source info for Cassandra connector [DBZ-1408](https://issues.jboss.org/browse/DBZ-1408)
* Clarify presence of old values when not using REPLICA IDENTITY FULL [DBZ-1518](https://issues.jboss.org/browse/DBZ-1518)
* Propagate replicator exception so failure reason is available from Connect [DBZ-1583](https://issues.jboss.org/browse/DBZ-1583)
* Envelope methods should accept Instant instead of long for "ts" parameter [DBZ-1607](https://issues.jboss.org/browse/DBZ-1607)


### Breaking changes since 1.0.0.Beta2

* Rename drop_on_stop to drop.on.stop [DBZ-1595](https://issues.jboss.org/browse/DBZ-1595)


### Fixes since 1.0.0.Beta2

* Debezium Erroneously Reporting No Tables to Capture [DBZ-1519](https://issues.jboss.org/browse/DBZ-1519)
* Debezium Oracle connector attempting to analyze tables [DBZ-1569](https://issues.jboss.org/browse/DBZ-1569)
* Null values in "before" are populated with "__debezium_unavailable_value" [DBZ-1570](https://issues.jboss.org/browse/DBZ-1570)
* Postgresql 11+ pgoutput plugin error with truncate [DBZ-1576](https://issues.jboss.org/browse/DBZ-1576)
* Regression of postgres Connector times out in schema discovery for DBs with many tables [DBZ-1579](https://issues.jboss.org/browse/DBZ-1579)
* The ts_ms value is not correct during the snapshot processing [DBZ-1588](https://issues.jboss.org/browse/DBZ-1588)
* LogInterceptor is not thread-safe [DBZ-1590](https://issues.jboss.org/browse/DBZ-1590)
* Heartbeats are not generated for non-whitelisted tables [DBZ-1592](https://issues.jboss.org/browse/DBZ-1592)
* Config `tombstones.on.delete` is missing from SQL Server Connector configDef [DBZ-1593](https://issues.jboss.org/browse/DBZ-1593)
* AWS RDS Performance Insights screwed a little by non-closed statement in "SELECT COUNT(1) FROM pg_publication" [DBZ-1596](https://issues.jboss.org/browse/DBZ-1596)
* Update Postgres documentation to use ts_ms instead of ts_usec [DBZ-1610](https://issues.jboss.org/browse/DBZ-1610)
* Exception while trying snapshot schema of non-whitelisted table [DBZ-1613](https://issues.jboss.org/browse/DBZ-1613)


### Other changes since 1.0.0.Beta2

* Auto-format source code upon build [DBZ-1392](https://issues.jboss.org/browse/DBZ-1392)
* Update documentation based on Technology Preview [DBZ-1543](https://issues.jboss.org/browse/DBZ-1543)
* Reduce size of Postgres container images [DBZ-1549](https://issues.jboss.org/browse/DBZ-1549)
* Debezium should not use SHARE UPDATE EXCLUSIVE MODE locks [DBZ-1559](https://issues.jboss.org/browse/DBZ-1559)
* Allows tags to be passed to CI jobs [DBZ-1578](https://issues.jboss.org/browse/DBZ-1578)
* Upgrade MongoDB driver to 3.11 [DBZ-1597](https://issues.jboss.org/browse/DBZ-1597)
* Run formatter validation in Travis CI [DBZ-1603](https://issues.jboss.org/browse/DBZ-1603)
* Place formatting rules into Maven module [DBZ-1605](https://issues.jboss.org/browse/DBZ-1605)
* Upgrade to Kafka 2.3.1 [DBZ-1612](https://issues.jboss.org/browse/DBZ-1612)
* Allow per-connector setting for schema/catalog precedence in TableId use [DBZ-1555](https://issues.jboss.org/browse/DBZ-1555)



## 1.0.0.Beta2
October 24th, 2019 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12343067)

### New features since 1.0.0.Beta1

* Update tooling image to use latest kafkacat [DBZ-1522](https://issues.jboss.org/browse/DBZ-1522)
* Validate configured replication slot names [DBZ-1525](https://issues.jboss.org/browse/DBZ-1525)
* Make password field to be hidden for MS SQL connector [DBZ-1554](https://issues.jboss.org/browse/DBZ-1554)
* Raise a warning about growing backlog [DBZ-1565](https://issues.jboss.org/browse/DBZ-1565)
* Support Postgres LTREE columns [DBZ-1336](https://issues.jboss.org/browse/DBZ-1336)


### Breaking changes since 1.0.0.Beta1

None


### Fixes since 1.0.0.Beta1


* Aborting snapshot due to error when last running 'UNLOCK TABLES': Only REPEATABLE READ isolation level is supported for START TRANSACTION WITH CONSISTENT SNAPSHOT in RocksDB Storage Engine. [DBZ-1428](https://issues.jboss.org/browse/DBZ-1428)
* MySQL Connector fails to parse DDL containing the keyword VISIBLE for index definitions [DBZ-1534](https://issues.jboss.org/browse/DBZ-1534)
* MySQL connector fails to parse DDL - GRANT SESSION_VARIABLES_ADMIN... [DBZ-1535](https://issues.jboss.org/browse/DBZ-1535)
* Mysql connector: The primary key cannot reference a non-existant column 'id' in table '***' [DBZ-1560](https://issues.jboss.org/browse/DBZ-1560)
* Incorrect source struct's collection field when dot is present in collection name [DBZ-1563](https://issues.jboss.org/browse/DBZ-1563)
* Transaction left open after db snapshot [DBZ-1564](https://issues.jboss.org/browse/DBZ-1564)


### Other changes since 1.0.0.Beta1

* Add Postgres 12 to testing matrix [DBZ-1542](https://issues.jboss.org/browse/DBZ-1542)
* Update Katacoda learning experience [DBZ-1548](https://issues.jboss.org/browse/DBZ-1548)



## 1.0.0.Beta1
October 17th, 2019 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12341896)

### New features since 0.10.0.Final

* Provide alternative mapping for INTERVAL [DBZ-1498](https://issues.jboss.org/browse/DBZ-1498)
* Ensure message keys have correct field order [DBZ-1507](https://issues.jboss.org/browse/DBZ-1507)
* Image incorrect on Deploying Debezium on OpenShift [DBZ-1545](https://issues.jboss.org/browse/DBZ-1545)
* Indicate table locking issues in log [DBZ-1280](https://issues.jboss.org/browse/DBZ-1280)


### Breaking changes since 0.10.0.Final

The ExtractNewDocumentState and EventRouter SMTs now propagate any heartbeat or schema change messages unchanged instead of dropping them as before. This is to ensure consistency with the ExtractNewRecordState SMT ([DBZ-1513](https://issues.jboss.org/browse/DBZ-1513)).

The new Postgres connector option `interval.handling.mode` allows to control whether `INTERVAL` columns should be exported as microseconds (previous behavior, remains the default) or as ISO 8601 formatted string ([DBZ-1498](https://issues.jboss.org/browse/DBZ-1498)). The following upgrade order must be maintained when existing connectors capture `INTERVAL` columns:

1. Upgrade the Debezium Kafka Connect Postgres connector
2. Upgrade the logical decoding plug-in installed in the database
3. (Optionally) switch `interval.handling.mode` to string

In particular it should be avoided to upgrade the logical decoding plug-in before the connector, as this will cause no value to be exported for `INTERVAL` columns.


### Fixes since 0.10.0.Final

* Debezium fails to snapshot large databases [DBZ-685](https://issues.jboss.org/browse/DBZ-685)
* Connector Postgres runs out of disk space [DBZ-892](https://issues.jboss.org/browse/DBZ-892)
* Debezium-MySQL Connector Fails while parsing AWS RDS internal events [DBZ-1492](https://issues.jboss.org/browse/DBZ-1492)
* MongoDB ExtractNewDocumentState SMT blocks heartbeat messages [DBZ-1513](https://issues.jboss.org/browse/DBZ-1513)
* pgoutput string decoding depends on JVM default charset [DBZ-1532](https://issues.jboss.org/browse/DBZ-1532)
* Whitespaces not stripped from table.whitelist [DBZ-1546](https://issues.jboss.org/browse/DBZ-1546)


### Other changes since 0.10.0.Final

* Upgrade to latest JBoss Parent POM [DBZ-675](https://issues.jboss.org/browse/DBZ-675)
* CheckStyle: Flag missing whitespace [DBZ-1341](https://issues.jboss.org/browse/DBZ-1341)
* Upgrade to the latest Checkstyle plugin [DBZ-1355](https://issues.jboss.org/browse/DBZ-1355)
* CheckStyle: no code after closing braces [DBZ-1391](https://issues.jboss.org/browse/DBZ-1391)
* Add "adopters" file [DBZ-1460](https://issues.jboss.org/browse/DBZ-1460)
* Add Google Analytics to Antora-published pages [DBZ-1526](https://issues.jboss.org/browse/DBZ-1526)
* Create 0.10 RPM for postgres-decoderbufs [DBZ-1540](https://issues.jboss.org/browse/DBZ-1540)
* Postgres documentation fixes [DBZ-1544](https://issues.jboss.org/browse/DBZ-1544)



## 0.10.0.Final
October 2nd, 2019 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12339267)

### New features since 0.10.0.CR2

None


### Breaking changes since 0.10.0.CR2

None


### Fixes since 0.10.0.CR2

* Debezium Postgres replication with pgoutput plugin sending events slowly for non-batched insertions [DBZ-1515](https://issues.jboss.org/browse/DBZ-1515)
* ExtractNewRecordState access operation field before checking message format [DBZ-1517](https://issues.jboss.org/browse/DBZ-1517)


### Other changes since 0.10.0.CR2

* Go back to original PG 10 container image for testing [DBZ-1504](https://issues.jboss.org/browse/DBZ-1504)
* Support delete propagation in end-to-end demo [DBZ-1506](https://issues.jboss.org/browse/DBZ-1506)
* Update Unwrap/UnwrapMongoDB SMT demos to use latest Debezium and delete event support [DBZ-1516](https://issues.jboss.org/browse/DBZ-1516)


## 0.10.0.CR2
September 26th, 2019 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12342807)

### New features since 0.10.0.CR1

* Allow user to customize key for DB tables through configuration [DBZ-1015](https://issues.jboss.org/browse/DBZ-1015)
* Replace Custom Schema with Pluggable Serializers via KC Schema in Cassandra Connector [DBZ-1405](https://issues.jboss.org/browse/DBZ-1405)
* Porting insert fields from source struct feature to ExtractNewDocumentState SMT [DBZ-1442](https://issues.jboss.org/browse/DBZ-1442)
* Add column_id column to metadata section in messages in Kafka topic [DBZ-1483](https://issues.jboss.org/browse/DBZ-1483)


### Breaking changes since 0.10.0.CR1

* Change type of MicroDuration to int64 [DBZ-1497](https://issues.jboss.org/browse/DBZ-1497)
* Convey information about unchanged TOAST column values [DBZ-1367](https://issues.jboss.org/browse/DBZ-1367)


### Fixes since 0.10.0.CR1

* Cannot use Avro for fields with dash in name [DBZ-1044](https://issues.jboss.org/browse/DBZ-1044)
* Detection of unsupported include-unchanged-toast parameter is failing [DBZ-1399](https://issues.jboss.org/browse/DBZ-1399)
* Possible issue with Debezium not properly shutting down PG connections during Connect rebalance [DBZ-1426](https://issues.jboss.org/browse/DBZ-1426)
* Common error when PG connector cannot connect is confusing [DBZ-1427](https://issues.jboss.org/browse/DBZ-1427)
* Postgres connector does not honor `publication.name` configuration [DBZ-1436](https://issues.jboss.org/browse/DBZ-1436)
* Wrong interrupt handling [DBZ-1438](https://issues.jboss.org/browse/DBZ-1438)
* CREATE DATABASE and TABLE statements do not support DEFAULT charset [DBZ-1470](https://issues.jboss.org/browse/DBZ-1470)
* Avoid NPE at runtime in EventRouter when incorrect configuration is given. [DBZ-1495](https://issues.jboss.org/browse/DBZ-1495)
* java.time.format.DateTimeParseException: java.time.format.DateTimeParseException [DBZ-1501](https://issues.jboss.org/browse/DBZ-1501)


### Other changes since 0.10.0.CR1

* Publish container images to quay.io [DBZ-1178](https://issues.jboss.org/browse/DBZ-1178)
* Document installation of DecoderBufs plug-in via RPM on Fedora [DBZ-1286](https://issues.jboss.org/browse/DBZ-1286)
* Fix intermittendly failing Postgres tests [DBZ-1383](https://issues.jboss.org/browse/DBZ-1383)
* Add MongoDB 4.2 to testing matrix [DBZ-1389](https://issues.jboss.org/browse/DBZ-1389)
* Upgrade to latest Postgres driver [DBZ-1462](https://issues.jboss.org/browse/DBZ-1462)
* Use old SMT name in 0.9 docs [DBZ-1471](https://issues.jboss.org/browse/DBZ-1471)
* Speak of "primary" and "secondary" nodes in the Postgres docs [DBZ-1472](https://issues.jboss.org/browse/DBZ-1472)
* PostgreSQL `snapshot.mode` connector option description should include 'exported' [DBZ-1473](https://issues.jboss.org/browse/DBZ-1473)
* Update example tutorial to show using Avro configuration at connector level [DBZ-1474](https://issues.jboss.org/browse/DBZ-1474)
* Upgrade protobuf to version 3.8.0 [DBZ-1475](https://issues.jboss.org/browse/DBZ-1475)
* Logging can be confusing when using fallback replication stream methods [DBZ-1479](https://issues.jboss.org/browse/DBZ-1479)
* Remove info on when an option was introduced from the docs [DBZ-1493](https://issues.jboss.org/browse/DBZ-1493)
* Unstable Mysql connector Integration test (shouldProcessCreateUniqueIndex) [DBZ-1500](https://issues.jboss.org/browse/DBZ-1500)
* Update PostgreSQL documentation [DBZ-1503](https://issues.jboss.org/browse/DBZ-1503)
* DocumentTest#shouldCreateArrayFromValues() fails on Windows [DBZ-1508](https://issues.jboss.org/browse/DBZ-1508)


## 0.10.0.CR1
September 10th, 2019 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12342542)

### New features since 0.10.0.Beta4

* Replace YAML Dependency with Property File in Cassandra Connector [DBZ-1406](https://issues.jboss.org/browse/DBZ-1406)
* Exported snapshots are supported by PostgreSQL 9.4+ [DBZ-1440](https://issues.jboss.org/browse/DBZ-1440)
* Enhance Postgresql & Mysql Docker example images with some Spatial geometry  [DBZ-1459](https://issues.jboss.org/browse/DBZ-1459)


### Breaking changes since 0.10.0.Beta4

* Upgrade ProtoBuf dependency [DBZ-1390](https://issues.jboss.org/browse/DBZ-1390)
* Support Connect date/time precision [DBZ-1419](https://issues.jboss.org/browse/DBZ-1419)


### Fixes since 0.10.0.Beta4

* Date conversion broken if date more than 3000 year [DBZ-949](https://issues.jboss.org/browse/DBZ-949)
* Overflowed Timestamp in Postgres Connection [DBZ-1205](https://issues.jboss.org/browse/DBZ-1205)
* Debezium does not expect a year larger than 9999 [DBZ-1255](https://issues.jboss.org/browse/DBZ-1255)
* ExportedSnapshotter and InitialOnlySnapshotter should not always execute a snapshot. [DBZ-1437](https://issues.jboss.org/browse/DBZ-1437)
* Source Fields Not Present on Delete Rewrite [DBZ-1448](https://issues.jboss.org/browse/DBZ-1448)
* NPE raises when a new connector has nothing to commit [DBZ-1457](https://issues.jboss.org/browse/DBZ-1457)
* MongoDB connector throws NPE on "op=n" [DBZ-1464](https://issues.jboss.org/browse/DBZ-1464)


### Other changes since 0.10.0.Beta4

* Engine does not stop on Exception [DBZ-1431](https://issues.jboss.org/browse/DBZ-1431)
* Create "architecture" and "feature" pages [DBZ-1458](https://issues.jboss.org/browse/DBZ-1458)


## 0.10.0.Beta4
August 16th, 2019 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12342545)

### New features since 0.10.0.Beta3

* Implement a CDC connector for Apache Cassandra [DBZ-607](https://issues.jboss.org/browse/DBZ-607)
* Support "Exported Snapshots" feature for taking lockless snapshots with Postgres [DBZ-1035](https://issues.jboss.org/browse/DBZ-1035)
* Snapshot Order of tables [DBZ-1254](https://issues.jboss.org/browse/DBZ-1254)
* Add ability to insert fields from source struct in ExtractNewRecordState SMT [DBZ-1395](https://issues.jboss.org/browse/DBZ-1395)


### Breaking changes since 0.10.0.Beta3

* Unify handling of attributes in EventRouter SMT [DBZ-1385](https://issues.jboss.org/browse/DBZ-1385)


### Fixes since 0.10.0.Beta3

* Debezium for MySQL fails on GRANT DELETE ON (table) [DBZ-1411](https://issues.jboss.org/browse/DBZ-1411)
* Debezium for MySQL tries to flush a table for a database not in the database whitelist [DBZ-1414](https://issues.jboss.org/browse/DBZ-1414)
* Table scan is performed anyway even if snapshot.mode is set to initial_schema_only [DBZ-1417](https://issues.jboss.org/browse/DBZ-1417)
* SMT ExtractNewDocumentState does not support Heartbeat events [DBZ-1430](https://issues.jboss.org/browse/DBZ-1430)
* Postgres connector does not honor `publication.name` configuration [DBZ-1436](https://issues.jboss.org/browse/DBZ-1436)


### Other changes since 0.10.0.Beta3

* Issue with debezium embedded documentation [DBZ-393](https://issues.jboss.org/browse/DBZ-393)
* Refactor Postgres connector to be based on new framework classes [DBZ-777](https://issues.jboss.org/browse/DBZ-777)
* Don't obtain new connection each time when getting xmin position [DBZ-1381](https://issues.jboss.org/browse/DBZ-1381)
* DockerHub: show container specific README files [DBZ-1387](https://issues.jboss.org/browse/DBZ-1387)
* Remove unused dependencies from Cassandra connector [DBZ-1424](https://issues.jboss.org/browse/DBZ-1424)
* Simplify custom engine name parsing grammar [DBZ-1432](https://issues.jboss.org/browse/DBZ-1432)


## 0.10.0.Beta3
July 23rd, 2019 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12342463)

### New features since 0.10.0.Beta2

* Handle tables without primary keys [DBZ-916](https://issues.jboss.org/browse/DBZ-916)
* Define exposed connector metrics in MySQL [DBZ-1120](https://issues.jboss.org/browse/DBZ-1120)
* Set heartbeat interval for the binlog reader [DBZ-1338](https://issues.jboss.org/browse/DBZ-1338)
* Outbox router should skip heartbeat messages by default [DBZ-1388](https://issues.jboss.org/browse/DBZ-1388)
* Introduce number ofEventsInError metric [DBZ-1222](https://issues.jboss.org/browse/DBZ-1222)
* Add option to skip table locks when snapshotting [DBZ-1238](https://issues.jboss.org/browse/DBZ-1238)
* Explore built-in logical decoding added in Postgres 10 [DBZ-766](https://issues.jboss.org/browse/DBZ-766)
* Support deletion events in the outbox routing SMT [DBZ-1320](https://issues.jboss.org/browse/DBZ-1320)


### Breaking changes since 0.10.0.Beta2

* Improve heart beat emission for Postgres [DBZ-1363](https://issues.jboss.org/browse/DBZ-1363)


### Fixes since 0.10.0.Beta2

* Incorrect offset may be committed despite unparseable DDL statements [DBZ-599](https://issues.jboss.org/browse/DBZ-599)
* SavePoints are getting stored in history topic [DBZ-794](https://issues.jboss.org/browse/DBZ-794)
* delete message "op:d" on tables with unique combination of 2 primary keys  = (composite keys) ,  the d records are not sent  [DBZ-1180](https://issues.jboss.org/browse/DBZ-1180)
* When a MongoDB collection haven't had activity for a period of time an initial sync is triggered [DBZ-1198](https://issues.jboss.org/browse/DBZ-1198)
* Restore compatibility with Kafka 1.x [DBZ-1361](https://issues.jboss.org/browse/DBZ-1361)
* no viable alternative at input 'LOCK DEFAULT' [DBZ-1376](https://issues.jboss.org/browse/DBZ-1376)
* NullPointer Exception on getReplicationSlotInfo for Postgres [DBZ-1380](https://issues.jboss.org/browse/DBZ-1380)
* CHARSET is not supported for CAST function [DBZ-1397](https://issues.jboss.org/browse/DBZ-1397)
* Aria engine is not known by Debezium parser [DBZ-1398](https://issues.jboss.org/browse/DBZ-1398)
* Debezium does not get the first change after creating the replication slot in PostgreSQL [DBZ-1400](https://issues.jboss.org/browse/DBZ-1400)
* Built-in database filter throws NPE [DBZ-1409](https://issues.jboss.org/browse/DBZ-1409)
* Error processing RDS heartbeats [DBZ-1410](https://issues.jboss.org/browse/DBZ-1410)
* PostgreSQL Connector generates false alarm for empty password [DBZ-1379](https://issues.jboss.org/browse/DBZ-1379)


### Other changes since 0.10.0.Beta2

* Developer Preview Documentation [DBZ-1284](https://issues.jboss.org/browse/DBZ-1284)
* Expose metric for progress of DB history recovery [DBZ-1356](https://issues.jboss.org/browse/DBZ-1356)
* Upgrade to Apache Kafka 2.3 [DBZ-1358](https://issues.jboss.org/browse/DBZ-1358)
* Stabilize test executions on CI [DBZ-1362](https://issues.jboss.org/browse/DBZ-1362)
* Handling tombstone emission option consistently [DBZ-1365](https://issues.jboss.org/browse/DBZ-1365)
* Avoid creating unnecessary type metadata instances; only init once per column. [DBZ-1366](https://issues.jboss.org/browse/DBZ-1366)
* Fix tests to run more reliably on Amazon RDS [DBZ-1371](https://issues.jboss.org/browse/DBZ-1371)


## 0.10.0.Beta2
June 27th, 2019 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12342231)

### New features since 0.10.0.Beta1

* Protect against invalid configuration [DBZ-1340](https://issues.jboss.org/browse/DBZ-1340)
* Make emission of tombstone events configurable [DBZ-835](https://issues.jboss.org/browse/DBZ-835)
* Support HSTORE array types [DBZ-1337](https://issues.jboss.org/browse/DBZ-1337)


### Breaking changes since 0.10.0.Beta1

None


### Fixes since 0.10.0.Beta1

* Events for TRUNCATE TABLE not being emitted [DBZ-708](https://issues.jboss.org/browse/DBZ-708)
* Connector consumes huge amount of memory [DBZ-1065](https://issues.jboss.org/browse/DBZ-1065)
* Exception when starting the connector on Kafka Broker 0.10.1.0 [DBZ-1270](https://issues.jboss.org/browse/DBZ-1270)
* Raise warning when renaming table causes  it to be captured or not captured any longer [DBZ-1278](https://issues.jboss.org/browse/DBZ-1278)
* no viable alternative at input 'ALTER TABLE `documents` RENAME INDEX' [DBZ-1329](https://issues.jboss.org/browse/DBZ-1329)
* MySQL DDL parser - issue with triggers and NEW [DBZ-1331](https://issues.jboss.org/browse/DBZ-1331)
* MySQL DDL parser - issue with COLLATE in functions [DBZ-1332](https://issues.jboss.org/browse/DBZ-1332)
* Setting "include.unknown.datatypes" to true works for streaming but not during snapshot [DBZ-1335](https://issues.jboss.org/browse/DBZ-1335)
* PostgreSQL db with materialized view failing during snapshot [DBZ-1345](https://issues.jboss.org/browse/DBZ-1345)
* Switch RecordsStreamProducer to use non-blocking stream call [DBZ-1347](https://issues.jboss.org/browse/DBZ-1347)
* Can't parse create definition on the mysql connector [DBZ-1348](https://issues.jboss.org/browse/DBZ-1348)
* String literal should support utf8mb3 charset [DBZ-1349](https://issues.jboss.org/browse/DBZ-1349)
* NO_AUTO_CREATE_USER sql mode is not supported in MySQL 8 [DBZ-1350](https://issues.jboss.org/browse/DBZ-1350)
* Incorrect assert for invalid timestamp check in MySQL 8 [DBZ-1353](https://issues.jboss.org/browse/DBZ-1353)


### Other changes since 0.10.0.Beta1

* Add to FAQ what to do on offset flush timeout [DBZ-799](https://issues.jboss.org/browse/DBZ-799)
* Update MongoDB driver to 3.10.1 [DBZ-1333](https://issues.jboss.org/browse/DBZ-1333)
* Fix test for partitioned table snapshot [DBZ-1342](https://issues.jboss.org/browse/DBZ-1342)
* Enable PostGIS for Alpine 9.6 [DBZ-1351](https://issues.jboss.org/browse/DBZ-1351)
* Fix description for state of Snapshot [DBZ-1346](https://issues.jboss.org/browse/DBZ-1346)
* Remove unused code for alternative topic selection strategy [DBZ-1352](https://issues.jboss.org/browse/DBZ-1352)


## 0.10.0.Beta1
June 11th, 2019 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12342194)

### New features since 0.10.0.Alpha2

* Issue a warning for filters not matching any table/database [DBZ-1242](https://issues.jboss.org/browse/DBZ-1242)


### Breaking changes since 0.10.0.Alpha2

None


### Fixes since 0.10.0.Alpha2

* Multiple cdc entries with exactly the same commitLsn and changeLsn [DBZ-1152](https://issues.jboss.org/browse/DBZ-1152)
* PostGIS does not work in Alpine images [DBZ-1307](https://issues.jboss.org/browse/DBZ-1307)
* Processing MongoDB document contains UNDEFINED type causes exception with MongoDB Unwrap SMT [DBZ-1315](https://issues.jboss.org/browse/DBZ-1315)
* Partial zero date datetime/timestamp will fail snapshot [DBZ-1318](https://issues.jboss.org/browse/DBZ-1318)
* Default value set null when modify a column from nullable to not null [DBZ-1321](https://issues.jboss.org/browse/DBZ-1321)
* Out-of-order chunks don't initiate commitTime [DBZ-1323](https://issues.jboss.org/browse/DBZ-1323)
* NullPointerException when receiving noop event [DBZ-1317](https://issues.jboss.org/browse/DBZ-1317)


### Other changes since 0.10.0.Alpha2

* Describe structure of SQL Server CDC events [DBZ-1296](https://issues.jboss.org/browse/DBZ-1296)
* Upgrade to Apache Kafka 2.2.1 [DBZ-1316](https://issues.jboss.org/browse/DBZ-1316)


## 0.10.0.Alpha2
June 3rd, 2019 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12342158)

### New features since 0.10.0.Alpha1

* "source" block for MySQL schema change events should contain db and table names [DBZ-871](https://issues.jboss.org/browse/DBZ-871)
* Adhere to Dockerfile good practices [DBZ-1279](https://issues.jboss.org/browse/DBZ-1279)


### Breaking changes since 0.10.0.Alpha1

* Change snapshot source field into three state [DBZ-1295](https://issues.jboss.org/browse/DBZ-1295)


### Fixes since 0.10.0.Alpha1

* DDL that contains `user` are unparsable by antlr [DBZ-1300](https://issues.jboss.org/browse/DBZ-1300)
* Only validate history topic name for affected connectors [DBZ-1283](https://issues.jboss.org/browse/DBZ-1283)


### Other changes since 0.10.0.Alpha1

* Upgrade ZooKeeper to 3.4.14 [DBZ-1298](https://issues.jboss.org/browse/DBZ-1298)
* Upgrade Docker tooling image [DBZ-1301](https://issues.jboss.org/browse/DBZ-1301)
* Upgrade Debezium Postgres Example image to 11 [DBZ-1302](https://issues.jboss.org/browse/DBZ-1302)
* Create profile to build assemblies without drivers [DBZ-1303](https://issues.jboss.org/browse/DBZ-1303)
* Modify release pipeline to use new Dockerfiles [DBZ-1304](https://issues.jboss.org/browse/DBZ-1304)
* Add 3rd party licences [DBZ-1306](https://issues.jboss.org/browse/DBZ-1306)
* Remove unused methods from ReplicationStream [DBZ-1310](https://issues.jboss.org/browse/DBZ-1310)
* Replace Predicate<Column> with ColumnNameFilter [DBZ-1092](https://issues.jboss.org/browse/DBZ-1092)


## 0.10.0.Alpha1
May 28th, 2019 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12340285)

### New features since 0.9.5.Final

* Excessive warnings in log about column missing charset [DBZ-844](https://issues.jboss.org/browse/DBZ-844)
* Update JDBC (and Mongo) drivers to latest versions [DBZ-1273](https://issues.jboss.org/browse/DBZ-1273)
* Support snapshot SELECT overrides for SQL Server connector [DBZ-1224](https://issues.jboss.org/browse/DBZ-1224)
* Generate warning in logs if change table list is empty [DBZ-1281](https://issues.jboss.org/browse/DBZ-1281)


### Breaking changes since 0.9.5.Final

* Align field names in source info block across connectors [DBZ-596](https://issues.jboss.org/browse/DBZ-596)
* Find better name for unwrap SMT [DBZ-677](https://issues.jboss.org/browse/DBZ-677)
* SnapshotReader should honor database.history.store.only.monitored.tables.ddl [DBZ-683](https://issues.jboss.org/browse/DBZ-683)
* Remove legacy DDL parser [DBZ-736](https://issues.jboss.org/browse/DBZ-736)
* Add database, schema and table names to "source" section of records for Oracle and SQL Server [DBZ-875](https://issues.jboss.org/browse/DBZ-875)
* "source" block for MongoDB change events should contain collection names [DBZ-1175](https://issues.jboss.org/browse/DBZ-1175)
* Make NumberOfEventsSkipped metric specific to MySQL [DBZ-1209](https://issues.jboss.org/browse/DBZ-1209)
* Remove deprecated features and configuration options [DBZ-1234](https://issues.jboss.org/browse/DBZ-1234)
* Make option names of outbox routing SMT more consistent [DBZ-1289](https://issues.jboss.org/browse/DBZ-1289)


### Fixes since 0.9.5.Final

* MySQL connection with client authentication does not work [DBZ-1228](https://issues.jboss.org/browse/DBZ-1228)
* Unhandled exception prevents snapshot.mode : when_needed functioning [DBZ-1244](https://issues.jboss.org/browse/DBZ-1244)
* MySQL connector stops working with a NullPointerException error [DBZ-1246](https://issues.jboss.org/browse/DBZ-1246)
* CREATE INDEX can fail for non-monitored tables after connector restart [DBZ-1264](https://issues.jboss.org/browse/DBZ-1264)
* Create a spec file for RPM for postgres protobuf plugin [DBZ-1272](https://issues.jboss.org/browse/DBZ-1272)
* Last transaction events get duplicated on EmbeddedEngine MySQL connector restart [DBZ-1276](https://issues.jboss.org/browse/DBZ-1276)


### Other changes since 0.9.5.Final

* Clean up integration tests under integration-tests [DBZ-263](https://issues.jboss.org/browse/DBZ-263)
* Misleading description for column.mask.with.length.chars parameter [DBZ-1290](https://issues.jboss.org/browse/DBZ-1290)
* Consolidate DDL parser tests [DBZ-733](https://issues.jboss.org/browse/DBZ-733)
* Document "database.ssl.mode" option [DBZ-985](https://issues.jboss.org/browse/DBZ-985)
* Synchronize MySQL grammar with upstream grammar [DBZ-1127](https://issues.jboss.org/browse/DBZ-1127)
* Add FAQ entry about -XX:+UseStringDeduplication JVM flag [DBZ-1139](https://issues.jboss.org/browse/DBZ-1139)
* Test and handle time 24:00:00 supported by PostgreSQL [DBZ-1164](https://issues.jboss.org/browse/DBZ-1164)
* Define final record format for MySQL, Postgres, SQL Server and MongoDB [DBZ-1235](https://issues.jboss.org/browse/DBZ-1235)
* Improve error reporting in case of misaligned schema and data [DBZ-1257](https://issues.jboss.org/browse/DBZ-1257)
* Adding missing contributors to COPYRIGHT.txt [DBZ-1259](https://issues.jboss.org/browse/DBZ-1259)
* Automate contributor check during release pipeline. [DBZ-1282](https://issues.jboss.org/browse/DBZ-1282)


## 0.9.5.Final
May 2nd, 2019 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12341657)

### New features since 0.9.4.Final

* Upgrade to Kafka 2.2.0 [DBZ-1227](https://issues.jboss.org/browse/DBZ-1227)
* Ability to specify batch size during snapshot [DBZ-1247](https://issues.jboss.org/browse/DBZ-1247)
* Postgresql ARRAY support [DBZ-1076](https://issues.jboss.org/browse/DBZ-1076)
* Add support macaddr and macaddr8 PostgreSQL column types [DBZ-1193](https://issues.jboss.org/browse/DBZ-1193)


### Breaking changes since 0.9.4.Final

None


### Fixes since 0.9.4.Final

* Failing to specify value for database.server.name results in invalid Kafka topic name [DBZ-212](https://issues.jboss.org/browse/DBZ-212)
* Escape sequence handling needs to be unified [DBZ-481](https://issues.jboss.org/browse/DBZ-481)
* Postgres Connector times out in schema discovery for DBs with many tables [DBZ-1214](https://issues.jboss.org/browse/DBZ-1214)
* Oracle connector: JDBC transaction can only capture single DML record  [DBZ-1223](https://issues.jboss.org/browse/DBZ-1223)
* Enable enumeration options to contain escaped characters or commas. [DBZ-1226](https://issues.jboss.org/browse/DBZ-1226)
* Antlr parser fails on column named with MODE keyword [DBZ-1233](https://issues.jboss.org/browse/DBZ-1233)
* Lost precision for timestamp with timezone [DBZ-1236](https://issues.jboss.org/browse/DBZ-1236)
* NullpointerException due to optional value for commitTime [DBZ-1241](https://issues.jboss.org/browse/DBZ-1241)
* Default value for datetime(0) is  incorrectly handled [DBZ-1243](https://issues.jboss.org/browse/DBZ-1243)
* Postgres connector failing because empty state data is being stored in offsets topic [DBZ-1245](https://issues.jboss.org/browse/DBZ-1245)
* Default value for Bit does not work for larger values [DBZ-1249](https://issues.jboss.org/browse/DBZ-1249)
* Microsecond precision is lost when reading timetz data from Postgres. [DBZ-1260](https://issues.jboss.org/browse/DBZ-1260)


### Other changes since 0.9.4.Final

* Zookeeper image documentation does not describe txns mountpoint [DBZ-1231](https://issues.jboss.org/browse/DBZ-1231)
* Parse enum and set options with Antlr [DBZ-739](https://issues.jboss.org/browse/DBZ-739)


## 0.9.4.Final
April 11th, 2019 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12341407)

### New features since 0.9.3.Final

* Add MySQL Connector metric to expose "number of filtered events" [DBZ-1206](https://issues.jboss.org/browse/DBZ-1206)
* Support TLS 1.2 for MySQL [DBZ-1208](https://issues.jboss.org/browse/DBZ-1208)
* Create new MysqlConnector metric exposing if the connector is tracking offsets using GTIDs or not. [DBZ-1221](https://issues.jboss.org/browse/DBZ-1221)
* Add support for columns of type INET [DBZ-1189](https://issues.jboss.org/browse/DBZ-1189)


### Breaking changes since 0.9.3.Final

None


### Fixes since 0.9.3.Final

* Incorrect value for datetime field for '0001-01-01 00:00:00' [DBZ-1143](https://issues.jboss.org/browse/DBZ-1143)
* PosgreSQL DecoderBufs crash when working with geometries in "public" schema [DBZ-1144](https://issues.jboss.org/browse/DBZ-1144)
* [postgres] differing logic between snapsnot and streams for create record [DBZ-1163](https://issues.jboss.org/browse/DBZ-1163)
* Error while deserializing binlog event [DBZ-1191](https://issues.jboss.org/browse/DBZ-1191)
* MySQL connector throw an exception when captured invalid datetime [DBZ-1194](https://issues.jboss.org/browse/DBZ-1194)
* Error when alter Enum column with CHARACTER SET [DBZ-1203](https://issues.jboss.org/browse/DBZ-1203)
* Mysql: Getting ERROR `Failed due to error: connect.errors.ConnectException: For input string: "false"` [DBZ-1204](https://issues.jboss.org/browse/DBZ-1204)
* MySQL connection timeout after bootstrapping a new table [DBZ-1207](https://issues.jboss.org/browse/DBZ-1207)
* SLF4J usage issues [DBZ-1212](https://issues.jboss.org/browse/DBZ-1212)
* JDBC Connection Not Closed in MySQL Connector Snapshot Reader [DBZ-1218](https://issues.jboss.org/browse/DBZ-1218)
* Support FLOAT(p) column definition style [DBZ-1220](https://issues.jboss.org/browse/DBZ-1220)


### Other changes since 0.9.3.Final

* Add WhitespaceAfter check to Checkstyle [DBZ-362](https://issues.jboss.org/browse/DBZ-362)
* Document RDS Postgres wal_level behavior [DBZ-1219](https://issues.jboss.org/browse/DBZ-1219)


## 0.9.3.Final
March 25th, 2019 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12340751)

### New features since 0.9.2.Final

* Support Outbox SMT as part of Debezium core [DBZ-1169](https://issues.jboss.org/browse/DBZ-1169)
* Add support for partial recovery from lost slot in postgres [DBZ-1082](https://issues.jboss.org/browse/DBZ-1082)


### Breaking changes since 0.9.2.Final

None


### Fixes since 0.9.2.Final

* Postgresql Snapshot with a table that has > 8192records hangs [DBZ-1161](https://issues.jboss.org/browse/DBZ-1161)
* HStores fail to Snapshot properly  [DBZ-1162](https://issues.jboss.org/browse/DBZ-1162)
* NullPointerException When there are multiple tables in different schemas in the whitelist  [DBZ-1166](https://issues.jboss.org/browse/DBZ-1166)
* Cannot set offset.flush.interval.ms via docker entrypoint [DBZ-1167](https://issues.jboss.org/browse/DBZ-1167)
* Missing Oracle OCI library is not reported as error [DBZ-1170](https://issues.jboss.org/browse/DBZ-1170)
* RecordsStreamProducer forgets to convert commitTime from nanoseconds to microseconds [DBZ-1174](https://issues.jboss.org/browse/DBZ-1174)
* MongoDB Connector doesn't fail on invalid hosts configuration [DBZ-1177](https://issues.jboss.org/browse/DBZ-1177)
* Handle NPE errors when trying to create history topic against confluent cloud [DBZ-1179](https://issues.jboss.org/browse/DBZ-1179)
* The Postgres wal2json streaming and non-streaming decoders do not process empty events [DBZ-1181](https://issues.jboss.org/browse/DBZ-1181)
* Can't continue after snapshot is done [DBZ-1184](https://issues.jboss.org/browse/DBZ-1184)
* ParsingException for SERIAL keyword [DBZ-1185](https://issues.jboss.org/browse/DBZ-1185)
* STATS_SAMPLE_PAGES config cannot be parsed [DBZ-1186](https://issues.jboss.org/browse/DBZ-1186)
* MySQL Connector generates false alarm for empty password [DBZ-1188](https://issues.jboss.org/browse/DBZ-1188)


### Other changes since 0.9.2.Final

* Ensure no brace-less if() blocks are used in the code base [DBZ-1039](https://issues.jboss.org/browse/DBZ-1039)
* Align Oracle DDL parser code to use the same structure as MySQL [DBZ-1192](https://issues.jboss.org/browse/DBZ-1192)


## 0.9.2.Final
February 22nd, 2019 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12340752)

### New features since 0.9.1.Final

* Add snapshotting mode NEVER for MongoDB connector [DBZ-867](https://issues.jboss.org/browse/DBZ-867)
* Allow passing of arbitrary parameters when replication slot is started [DBZ-1130](https://issues.jboss.org/browse/DBZ-1130)


### Breaking changes since 0.9.1.Final

None


### Fixes since 0.9.1.Final

* Integer default value for DECIMAL column fails with Avro Converter [DBZ-1077](https://issues.jboss.org/browse/DBZ-1077)
* connect binds only to hostname interface [DBZ-1108](https://issues.jboss.org/browse/DBZ-1108)
* Connector fails to connect to binlog on connectors rebalance, throws ServerException [DBZ-1132](https://issues.jboss.org/browse/DBZ-1132)
* Fail to parse MySQL TIME with values bigger than 23:59:59.999999 [DBZ-1137](https://issues.jboss.org/browse/DBZ-1137)
* Test dependencies shouldn't be part of the SQL Server connector archive [DBZ-1138](https://issues.jboss.org/browse/DBZ-1138)
* Emit correctly-typed fallback values for replica identity DEFAULT [DBZ-1141](https://issues.jboss.org/browse/DBZ-1141)
* Unexpected exception while streaming changes from row with unchanged toast [DBZ-1146](https://issues.jboss.org/browse/DBZ-1146)
* SQL syntax error near '"gtid_purged"' [DBZ-1147](https://issues.jboss.org/browse/DBZ-1147)
* Postgres delete operations throwing DataException [DBZ-1149](https://issues.jboss.org/browse/DBZ-1149)
* Antlr parser fails on column names that are keywords [DBZ-1150](https://issues.jboss.org/browse/DBZ-1150)
* SqlServerConnector doesn't work with table names with "special characters" [DBZ-1153](https://issues.jboss.org/browse/DBZ-1153)


### Other changes since 0.9.1.Final

* Describe topic-level settings to ensure event consumption when log compaction is enabled [DBZ-1136](https://issues.jboss.org/browse/DBZ-1136)
* Upgrade binlog client to 0.19.0 [DBZ-1140](https://issues.jboss.org/browse/DBZ-1140)
* Upgrade kafkacat to 1.4.0-RC1 [DBZ-1148](https://issues.jboss.org/browse/DBZ-1148)
* Upgrade Avro connector version to 5.1.2 [DBZ-1156](https://issues.jboss.org/browse/DBZ-1156)
* Upgrade to Kafka 2.1.1 [DBZ-1157](https://issues.jboss.org/browse/DBZ-1157)


## 0.9.1.Final
February 13th, 2019 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12340576)

### New features since 0.9.0.Final

* Provide new container image with tooling for examples and demos [DBZ-1125](https://issues.jboss.org/browse/DBZ-1125)


### Breaking changes since 0.9.0.Final

None


### Fixes since 0.9.0.Final

* BigDecimal has mismatching scale value for given Decimal schema error due to permissive mysql ddl [DBZ-983](https://issues.jboss.org/browse/DBZ-983)
* Primary key changes cause UnsupportedOperationException [DBZ-997](https://issues.jboss.org/browse/DBZ-997)
* java.lang.IllegalArgumentException: timeout value is negative [DBZ-1019](https://issues.jboss.org/browse/DBZ-1019)
* Connector consumes huge amount of memory [DBZ-1065](https://issues.jboss.org/browse/DBZ-1065)
* Strings.join() doesn't apply conversation for first element [DBZ-1112](https://issues.jboss.org/browse/DBZ-1112)
* NPE if database history filename has no parent folder [DBZ-1122](https://issues.jboss.org/browse/DBZ-1122)
* Generated columns not supported by DDL parser [DBZ-1123](https://issues.jboss.org/browse/DBZ-1123)
* Advancing LSN in the first iteration - possible data loss [DBZ-1128](https://issues.jboss.org/browse/DBZ-1128)
* Incorrect LSN comparison can cause out of order processing [DBZ-1131](https://issues.jboss.org/browse/DBZ-1131)


### Other changes since 0.9.0.Final

* io.debezium.connector.postgresql.PostgisGeometry shouldn't use DatatypeConverter [DBZ-962](https://issues.jboss.org/browse/DBZ-962)
* Schema change events should be of type ALTER when table is modified [DBZ-1121](https://issues.jboss.org/browse/DBZ-1121)
* Wal2json ISODateTimeFormatTest fails with a locale other than Locale.ENGLISH [DBZ-1126](https://issues.jboss.org/browse/DBZ-1126)


### Known issues

A potential [race condition](https://github.com/shyiko/mysql-binlog-connector-java/pull/260) was identified in upstream library for MySQL's binary log processing.
The problem exhibits as the issue [DBZ-1132](https://issues.jboss.org/projects/DBZ/issues/DBZ-1132).
If you are affected by it we propose as the workaround to increase Kafka Connect configuration options `task.shutdown.graceful.timeout.ms` and `connect.rebalance.timeout.ms`.
If the problem persists please disable keepalive thread via Debezium configration option `connect.keep.alive`.



## 0.9.0.Final
February 5th, 2019 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12340275)

### New features since 0.9.0.CR1

* Expose more useful metrics and improve Grafana dashboard [DBZ-1040](https://issues.jboss.org/browse/DBZ-1040)


### Breaking changes since 0.9.0.CR1

None


### Fixes since 0.9.0.CR1

* Allow to use drop-slot-on-close option with wal2json [DBZ-1111](https://issues.jboss.org/browse/DBZ-1111)
* MySqlDdlParser does not support adding multiple partitions in a single ALTER TABLE ... ADD PARTITION statement  [DBZ-1113](https://issues.jboss.org/browse/DBZ-1113)
* Debezium fails to take a lock during snapshot [DBZ-1115](https://issues.jboss.org/browse/DBZ-1115)
* Data from Postgres partitioned table written to wrong topic during snapshot [DBZ-1118](https://issues.jboss.org/browse/DBZ-1118)


### Other changes since 0.9.0.CR1

* Clarify whether DDL parser is actually needed for SQL Server connector [DBZ-1096](https://issues.jboss.org/browse/DBZ-1096)
* Add design description to SqlServerStreamingChangeEventSource [DBZ-1097](https://issues.jboss.org/browse/DBZ-1097)
* Put out message about missing LSN at WARN level [DBZ-1116](https://issues.jboss.org/browse/DBZ-1116)


## 0.9.0.CR1
January 28th, 2019 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12340263)

### New features since 0.9.0.Beta2

* Snapshot isolation level overhaul [DBZ-947](https://issues.jboss.org/browse/DBZ-947)
* Kafka docker image - support for topic cleanup policy [DBZ-1038](https://issues.jboss.org/browse/DBZ-1038)
* Optimize sys.fn_cdc_map_lsn_to_time() calls [DBZ-1078](https://issues.jboss.org/browse/DBZ-1078)
* Fallback to restart_lsn if confirmed_flush_lsn is not found [DBZ-1081](https://issues.jboss.org/browse/DBZ-1081)
* table.whitelist option update for an existing connector doesn't work [DBZ-175](https://issues.jboss.org/browse/DBZ-175)
* EmbeddedEngine should allow for more flexible record consumption [DBZ-1080](https://issues.jboss.org/browse/DBZ-1080)
* Client-side column blacklisting in SQL Server connector [DBZ-1067](https://issues.jboss.org/browse/DBZ-1067)
* column.propagate.source.type missing scale [DBZ-1073](https://issues.jboss.org/browse/DBZ-1073)


### Breaking changes since 0.9.0.Beta2

SQL Server connector has re-worked semantics ([DBZ-1101](https://issues.jboss.org/browse/DBZ-1101)) of snapshot modes.\
SQL Server connector also adds a new field to offsets in the streaming mode ([DBZ-1090](https://issues.jboss.org/browse/DBZ-1090)) which could prevent seamless upgrading of versions. We recommend to re-register and restart the connector.\
SQL Server connector has changed the schema name of message schemas ([DBZ-1089](https://issues.jboss.org/browse/DBZ-1089)), superfluous database name has been dropped.


### Fixes since 0.9.0.Beta2

* ArrayIndexOutOfBoundsException when a column is deleted (Postgres) [DBZ-996](https://issues.jboss.org/browse/DBZ-996)
* Messages from tables without PK and with REPLICA IDENTITY FULL [DBZ-1029](https://issues.jboss.org/browse/DBZ-1029)
* Inconsistent schema name in streaming and snapshotting phase [DBZ-1051](https://issues.jboss.org/browse/DBZ-1051)
* "watch-topic" and "create-topic" commands fail [DBZ-1057](https://issues.jboss.org/browse/DBZ-1057)
* Antlr Exception: mismatched input '.' expecting {<EOF>, '--'} [DBZ-1059](https://issues.jboss.org/browse/DBZ-1059)
* MySQL JDBC Context sets the wrong truststore password [DBZ-1062](https://issues.jboss.org/browse/DBZ-1062)
* Unsigned smallint column in mysql failing due to out of range error [DBZ-1063](https://issues.jboss.org/browse/DBZ-1063)
* NULL Values are replaced by default values even in NULLABLE fields [DBZ-1064](https://issues.jboss.org/browse/DBZ-1064)
* Uninformative "Found previous offset" log [DBZ-1066](https://issues.jboss.org/browse/DBZ-1066)
* SQL Server connector does not persist LSNs in Kafka [DBZ-1069](https://issues.jboss.org/browse/DBZ-1069)
* [debezium] ERROR: option \"include-unchanged-toast\" = \"0\" is unknown [DBZ-1083](https://issues.jboss.org/browse/DBZ-1083)
* Debezium fails when consuming table without primary key with turned on topic routing [DBZ-1086](https://issues.jboss.org/browse/DBZ-1086)
* Wrong message key and event used when primary key is updated [DBZ-1088](https://issues.jboss.org/browse/DBZ-1088)
* Connect schema name is wrong for SQL Server [DBZ-1089](https://issues.jboss.org/browse/DBZ-1089)
* Incorrect LSN tracking - possible data loss [DBZ-1090](https://issues.jboss.org/browse/DBZ-1090)
* Race condition in EmbeddedEngine shutdown [DBZ-1103](https://issues.jboss.org/browse/DBZ-1103)


### Other changes since 0.9.0.Beta2

* Intermittent failures in RecordsStreamProducerIT#shouldPropagateSourceColumnTypeToSchemaParameter() [DBZ-781](https://issues.jboss.org/browse/DBZ-781)
* Assert MongoDB supported versions [DBZ-988](https://issues.jboss.org/browse/DBZ-988)
* Describe how to do DDL changes for SQL Server [DBZ-993](https://issues.jboss.org/browse/DBZ-993)
* Verify version of wal2json on RDS [DBZ-1056](https://issues.jboss.org/browse/DBZ-1056)
* Move SQL Server connector to main repo [DBZ-1084](https://issues.jboss.org/browse/DBZ-1084)
* Don't enqueue further records when connector is stopping [DBZ-1099](https://issues.jboss.org/browse/DBZ-1099)
* Race condition in SQLServer tests during snapshot phase [DBZ-1101](https://issues.jboss.org/browse/DBZ-1101)
* Remove columnNames field from TableImpl [DBZ-1105](https://issues.jboss.org/browse/DBZ-1105)
* column.propagate.source.type missing scale [DBZ-387](https://issues.jboss.org/browse/DBZ-387)
* write catch-up binlog reader [DBZ-387](https://issues.jboss.org/browse/DBZ-388)
* changes to Snapshot and Binlog readers to allow for concurrent/partial running [DBZ-387](https://issues.jboss.org/browse/DBZ-389)


## 0.9.0.Beta2
December 19th, 2018 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12339976)

### New features since 0.9.0.Beta1

* Add support for Oracle 11g [DBZ-954](https://issues.jboss.org/browse/DBZ-954)
* UnwrapFromMongoDbEnvelope refactor [DBZ-1020](https://issues.jboss.org/browse/DBZ-1020)
* Add option for dropping deletes and tombstone events to MongoDB struct recreation SMT [DBZ-563](https://issues.jboss.org/browse/DBZ-563)
* Expose "snapshot.delay.ms" option for all connectors [DBZ-966](https://issues.jboss.org/browse/DBZ-966)
* Convey original operation type when using flattening SMTs [DBZ-971](https://issues.jboss.org/browse/DBZ-971)
* Provide last event and captured tables in metrics [DBZ-978](https://issues.jboss.org/browse/DBZ-978)
* Skip MySQL BinLog Event in case of Invalid Cell Values [DBZ-1010](https://issues.jboss.org/browse/DBZ-1010)


### Breaking changes since 0.9.0.Beta1

MongoDB CDC Event Flattening transormation now by default removes deletion messages.
Previous default was to keep them.


### Fixes since 0.9.0.Beta1

* BinaryLogClient can't disconnect when adding records after shutdown has been initiated [DBZ-604](https://issues.jboss.org/browse/DBZ-604)
* UnwrapFromMongoDbEnvelope fails when encountering $unset operator [DBZ-612](https://issues.jboss.org/browse/DBZ-612)
* "no known snapshots" error when DBs rows are large [DBZ-842](https://issues.jboss.org/browse/DBZ-842)
* MongoDB connector stops processing oplog events after encountering "new primary" event [DBZ-848](https://issues.jboss.org/browse/DBZ-848)
* MySQL active-passive: brief data loss on failover when Debezium encounters new GTID channel [DBZ-923](https://issues.jboss.org/browse/DBZ-923)
* ConnectException: Only REPEATABLE READ isolation level is supported for START TRANSACTION WITH CONSISTENT SNAPSHOT in RocksDB Storage Engine [DBZ-960](https://issues.jboss.org/browse/DBZ-960)
* ConnectException during ALTER TABLE for non-whitelisted table [DBZ-977](https://issues.jboss.org/browse/DBZ-977)
* UnwrapFromMongoDbEnvelope fails when encountering full updates [DBZ-987](https://issues.jboss.org/browse/DBZ-987)
* UnwrapFromMongoDbEnvelope fails when encountering Tombstone messages [DBZ-989](https://issues.jboss.org/browse/DBZ-989)
* Postgres schema changes detection (not-null constraint) [DBZ-1000](https://issues.jboss.org/browse/DBZ-1000)
* NPE in SqlServerConnectorTask#cleanupResources() if connector failed to start [DBZ-1002](https://issues.jboss.org/browse/DBZ-1002)
* Explicitly initialize history topic in HistorizedRelationalDatabaseSchema [DBZ-1003](https://issues.jboss.org/browse/DBZ-1003)
* BinlogReader ignores GTIDs for empty database [DBZ-1005](https://issues.jboss.org/browse/DBZ-1005)
* NPE in MySqlConnectorTask.stop() [DBZ-1006](https://issues.jboss.org/browse/DBZ-1006)
* The name of captured but not whitelisted table is not logged [DBZ-1007](https://issues.jboss.org/browse/DBZ-1007)
* GTID set is not properly initialized after DB failover [DBZ-1008](https://issues.jboss.org/browse/DBZ-1008)
* Postgres Connector fails on none nullable MACADDR field during initial snapshot [DBZ-1009](https://issues.jboss.org/browse/DBZ-1009)
* Connector crashes with java.lang.NullPointerException when using multiple sinks to consume the messages [DBZ-1017](https://issues.jboss.org/browse/DBZ-1017)
* Postgres connector fails upon event of recently deleted table [DBZ-1021](https://issues.jboss.org/browse/DBZ-1021)
* ORA-46385: DML and DDL operations are not allowed on table "AUDSYS"."AUD$UNIFIED" [DBZ-1023](https://issues.jboss.org/browse/DBZ-1023)
* Postgres plugin does not signal the end of snapshot properly [DBZ-1024](https://issues.jboss.org/browse/DBZ-1024)
* MySQL Antlr runtime.NoViableAltException [DBZ-1028](https://issues.jboss.org/browse/DBZ-1028)
* Debezium 0.8.2 and 0.8.3.Final Not Available on Confluent Hub [DBZ-1030](https://issues.jboss.org/browse/DBZ-1030)
* Snapshot of tables with reserved names fails [DBZ-1031](https://issues.jboss.org/browse/DBZ-1031)
* UnwrapFromMongoDbEnvelope doesn't support operation header on tombstone messages [DBZ-1032](https://issues.jboss.org/browse/DBZ-1032)
* Mysql binlog reader lost data if restart task when last binlog event is QUERY event. [DBZ-1033](https://issues.jboss.org/browse/DBZ-1033)
* The same capture instance name is logged twice [DBZ-1047](https://issues.jboss.org/browse/DBZ-1047)


### Other changes since 0.9.0.Beta1

* MySQL 8 compatibility [DBZ-688](https://issues.jboss.org/browse/DBZ-688)
* Don't hard code list of supported MySQL storage engines in Antlr grammar [DBZ-992](https://issues.jboss.org/browse/DBZ-992)
* Provide updated KSQL example [DBZ-999](https://issues.jboss.org/browse/DBZ-999)
* Update to Kafka 2.1 [DBZ-1001](https://issues.jboss.org/browse/DBZ-1001)
* Skipt Antlr tests when tests are skipped [DBZ-1004](https://issues.jboss.org/browse/DBZ-1004)
* Fix expected records counts in MySQL tests [DBZ-1016](https://issues.jboss.org/browse/DBZ-1016)
* Cannot run tests against Kafka 1.x [DBZ-1037](https://issues.jboss.org/browse/DBZ-1037)
* Configure MySQL Matrix testing job to test with and without GTID [DBZ-1050](https://issues.jboss.org/browse/DBZ-1050)


## 0.9.0.Beta1
November 20th, 2018 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12339372)

### New features since 0.9.0.Alpha2

* Add STATUS_STORAGE_TOPIC environment variable to container images [DBZ-893](https://issues.jboss.org/browse/DBZ-893)
* Support Postgres 11 in Decoderbufs [DBZ-955](https://issues.jboss.org/browse/DBZ-955)
* Define the data directory where tests are storing their data [DBZ-963](https://issues.jboss.org/browse/DBZ-963)
* Upgrade Kafka to 2.0.1 [DBZ-979](https://issues.jboss.org/browse/DBZ-979)
* Implement unified metrics across connectors [DBZ-776](https://issues.jboss.org/browse/DBZ-776)
* Initial snapshot using snapshot isolation level [DBZ-941](https://issues.jboss.org/browse/DBZ-941)
* Add decimal.handling.mode for SQLServer Configuration [DBZ-953](https://issues.jboss.org/browse/DBZ-953)
* Support pass-through of "database." properties to JDBC driver [DBZ-964](https://issues.jboss.org/browse/DBZ-964)
* Handle changes of table definitions and tables created while streaming [DBZ-812](https://issues.jboss.org/browse/DBZ-812)


### Breaking changes since 0.9.0.Alpha2

MySQL Connector now uses Antlr parser as [the default](https://issues.jboss.org/browse/DBZ-990).


### Fixes since 0.9.0.Alpha2

* Error while parsing JSON column type for MySQL [DBZ-935](https://issues.jboss.org/browse/DBZ-935)
* wal2json CITEXT columns set to empty strings [DBZ-937](https://issues.jboss.org/browse/DBZ-937)
* Base docker image is deprecated [DBZ-939](https://issues.jboss.org/browse/DBZ-939)
* Mysql connector failed to parse add partition statement [DBZ-959](https://issues.jboss.org/browse/DBZ-959)
* PostgreSQL replication slots not updated in transactions [DBZ-965](https://issues.jboss.org/browse/DBZ-965)
* wal2json_streaming decoder does not provide the right plugin name [DBZ-970](https://issues.jboss.org/browse/DBZ-970)
* Create topics command doesn't work in Kafka docker image [DBZ-976](https://issues.jboss.org/browse/DBZ-976)
* Antlr parser: support quoted engine names in DDL [DBZ-990](https://issues.jboss.org/browse/DBZ-990)


### Other changes since 0.9.0.Alpha2

* Switch to Antlr-based parser implementation by default [DBZ-757](https://issues.jboss.org/browse/DBZ-757)
* Support RENAME column syntax from MySQL 8.0 [DBZ-780](https://issues.jboss.org/browse/DBZ-780)
* Fix documentation of 'array.encoding' option [DBZ-925](https://issues.jboss.org/browse/DBZ-925)
* Support MongoDB 4.0 [DBZ-974](https://issues.jboss.org/browse/DBZ-974)


## 0.9.0.Alpha2
October 4th, 2018 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12338766)

### New features since 0.9.0.Alpha1

* Build Alpine Linux versions of the PostgreSQL containers [DBZ-705](https://issues.jboss.org/browse/DBZ-705)
* Refactor methods to read MySQL sytem variables [DBZ-849](https://issues.jboss.org/browse/DBZ-849)
* Correct param name for excludeColumns(String fullyQualifiedTableNames) [DBZ-854](https://issues.jboss.org/browse/DBZ-854)
* Make BinlogReader#informAboutUnknownTableIfRequired() log with tableId [DBZ-855](https://issues.jboss.org/browse/DBZ-855)
* MySQL identifier with dot or space could not be parsed [DBZ-878](https://issues.jboss.org/browse/DBZ-878)
* Use postgres:10 instead of postgres:10.0 as base docker image [DBZ-929](https://issues.jboss.org/browse/DBZ-929)
* Support temporary replication slots with Postgres >= 10 [DBZ-934](https://issues.jboss.org/browse/DBZ-934)
* Support white/black-listing Mongo fields [DBZ-633](https://issues.jboss.org/browse/DBZ-633)
* Postgres connector - add database, schema and table names to "source" section of records [DBZ-866](https://issues.jboss.org/browse/DBZ-866)
* Support renaming Mongo fields [DBZ-881](https://issues.jboss.org/browse/DBZ-881)
* use tcpKeepAlive by default [DBZ-895](https://issues.jboss.org/browse/DBZ-895)
* Hstore support in Postgresql-connector [DBZ-898](https://issues.jboss.org/browse/DBZ-898)
* Add connector type to source info [DBZ-918](https://issues.jboss.org/browse/DBZ-918)


### Breaking changes since 0.9.0.Alpha1

MySQL JDBC driver was [upgraded](https://issues.jboss.org/browse/DBZ-763) to version 8.x.
Kafka has been [upgraded](https://issues.jboss.org/browse/DBZ-858) to version 2.0.0.


### Fixes since 0.9.0.Alpha1

* Global read lock not release when exception raised during snapshot [DBZ-769](https://issues.jboss.org/browse/DBZ-769)
* Abort loops in MongoPrimary#execute() if the connector is stopped [DBZ-784](https://issues.jboss.org/browse/DBZ-784)
* Initial synchronization is not interrupted [DBZ-838](https://issues.jboss.org/browse/DBZ-838)
* Kafka database history miscounting attempts even if there are more database history records to consume [DBZ-853](https://issues.jboss.org/browse/DBZ-853)
* Schema_only snapshot on idle server - offsets not stored after snapshot [DBZ-859](https://issues.jboss.org/browse/DBZ-859)
* DDL parsing in MySQL - default value of primary key is set to null [DBZ-860](https://issues.jboss.org/browse/DBZ-860)
* Antlr DDL parser exception for "create database ... CHARSET=..." [DBZ-864](https://issues.jboss.org/browse/DBZ-864)
* Error when MongoDB collection contains characters not compatible with kafka topic naming [DBZ-865](https://issues.jboss.org/browse/DBZ-865)
* AlterTableParserListener does not remove column definition listeners [DBZ-869](https://issues.jboss.org/browse/DBZ-869)
* MySQL parser does not recognize 0 as default value for date/time [DBZ-870](https://issues.jboss.org/browse/DBZ-870)
* Antlr parser ignores table whitelist filter [DBZ-872](https://issues.jboss.org/browse/DBZ-872)
* A new column might not be added with ALTER TABLE antlr parser [DBZ-877](https://issues.jboss.org/browse/DBZ-877)
* MySQLConnectorTask always reports it has the required Binlog file from MySQL [DBZ-880](https://issues.jboss.org/browse/DBZ-880)
* Execution of RecordsStreamProducer.closeConnections() is susceptible to race condition [DBZ-887](https://issues.jboss.org/browse/DBZ-887)
* Watch-topic command in docker image uses unsupported parameter [DBZ-890](https://issues.jboss.org/browse/DBZ-890)
* SQLServer should use only schema and table name in table naming [DBZ-894](https://issues.jboss.org/browse/DBZ-894)
* Prevent resending of duplicate change events after restart [DBZ-897](https://issues.jboss.org/browse/DBZ-897)
* PostgresConnection.initTypeRegistry() takes ~24 mins [DBZ-899](https://issues.jboss.org/browse/DBZ-899)
* java.time.format.DateTimeParseException: Text '1970-01-01 00:00:00' in mysql ALTER [DBZ-901](https://issues.jboss.org/browse/DBZ-901)
* org.antlr.v4.runtime.NoViableAltException on CREATE DEFINER=`web`@`%` PROCEDURE `... [DBZ-903](https://issues.jboss.org/browse/DBZ-903)
* MySQL default port is wrong in tutorial link [DBZ-904](https://issues.jboss.org/browse/DBZ-904)
* RecordsStreamProducer should report refresh of the schema due to different column count [DBZ-907](https://issues.jboss.org/browse/DBZ-907)
* MongoDbConnector returns obsolete config values during validation [DBZ-908](https://issues.jboss.org/browse/DBZ-908)
* Can't parse create definition on the mysql connector [DBZ-910](https://issues.jboss.org/browse/DBZ-910)
* RecordsStreamProducer#columnValues() does not take into account unchanged TOASTed columns, refreshing table schemas unnecessarily [DBZ-911](https://issues.jboss.org/browse/DBZ-911)
* Wrong type in timeout call for Central wait release [DBZ-914](https://issues.jboss.org/browse/DBZ-914)
* Exception while parsing table schema with invalid default value for timestamp field [DBZ-927](https://issues.jboss.org/browse/DBZ-927)
* Discard null fields in MongoDB event flattening SMT [DBZ-928](https://issues.jboss.org/browse/DBZ-928)


### Other changes since 0.9.0.Alpha1

* Create Travis CI build for debezium-incubator repository [DBZ-817](https://issues.jboss.org/browse/DBZ-817)
* Cache prepared statements in JdbcConnection [DBZ-819](https://issues.jboss.org/browse/DBZ-819)
* Upgrade to Kafka 2.0.0 [DBZ-858](https://issues.jboss.org/browse/DBZ-858)
* Upgrad SQL Server image to CU9 GDR2 release [DBZ-873](https://issues.jboss.org/browse/DBZ-873)
* Speed-up Travis builds using parallel build [DBZ-874](https://issues.jboss.org/browse/DBZ-874)
* Add version format check into the release pipeline [DBZ-884](https://issues.jboss.org/browse/DBZ-884)
* Handle non-complete list of plugins [DBZ-885](https://issues.jboss.org/browse/DBZ-885)
* Parametrize wait time for Maven central sync [DBZ-889](https://issues.jboss.org/browse/DBZ-889)
* Assert non-empty release in release script [DBZ-891](https://issues.jboss.org/browse/DBZ-891)
* Upgrade Postgres driver to 42.2.5 [DBZ-912](https://issues.jboss.org/browse/DBZ-912)
* Upgrade MySQL JDBC driver to version 8.0.x [DBZ-763](https://issues.jboss.org/browse/DBZ-763)
* Upgrade MySQL binlog connector [DBZ-764](https://issues.jboss.org/browse/DBZ-764)


## 0.8.3.Final
September 19th, 2018 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12339197)

### New features since 0.8.2.Final

* Ability to rewrite deleted records [DBZ-857](https://issues.jboss.org/browse/DBZ-857)
* use tcpKeepAlive by default [DBZ-895](https://issues.jboss.org/browse/DBZ-895)

### Breaking changes since 0.8.2.Final

There are no breaking changes in this release.


### Fixes since 0.8.2.Final

* Global read lock not release when exception raised during snapshot [DBZ-769](https://issues.jboss.org/browse/DBZ-769)
* Abort loops in MongoPrimary#execute() if the connector is stopped [DBZ-784](https://issues.jboss.org/browse/DBZ-784)
* GtidModeEnabled method check gtid mode will always be true [DBZ-820](https://issues.jboss.org/browse/DBZ-820)
* Sensitive vars CONNECT_CONSUMER_SASL_JAAS_CONFIG and CONNECT_PRODUCER_SASL_JAAS_CONFIG are printed to the log [DBZ-861](https://issues.jboss.org/browse/DBZ-861)
* A new replication slot waits for all concurrent transactions to finish [DBZ-862](https://issues.jboss.org/browse/DBZ-862)
* Execution of RecordsStreamProducer.closeConnections() is susceptible to race condition [DBZ-887](https://issues.jboss.org/browse/DBZ-887)
* PostgresConnection.initTypeRegistry() takes ~24 mins [DBZ-899](https://issues.jboss.org/browse/DBZ-899)
* java.time.format.DateTimeParseException: Text '1970-01-01 00:00:00' in mysql ALTER [DBZ-901](https://issues.jboss.org/browse/DBZ-901)
* org.antlr.v4.runtime.NoViableAltException on CREATE DEFINER=`web`@`%` PROCEDURE `... [DBZ-903](https://issues.jboss.org/browse/DBZ-903)
* RecordsStreamProducer should report refresh of the schema due to different column count [DBZ-907](https://issues.jboss.org/browse/DBZ-907)
* MongoDbConnector returns obsolete config values during validation [DBZ-908](https://issues.jboss.org/browse/DBZ-908)
* Can't parse create definition on the mysql connector [DBZ-910](https://issues.jboss.org/browse/DBZ-910)


### Other changes since 0.8.2.Final

None


## 0.8.2.Final
August 30th, 2018 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12338793)

### New features since 0.8.1.Final

* Postgres connector - add database, schema and table names to "source" section of records [DBZ-866](https://issues.jboss.org/browse/DBZ-866)


### Breaking changes since 0.8.1.Final

There are no breaking changes in this release.


### Fixes since 0.8.1.Final

* Initial synchronization is not interrupted [DBZ-838](https://issues.jboss.org/browse/DBZ-838)
* DDL parsing in MySQL - default value of primary key is set to null [DBZ-860](https://issues.jboss.org/browse/DBZ-860)
* Antlr DDL parser exception for "create database ... CHARSET=..." [DBZ-864](https://issues.jboss.org/browse/DBZ-864)
* Missing 0.8.1.Final tags for Zookeper and Kafka [DBZ-868](https://issues.jboss.org/browse/DBZ-868)
* AlterTableParserListener does not remove column definition listeners [DBZ-869](https://issues.jboss.org/browse/DBZ-869)
* MySQL parser does not recognize 0 as default value for date/time [DBZ-870](https://issues.jboss.org/browse/DBZ-870)
* A new column might not be added with ALTER TABLE antlr parser [DBZ-877](https://issues.jboss.org/browse/DBZ-877)
* MySQLConnectorTask always reports it has the required Binlog file from MySQL [DBZ-880](https://issues.jboss.org/browse/DBZ-880)


### Other changes since 0.8.1.Final

None


## 0.9.0.Alpha1
July 26th, 2018 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12338152)

### New features since 0.8.1.Final

* Ingest change data from SQL Server databases [DBZ-40](https://issues.jboss.org/browse/DBZ-40)
* Oracle connector implementation cont'd (initial snapshotting etc.) [DBZ-716](https://issues.jboss.org/browse/DBZ-716)
* Implement initial snapshotting for Oracle [DBZ-720](https://issues.jboss.org/browse/DBZ-720)
* Implement capturing of streamed changes for SQL Server[DBZ-787](https://issues.jboss.org/browse/DBZ-787)
* Implement initial snapshotting for SQL Server [DBZ-788](https://issues.jboss.org/browse/DBZ-788)
* Emit NUMBER columns as Int32/Int64 if precision and scale allow [DBZ-804](https://issues.jboss.org/browse/DBZ-804)
* Support heartbeat messages for Oracle [DBZ-815](https://issues.jboss.org/browse/DBZ-815)
* Upgrade to Kafka 1.1.1 [DBZ-829](https://issues.jboss.org/browse/DBZ-829)

### Breaking changes since 0.8.1.Final

The Oracle connector was storing event timestamp in the `source` block in field `ts_sec`. The time stamp is in fact measured in milliseconds to so the field was [renamed](https://issues.jboss.org/browse/DBZ-795) to `ts_ms`.


### Fixes since 0.8.1.Final

* Offset remains with "snapshot" set to true after completing schema only snapshot [DBZ-803](https://issues.jboss.org/browse/DBZ-803)
* Misleading timestamp field name [DBZ-795](https://issues.jboss.org/browse/DBZ-795)
* Adjust scale of decimal values to column's scale if present [DBZ-818](https://issues.jboss.org/browse/DBZ-818)
* Avoid NPE if commit is called before any offset is prepared [DBZ-826](https://issues.jboss.org/browse/DBZ-826)


### Other changes since 0.8.1.Final

* Make DatabaseHistory set-up code re-usable [DBZ-816](https://issues.jboss.org/browse/DBZ-816)
* Use TableFilter contract instead of Predicate<TableId> [DBZ-793](https://issues.jboss.org/browse/DBZ-793)
* Expand SourceInfo [DBZ-719](https://issues.jboss.org/browse/DBZ-719)
* Provide Maven module and Docker set-up [DBZ-786](https://issues.jboss.org/browse/DBZ-786)
* Avoid a few raw type warnings [DBZ-801](https://issues.jboss.org/browse/DBZ-801)


## 0.8.1.Final
July 25th, 2018 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12338169)

### New features since 0.8.0.Final

None


### Breaking changes since 0.8.0.Final

There are no breaking changes in this release.


### Fixes since 0.8.0.Final

*  PostgreSQL LSNs are not committed when receiving events for filtered-out tables [DBZ-800](https://issues.jboss.org/browse/DBZ-800)


### Other changes since 0.8.0.Final

* Extract common TopicSelector contract [DBZ-627](https://issues.jboss.org/browse/DBZ-627)
* Remove redundant Docker configuration [DBZ-796](https://issues.jboss.org/browse/DBZ-796)


## 0.8.0.Final
July 11th, 2018 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12338151)

### New features since 0.8.0.CR1

* Expose more granular snapshot metrics via JMX [DBZ-789](https://issues.jboss.org/browse/DBZ-789)


### Breaking changes since 0.8.0.CR1

The topic naming for Oracle connector has [changed](https://issues.jboss.org/browse/DBZ-725) and the database name is no longer part of the name.
The naming convention is thus consistent accross all connectors.


### Fixes since 0.8.0.CR1

None


### Other changes since 0.8.0.CR1

* Remove DB name from topic ids [DBZ-725](https://issues.jboss.org/browse/DBZ-725)
* Don't use user with DBA permissions for Oracle connector tests [DBZ-791](https://issues.jboss.org/browse/DBZ-791)


## 0.8.0.CR1
July 4th, 2018 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12338150)

### New features since 0.8.0.Beta1

* List collections only for databases accepted by database filter [DBZ-713](https://issues.jboss.org/browse/DBZ-713)
* Set DECIMAL precision as schema parameter [DBZ-751](https://issues.jboss.org/browse/DBZ-751)
* Stop MongoDB connector in case of authorization failure [DBZ-782](https://issues.jboss.org/browse/DBZ-782)
* Add the original data type of a column as schema parameter [DBZ-644](https://issues.jboss.org/browse/DBZ-644)
* Add support for columns of type CITEXT [DBZ-762](https://issues.jboss.org/browse/DBZ-762)


### Breaking changes since 0.8.0.Beta1

There are no breaking changes in this release.


### Fixes since 0.8.0.Beta1

* Allow Empty Database Passwords [DBZ-743](https://issues.jboss.org/browse/DBZ-743)
* Antlr parser raising exception for MySQL-valid ALTER TABLE [DBZ-767](https://issues.jboss.org/browse/DBZ-767)
* Timestamp format must be yyyy-mm-dd hh:mm:ss[.fffffffff] [DBZ-768](https://issues.jboss.org/browse/DBZ-768)
* Antlr parser raising exception when parsing ENUM fields [DBZ-770](https://issues.jboss.org/browse/DBZ-770)
* Default value is not removed when changing a column's type [DBZ-771](https://issues.jboss.org/browse/DBZ-771)


### Other changes since 0.8.0.Beta1

* Add documentation for supported geometry types [DBZ-573](https://issues.jboss.org/browse/DBZ-573)
* Benchmark Antlr parser [DBZ-742](https://issues.jboss.org/browse/DBZ-742)
* Document rules for "slot.name" property of the Postgres connector [DBZ-746](https://issues.jboss.org/browse/DBZ-746)
* Add table-of-contents sections to connector doc pages [DBZ-752](https://issues.jboss.org/browse/DBZ-752)
* Guard against simple bugs [DBZ-759](https://issues.jboss.org/browse/DBZ-759)
* Reduce test log output [DBZ-765](https://issues.jboss.org/browse/DBZ-765)
* Document wal2json plugin streaming mode [DBZ-772](https://issues.jboss.org/browse/DBZ-772)
* Extract common base class for relational DatabaseSchema implementations [DBZ-773](https://issues.jboss.org/browse/DBZ-773)
* Intermittent failures in ReplicationConnectionIT#shouldCloseConnectionOnInvalidSlotName() [DBZ-778](https://issues.jboss.org/browse/DBZ-778)
* Stabilize MongoDB integration test execution [DBZ-779](https://issues.jboss.org/browse/DBZ-779)


## 0.8.0.Beta1
June 21st, 2018 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12337217)

### New features since 0.7.5

* Improve MySQL connector's handling of DML / DDL statements [DBZ-252](https://issues.jboss.org/browse/DBZ-252)
* Snapshots fail if launching multiple connectors at once [DBZ-601](https://issues.jboss.org/browse/DBZ-601)
* Data-inclusive snapshot without table locks (For RDS/Aurora) [DBZ-639](https://issues.jboss.org/browse/DBZ-639)
* Enable ordered snapshotting of data-Mysql Connector [DBZ-666](https://issues.jboss.org/browse/DBZ-666)
* Add a topic name configuration for the heartbeat messages [DBZ-668](https://issues.jboss.org/browse/DBZ-668)
* Mongo cursor cleanup [DBZ-672](https://issues.jboss.org/browse/DBZ-672)
* wal2json on RDS omits initial changes in streaming mode [DBZ-679](https://issues.jboss.org/browse/DBZ-679)
* Make PG_CONFIG configurable (postgres-decoderbufs) [DBZ-686](https://issues.jboss.org/browse/DBZ-686)
* Rebase Debezium to Kafka 1.1 and Confluent platform 4.1 [DBZ-687](https://issues.jboss.org/browse/DBZ-687)
* When MySQL has BINLOG_ROWS_QUERY_LOG_EVENTS enabled, include original SQL query in event. [DBZ-706](https://issues.jboss.org/browse/DBZ-706)
* Ingest change data from Oracle databases using XStream [DBZ-20](https://issues.jboss.org/browse/DBZ-20)
* Support defaults in MySQL [DBZ-191](https://issues.jboss.org/browse/DBZ-191)
* Run test suite against MongoDB 3.6 [DBZ-529](https://issues.jboss.org/browse/DBZ-529)
* Provide option to flatten structs in MongoDB unwrapping SMT [DBZ-561](https://issues.jboss.org/browse/DBZ-561)
* Allow configuration option for keep alive interval for Mysql binlog reader [DBZ-670](https://issues.jboss.org/browse/DBZ-670)
* Add support for databases with encodings other than UTF-8/16/32 [DBZ-676](https://issues.jboss.org/browse/DBZ-676)
* Provide option to specify statements to be executed upon connection creation (e.g.  connection wait timeout) [DBZ-693](https://issues.jboss.org/browse/DBZ-693)


### Breaking changes since 0.7.5

Apache Kafka was upgraded to version 1.1 ([DBZ-687](https://issues.jboss.org/browse/DBZ-687)).
Please see [upgrade documentation](http://kafka.apache.org/11/documentation.html#upgrade) for correct upgrade procedure.

Topic names for heartbeat messages followed a hard-coded naming schema.
The rules were made more flexible in [DBZ-668](https://issues.jboss.org/browse/DBZ-668).

Transaction id (`txId` field of `Envelope`) for PostgreSQL was originally encoded as an 32-bit `integer` type.
The real range is a 64-bit `long` type so this was changed in [DBZ-673](https://issues.jboss.org/browse/DBZ-673).

The datatypes without timezone were not correctly offsetted for databases running in non-UTC timezones.
This was fixed in [DBZ-587](https://issues.jboss.org/browse/DBZ-578) and [DBZ-741](https://issues.jboss.org/browse/DBZ-741).
See [MySQL](https://debezium.io/docs/connectors/mysql/#temporal-values) and [PostgreSQL](https://debezium.io/docs/connectors/postgresql/#temporal-values) connector documentation for further details.


### Fixes since 0.7.5

* Timestamps are not converted to UTC during snapshot [DBZ-578](https://issues.jboss.org/browse/DBZ-578)
* wal2json cannot handle transactions bigger than 1Gb [DBZ-638](https://issues.jboss.org/browse/DBZ-638)
* SMT - DataException with io.debezium.connector.mongodb.transforms.UnwrapFromMongoDbEnvelope [DBZ-649](https://issues.jboss.org/browse/DBZ-649)
* SchemaParseException when using UnwrapFromMongoDbEnvelope SMT with Avro format [DBZ-650](https://issues.jboss.org/browse/DBZ-650)
* Upgrade OpenShift intructions to Strimzi 0.2.0 [DBZ-654](https://issues.jboss.org/browse/DBZ-654)
* Mysql ddl parser cannot parse scientific format number in exponential notation default values [DBZ-667](https://issues.jboss.org/browse/DBZ-667)
* Close Kafka admin client after DB history topic has been created [DBZ-669](https://issues.jboss.org/browse/DBZ-669)
* Postgres DateTimeParseException [DBZ-671](https://issues.jboss.org/browse/DBZ-671)
* Transaction ID must be handled as long [DBZ-673](https://issues.jboss.org/browse/DBZ-673)
* PostgreSQL connector doesn't handle TIME(p) columns correctly with wal2json [DBZ-681](https://issues.jboss.org/browse/DBZ-681)
* Error on initial load for records with negative timestamp [DBZ-694](https://issues.jboss.org/browse/DBZ-694)
* Postgres Connector inconsistent handling of timestamp precision [DBZ-696](https://issues.jboss.org/browse/DBZ-696)
* Debezium is throwing exception when max OID in pg db is larger than max int [DBZ-697](https://issues.jboss.org/browse/DBZ-697)
* PostgresReplicationConnection doesn't close jdbc connection [DBZ-699](https://issues.jboss.org/browse/DBZ-699)
* Debezium is throwing exception when max typelem in pg db is larger than max int [DBZ-701](https://issues.jboss.org/browse/DBZ-701)
* Plaintext jaas configuration passwords logged out [DBZ-702](https://issues.jboss.org/browse/DBZ-702)
* Postgres TIME columns are always exported as nano-seconds, unlike documented [DBZ-709](https://issues.jboss.org/browse/DBZ-709)
* Incorrect options for PostgreSQL sslmode listed in documentation [DBZ-711](https://issues.jboss.org/browse/DBZ-711)
* Mongo Connector - doesn't redo initial sync after connector restart [DBZ-712](https://issues.jboss.org/browse/DBZ-712)
* NUMERIC column without scale value causes exception [DBZ-727](https://issues.jboss.org/browse/DBZ-727)
* Inconsistency in parameter names for database histy producer/consumer [DBZ-728](https://issues.jboss.org/browse/DBZ-728)
* MySQL DATETIME Value Incorrectly Snapshotted [DBZ-741](https://issues.jboss.org/browse/DBZ-741)


### Other changes since 0.7.5

* Support incubator repo in release process [DBZ-749](https://issues.jboss.org/browse/DBZ-749)
* Upgrade Postgres Docker images to wal2json 1.0 [DBZ-750](https://issues.jboss.org/browse/DBZ-750)
* Provide Maven profile so that the MySQL module test suite can be run using old and new parser [DBZ-734](https://issues.jboss.org/browse/DBZ-734)


## 0.7.5
March 20th, 2018 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12337159)

### New features since 0.7.4

* Keep SnapshotReaderMetrics bean registered after snapshot completed [DBZ-640](https://issues.jboss.org/browse/DBZ-640)
* Cache replaced topic names and shard ids in ByLogicalTableRouter SMT [DBZ-655](https://issues.jboss.org/browse/DBZ-655)
* Filter out useless commands from the history topic [DBZ-661](https://issues.jboss.org/browse/DBZ-661)
* Apache Kafka 1.0.1 updates [DBZ-647](https://issues.jboss.org/browse/DBZ-647)


### Breaking changes since 0.7.4

Debezium was creating  database history topic with an infinite time-based log retention but a broker default one for topic size log retention.
This was fixed in [DBZ-663](https://issues.jboss.org/browse/DBZ-663).
See our [blogpost](https://debezium.io/blog/2018/03/16/note-on-database-history-topic-configuration/) for more details.

Snapshot JMX metrics were removed after the snapshot was completed.
This was changed in [DBZ-640](https://issues.jboss.org/browse/DBZ-640) and the metrics are available till next connector restart.

### Fixes since 0.7.4

* io.debezium.text.ParsingException for TokuDB table [DBZ-646](https://issues.jboss.org/browse/DBZ-646)
* MongoDB connector continues to try to connect to invalid host even after deletion [DBZ-648](https://issues.jboss.org/browse/DBZ-648)
* Streaming stopped due to JsonParseException [DBZ-657](https://issues.jboss.org/browse/DBZ-657)
* 'ALTER TABLE `tbl_name` ADD CONSTRAINT UNIQUE KEY `key_name` (`colname`)' throwing exception [DBZ-660](https://issues.jboss.org/browse/DBZ-660)
* Missing setting for the automatic history topic creation [DBZ-663](https://issues.jboss.org/browse/DBZ-663)
* EmbeddedEngine passes time of last commit to policy, not time since [DBZ-665](https://issues.jboss.org/browse/DBZ-665)


### Other changes since 0.7.4

* "snapshot" attribute should be false instead of null for events based on the binlog [DBZ-592](https://issues.jboss.org/browse/DBZ-592)
* Describe limitations of wal2json version currently used on RDS [DBZ-619](https://issues.jboss.org/browse/DBZ-619)


## 0.7.4
March 7th, 2018 [Detailed release notes](https://issues.jboss.org/secure/ReleaseNote.jspa?projectId=12317320&version=12336214)

### New features since 0.7.3

* Provide MySQL snapshot mode that does not require table locks [DBZ-602](https://issues.jboss.org/browse/DBZ-602)
* Add support for columns of type "bytea" [DBZ-605](https://issues.jboss.org/browse/DBZ-605)
* Add string as an option for decimal.handling.mode [DBZ-611](https://issues.jboss.org/browse/DBZ-611)
* Support CREATE TABLE statements with PARTITION ... ENGINE=InnoDB [DBZ-641](https://issues.jboss.org/browse/DBZ-641)
* Document VariableScaleDecimal in PG connector docs [DBZ-631](https://issues.jboss.org/browse/DBZ-631)
* Propagate schema validator by passing AvroValidator instance instead of Function<String, String> [DBZ-626](https://issues.jboss.org/browse/DBZ-626)
* Move `MAX_QUEUE_SIZE`, `MAX_BATCH_SIZE` and `POLL_INTERVAL_MS` to CommonConnectorConfig [DBZ-628](https://issues.jboss.org/browse/DBZ-628)
* Unify common start-up logic across connectors [DBZ-630](https://issues.jboss.org/browse/DBZ-630)
* Removing unused code from database history classes [DBZ-632](https://issues.jboss.org/browse/DBZ-632)


### Breaking changes since 0.7.3

`NUMERIC` and geo-spatial schema types were optional regardless of database column configuration. This was fixed in [DBZ-635](https://issues.jboss.org/browse/DBZ-635).
PostgresSQL decoder plug-in now uses text to transfer decimal values insted of double - [DBZ-351](https://issues.jboss.org/browse/DBZ-351). Debezium is backward compatible with the old version. It is thus necessary first to upgrade Debezium and after that upgrade logical decoder plug-in.

### Fixes and changes since 0.7.3

* Numeric datatype is transferred with lost precision [DBZ-351](https://issues.jboss.org/browse/DBZ-351)
* Cannot Serialize NaN value(numeric field) in Postgres [DBZ-606](https://issues.jboss.org/browse/DBZ-606)
* Decimal datatype DDL issues [DBZ-615](https://issues.jboss.org/browse/DBZ-615)
* Avoid NPE if `confirmed_flush_lsn` is null [DBZ-623](https://issues.jboss.org/browse/DBZ-623)
* REAL column values are omitted if value is an exact integer [DBZ-625](https://issues.jboss.org/browse/DBZ-625)
* Fix intermittent error in BinlogReaderIT [DBZ-629](https://issues.jboss.org/browse/DBZ-629)
* Schema for NUMERIC and geo-spatial array columns shouldn't be optional by default [DBZ-635](https://issues.jboss.org/browse/DBZ-635)
* Fix typo in README of debezium/connect-base image [DBZ-636](https://issues.jboss.org/browse/DBZ-636)
* Avoid repeated creation of Envelope schema [DBZ-620](https://issues.jboss.org/browse/DBZ-620)


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

* New MongoDB connector supports capturing changes from a MongoDB replica set or a MongoDB sharded cluster. See the [documentation](https://debezium.io/docs/connectors/mongodb) for details. [DBZ-2](https://issues.jboss.org/projects/DBZ/issues/DBZ-2)

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

* MySQL connector supports *high availability* MySQL cluster topologies. See the [documentation](https://debezium.io/docs/connectors/mysql) for details. ([DBZ-37](https://issues.jboss.org/projects/DBZ/issues/DBZ-37))
* MySQL connector now by default starts by performing a *consistent snapshot* of the schema and contents of the upstream MySQL databases in its current state. See the [documentation](https://debezium.io/docs/connectors/mysql#snapshots) for details about how this works and how it impacts other database clients. ([DBZ-31](https://issues.jboss.org/projects/DBZ/issues/DBZ-31))
* MySQL connector can be configured to *exclude*, *truncate*, or *mask* specific columns in events. ([DBZ-29](https://issues.jboss.org/projects/DBZ/issues/DBZ-29))
* MySQL connector events can be serialized using the [Confluent Avro converter](http://docs.confluent.io/3.0.0/avro.html) or the JSON converter. Previously, only the JSON converter could be used. ([DBZ-29](https://issues.jboss.org/projects/DBZ/issues/DBZ-29), [DBZ-63](https://issues.jboss.org/projects/DBZ/issues/DBZ-63), [DBZ-64](https://issues.jboss.org/projects/DBZ/issues/DBZ-64))

### Backwards-incompatible changes since 0.1

* Completely redesigned the structure of event messages produced by MySQL connector and stored in Kafka topics. Events now contain an _envelope_ structure with information about the source event, the kind of operation (create/insert, update, delete, read), the time that Debezium processed the event, and the state of the row before and/or after the event. The messages written to each topic have a distinct Avro-compliant Kafka Connect schema that reflects the structure of the source table, which may vary over time independently from the schemas of all other topics. See the [documentation](https://debezium.io/docs/connectors/mysql#events) for details. This envelope structure will likely be used by future connectors. ([DBZ-50](https://issues.jboss.org/projects/DBZ/issues/DBZ-50), [DBZ-52](https://issues.jboss.org/projects/DBZ/issues/DBZ-52), [DBZ-45](https://issues.jboss.org/projects/DBZ/issues/DBZ-45), [DBZ-60](https://issues.jboss.org/projects/DBZ/issues/DBZ-60))
* MySQL connector handles deletion of a row by recording a delete event message whose value contains the state of the removed row (and other metadata), followed by a _tombstone event_ message with a null value to signal *Kafka's log compaction* that all prior messages with the same key can be garbage collected. See the [documentation](https://debezium.io/docs/connectors/mysql#events) for details. ([DBZ-44](https://issues.jboss.org/projects/DBZ/issues/DBZ-44))
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
