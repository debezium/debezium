# Change log

All notable changes are documented in this file. Release numbers follow [Semantic Versioning](http://semver.org)


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
