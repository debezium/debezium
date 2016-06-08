# Change log

All notable changes are documented in this file. Release numbers follow [Semantic Versioning](http://semver.org)

## 0.2

June 8, 2016 - [Detailed release notes](https://issues.jboss.org/browse/DBZ/fixforversion/12329465)

### Added

* MySQL connector supports *high availability* MySQL cluster topologies. ([DBZ-37](https://issues.jboss.org/projects/DBZ/issues/DBZ-37))
* MySQL connector now by default starts by performing a *consistent snapshot* of the schema and contents of the upstream MySQL databases. This can be disabled if the MySQL server's binlog contains the entire history of the databases. ([DBZ-31](https://issues.jboss.org/projects/DBZ/issues/DBZ-31))
* MySQL connector can be configured to *exclude*, *truncate*, or *mask* specific columns in events. ([DBZ-29](https://issues.jboss.org/projects/DBZ/issues/DBZ-29))

### Changed

* Completely redesigned the structure of event messages produced by MySQL connector to contain information about the source event, the kind of operation (create/insert, update, delete, read), the time that Debezium processed the event, and the state of the row before and/or after the event. The messages written to each topic have a distinct Kafka Connect (and Avro) schema that incorporates the structure of the source table, which may vary over time independently from the schemas of all other topics. **This change is not backward compatible with Debezium 0.1.** Future connectors will use a very similar envelope structure. ([DBZ-50](https://issues.jboss.org/projects/DBZ/issues/DBZ-50), [DBZ-52](https://issues.jboss.org/projects/DBZ/issues/DBZ-52), [DBZ-45](https://issues.jboss.org/projects/DBZ/issues/DBZ-45), [DBZ-60](https://issues.jboss.org/projects/DBZ/issues/DBZ-60))
* MySQL connector handles deletion of a row by recording a delete event message whose value contains the state of the removed row (and other metadata), followed by a tombstone event message with a null value to signal *Kafka's log compaction* that all prior messages with the same key can be garbage collected. ([DBZ-44](https://issues.jboss.org/projects/DBZ/issues/DBZ-44))
* DDL parsing framework identifies table affected by statements. ([DBZ-38](https://issues.jboss.org/projects/DBZ/issues/DBZ-38))

### Fixed

* MySQL connector events can be serialized using JSON converter or the [Confluent Avro converter](http://docs.confluent.io/3.0.0/avro.html). ([DBZ-29](https://issues.jboss.org/projects/DBZ/issues/DBZ-29), [DBZ-63](https://issues.jboss.org/projects/DBZ/issues/DBZ-63), [DBZ-64](https://issues.jboss.org/projects/DBZ/issues/DBZ-64))
* Fixed format of MySQL connector's schema change events. ([DBZ-43](https://issues.jboss.org/projects/DBZ/issues/DBZ-43), [DBZ-55](https://issues.jboss.org/projects/DBZ/issues/DBZ-55))
* MySQL connector now properly parses additional DDL statements. ([DBZ-48](https://issues.jboss.org/projects/DBZ/issues/DBZ-48), [DBZ-49](https://issues.jboss.org/projects/DBZ/issues/DBZ-49), [DBZ-57](https://issues.jboss.org/projects/DBZ/issues/DBZ-57))
* MySQL connector properly handles binary values that are hexadecimal strings ([DBZ-61](https://issues.jboss.org/projects/DBZ/issues/DBZ-61))

## 0.1

March 17, 2016 - [Detailed release notes](https://issues.jboss.org/browse/DBZ/fixforversion/12329464)

### Added

* MySQL connector for ingesting change events from MySQL databases. ([DBZ-1](https://issues.jboss.org/projects/DBZ/issues/DBZ-1))
* Kafka Connect plugin archive for MySQL connector. ([DBZ-17](https://issues.jboss.org/projects/DBZ/issues/DBZ-17))
* Simple DDL parsing framework that can be extended and used by various connectors. ([DBZ-1](https://issues.jboss.org/projects/DBZ/issues/DBZ-1))
* Framework for embedding a single Kafka Connect connector inside an application. ([DBZ-8](https://issues.jboss.org/projects/DBZ/issues/DBZ-8))

### Changed

### Fixed
