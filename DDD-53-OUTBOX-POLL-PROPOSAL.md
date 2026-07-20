# DDD-53: Trigger-less Outbox Polling Connector, proposal and reference implementation

This is a draft proposal, not a code contribution to the build. It adds a single
standalone document and does not touch any Maven module, pom.xml, or existing
source under this repository.

## What this is

A working reference implementation of the trigger-less detection mode
described in [DDD-53](https://github.com/debezium/debezium-design-documents/blob/main/DDD-53.md),
currently under discussion in
[debezium/debezium-design-documents#51](https://github.com/debezium/debezium-design-documents/pull/51).

The design proposes a database-agnostic way to detect row changes without a
replication slot, without WAL access, and without any trigger, connector-owned
or user-owned. Each connector instance holds an in-memory baseline of a
watched table (sorted id/checksum pairs) and periodically diffs current state
against that baseline using a sort-merge algorithm, writing detected changes
to a `debezium_outbox` table. Downstream delivery is explicitly out of scope
for the connector itself.

## Where the code lives right now

The reference implementation is a standalone repository, not yet part of this
monorepo's module structure:
[zavera/debezium-connector-outbox-poll](https://github.com/zavera/debezium-connector-outbox-poll),
tracked in [PR #1](https://github.com/zavera/debezium-connector-outbox-poll/pull/1).

It currently ships as a zero-dependency JVM agent (no Kafka Connect
dependency), since the trigger-less mode in DDD-53 has no requirement on
Kafka or the Connect framework, unlike every other connector module in this
repository. That is a real architectural difference worth surfacing early,
before any code is proposed for integration here.

## Why this PR has no code

Per `CONTRIBUTING.md`, code changes should follow discussion and consensus.
DDD-53 is still an open design discussion, so this PR intentionally holds off
on proposing any module structure, package layout, or build wiring until
there is agreement on the design and on whether/how a trigger-less,
non-Connect-based mode belongs in this repository's module structure. It is
opened as a draft to make the reference implementation and design doc
discoverable from this repository, and to invite early feedback from
maintainers on the integration question itself.

## Feedback wanted

- Does a trigger-less, non-Kafka-Connect JVM agent belong as a module in this
  monorepo at all, or should it stay a separate project that happens to write
  into a table Debezium's own connectors could also read from
- If it belongs here, what would the module boundary and package convention
  look like, given every existing connector module assumes Kafka Connect
- Any concerns with the memory model (in-memory baseline, ~15 MB per million
  watched rows, validated to about 5M rows per replica) as a supported mode
