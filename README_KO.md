[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.debezium/debezium-parent/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22io.debezium%22)
[![User chat](https://img.shields.io/badge/chat-users-brightgreen.svg)](https://debezium.zulipchat.com/#narrow/stream/302529-users)
[![Developer chat](https://img.shields.io/badge/chat-devs-brightgreen.svg)](https://debezium.zulipchat.com/#narrow/stream/302533-dev)
[![Google Group](https://img.shields.io/:mailing%20list-debezium-brightgreen.svg)](https://groups.google.com/forum/#!forum/debezium)
[![Stack Overflow](http://img.shields.io/:stack%20overflow-debezium-brightgreen.svg)](http://stackoverflow.com/questions/tagged/debezium)

Copyright Debezium Authors.
Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
The Antlr grammars within the debezium-ddl-parser module are licensed under the [MIT License](https://opensource.org/licenses/MIT).

[English](README.md) | [Chinese](README_ZH.md) | [Japanese](README_JA.md) | Korean

# Debezium
Debezium은 변경 데이터 캡처(Change Data Capture; CDC)를 위한 최소 지연 데이터 스트리밍 플랫폼을 제공하는 오픈 소스 프로젝트입니다.
데이터베이스를 모니터링하도록 Debezium을 설정하면 데이터베이스에서 수행되는 각 행 수준의 변경 사항을 응용 프로그램에서 사용할 수 있습니다.
데이터베이스에 커밋된 변경 사항만 모니터링되기에 애플리케이션에서 트랜잭션이나 롤백된 변경 사항에 대해서는 반영되지 않습니다. 그래서 애플리케이션 레벨에서의 작업에 대해 걱정할 필요가 없습니다.
