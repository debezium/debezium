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

Debezium은 Change Data Capture(CDC)를 위한 저지연 데이터 스트리밍 플랫폼을 제공하는 오픈 소스 프로젝트입니다. 데이터베이스를 모니터링 하도록 Debezium을 설정하고 구성하면 데이터베이스에서 발생하는 변경 사항에 대한 이벤트를 애플리케이션에서 사용할 수 있습니다. 커밋된 변경 사항만 보여지기에 애플리케이션은 트랜잭션이나 롤백 처리된 변경 사항에 대해서 고민할 필요가 없습니다. Debezium은 모든 변경 이벤트에 대한 단일 모델을 제공하기에 여러 종류의 데이터베이스 관리의 복잡성에 대해서 고민할 필요가 없습니다. 추가로, Debezium은 데이터 변경 내역을 영구적으로 보관하는 복제 로그에 기록하기 때문에 애플리케이션을 언제든지 재시작해도 중지된 기간 동안 누락된 모든 이벤트를 완벽하게 처리합니다.

데이터가 변경되는 것에 대해  알림을 받거나 모니터링하는 것은 복잡하고 어렵습니다. 관계형 데이터베이스의 트리거 기능을 활용할 수도 있지만, 데이터베이스마다 다르고 간혹 동일 데이터베이스내의 업데이트 상태로 제한됩니다.(외부 프로세스와 통신하지 않음) 특정 데이터베이스는 변경 사항 모니터링을 위한 API나 프레임워크를 제공하지만, 표준이 없기 때문에 각 데이터베이스마다 접근 방식이 다르고 각각 다른 코드가 필요합니다. 따라서 데이터베이스에 미치는 영향을 최소화하면서 모든 변경 사항을 동일한 순서로 확인하고 처리한다는 것은 매우 어려운 일입니다.

Debezium은 위 작업을 수행하는 모듈을 제공합니다. 일부 모듈은 여러 데이터베이스 관리 시스템과 함께 작동되지만 기능 및 성능 관점에서 제한적입니다. 다른 모듈은 특정 데이터베이스 관리 시스템에 맞게 최적화되어 훨씬 더 성능이 뛰어나고 시스템의 특정 기능을 활용하는 경우가 많습니다.

## 기본 아키텍처

Debezium은 Kafka 및 Kafka Connect를 재사용하여 내구성, 안정성 및 내결함성 품질을 달성하는 CDC(Change Data Capture) 플랫폼입니다.

확장 가능한 분산형 내결함성 서비스에 배포된 각 커넥터는 단일 업스트림 데이터베이스 서버를 모니터링하여 모든 변경 사항을 캡쳐하고 하나 이상의 Kafka 토픽(일반적으로 데이터베이스 테이블당 하나의 토픽)에 기록합니다. Kafka는 모든 데이터 변경 이벤트가 복제되고 완전히 정렬되도록 보장하며, 많은 클라이언트가 업스트림 시스템에 거의 영향을 주지 않고 데이터 변경 이벤트를 독립적으로 사용할 수 있도록 지원합니다.

또한, 클라이언트는 언제든지 소비(Consuming)를 중단할 수 있고 다시 시작하면 중단했던 위치에서 다시 시작됩니다. 각 클라이언트는 모든 데이터 변경 이벤트를 정확히 한 번 전송(exactly-once)할지 아니면 최소한 한 번 전송(at-least-once)할지 결정할 수 있으며, 각 데이터베이스/테이블의 모든 데이터 변경 이벤트는 데이터베이스에서 보낸 것과 동일한 순서로 전달됩니다.

Kafka를 통한 내결함성, 성능, 확장성 및 안정성이 필요하지 않거나 커넥터가 직접 애플리케이션으로 전송하는 것을 원한다면, 애플리케이션 내부에서 Debezium 임베디드 커넥터 엔진을 사용하여 직접 커넥터를 실행할 수 있습니다.

## 일반적인 활용 사례

Debezium을 유용하게 활용할 수 있는 여러 시나리오가 있지만, 여기서는 일반적인 몇 가지 시나리오에 대해서 간략히 설명합니다.

### 캐시 무효화

레코드가 변경되거나 제거되면 즉시 캐시를 자동으로 무효화합니다. 캐시가 분리된 프로세스(Redis, Memcache, Infinispan등)에서 실행 중인 경우에는 캐시 무효화 로직을 배치하여 해결할 수 있습니다. 경우에 따라 변경된 이벤트의 업데이트된 데이터를 사용하여 캐시 항목을 업데이트할 수 있습니다.

### 모놀리식 애플리케이션 단순화

많은 애플리케이션이 데이터베이스에 변경 사항이 커밋된 후 검색 인덱스 업데이트, 캐시 업데이트, 알림 전송, 비즈니스 로직 실행등과 같은 추가 작업을 수행합니다. 단일 트랙잭션을 위해 외부의 여러 시스템에도 업데이트하기 때문에 이런 행위를  `이중 쓰기`라고 부릅니다. 커밋 후 다른 모든 업데이트가 수행되기 전에 충돌이 발생할 경우 이중 쓰기는 데이터를 손실하거나 다른 시스템의 일관성을 유지하지 못할 위험이 있습니다. 그리고 이런 상황에 대처하기 위해 애플리케이션 로직이 복잡하게되기에 유지보수가 어렵게 됩니다.

CDC(Change Data Capture)를 사용하면 데이터가 오리지널 데이터베이스에서 커밋될 때 별도의 스레드 또는 프로세스/서비스에서 작업을 수행할 수 있습니다. 이 접근 방식은 장애를 더 잘 견디고, 이벤트를 놓치지 않으며, 확장성이 뛰어나고, 업그레이드 및 운영을 보다 쉽게 지원합니다.

### 데이터베이스 공유

여러 애플리케이션이 단일 데이터베이스를 공유하는 경우, 하나의 애플리케이션이 다른 애플리케이션에서 커밋한 변경 사항을 인식하는 것이 필요한 사항이 발생합니다. 한가지 접근법은 메시지 버스를 사용하는 것이지만, `이중 쓰기`의 문제에서 자유롭지 못합니다. Debezium을 사용하면 각 애플리케이션이 데이터베이스를 모니터링하고 변경사항에 대응하는 것이 매우 간단해집니다.

### 데이터 통합

데이터는 다른 용도로 사용되는 경우가 있으며, 형태가 다를 경우 여러 위치에 저장되는 경우가 많습니다. 여러 시스템을 동기화 상태로 유지하는 것은 어려울 수 있지만 Debezium을 이용하면 간단하게 이벤트 처리 로직을 빠르게 구현할 수 있습니다.

### CQRS

[Command Query Responsibility Separation (CQRS)](http://martinfowler.com/bliki/CQRS.html) 아키텍처 패턴은 업데이트를 위해 하나의 데이터 모델을 사용하고 데이터를 읽기 위해 하나 이상의 데이터 모델을 사용합니다. 일반적으로 CQRS을 이용해 애플리케이션을 구현하는 것은 복잡하며 특히, 신뢰할 수 있고 완전히 정렬된 처리를 보장해야 할 경우 더욱 복잡해집니다. 하지만 Debezium을 사용하면 더 쉽게 처리할 수 있습니다. 쓰기쪽은 정상적으로 기록하고 읽기는 Debezium을 통해 비동기 방식으로 업데이트하도록 하여 변경 사항을 캡처합니다. 쓰기쪽 테이블은 도메인 지향 엔티티를 나타낼 수 있으며, CQRS [이벤트 소싱](http://martinfowler.com/eaaDev/EventSourcing.html)과 쌍을 이루는 경우 쓰기쪽 테이블은 명령 전용 추가 이벤트 로그가 됩니다.

## Debezium 빌드 하기

아래의 소프트웨어는 로컬에서 Debezium을 빌드하기 위해 필요합니다.

* [Git](https://git-scm.com) 2.2.1 버전 이상
* JDK 17 버전 이상, 예) [OpenJDK](http://openjdk.java.net/projects/jdk/)
* [Docker Engine](https://docs.docker.com/engine/install/) 또는 [Docker Desktop](https://docs.docker.com/desktop/) 1.9 버전 이상
* [Apache Maven](https://maven.apache.org/index.html) 3.8.4 버전 이상  
  (or invoke the wrapper with `./mvnw` for Maven commands)

각 소프트웨어를 설치하기 위해서는 위의 링크의 지침을 확인하세요. 아래의 명령어를 통해 설치되었는지 확인할 수 있습니다.

    $ git --version
    $ javac -version
    $ mvn -version
    $ docker --version

### 왜 도커인가?

많은 오픈 소스 프로젝트에서 Git, Java 및 Maven을 사용하지만 도커를 요구하는 것은 일반적이지 않습니다. Debezium은 다양한 데이터베이스 및 여러 외부 시스템과 통신하도록 설계되었으며, 통합 테스트를 통해 Debezium이 올바르게 작동되는지 확인합니다. 테스트를 위해 필요한 모든 소프트웨어 시스템이 로컬에 설치되어 있을 것으로 예상하는 대신 Debezium의 빌드 시스템은 도커를 사용하여 필요한 이미지를 자동으로 다운로드 하거나 생성하고 각 시스템에 대한 컨테이너를 동작 시킵니다. 이를 기반으로 통합 테스트를 통해 Debezium이 예상대로 작동하는지 확인할 수 있으며 통합 테스트가 완료되면 Debezium의 빌드 스크립트가 구동중인 모든 컨테이너를 자동으로 중지합니다.

또한 Debezium에는 Java로 작성되지 않은 몇 개의 모듈이 있기에 특정 OS가 필요합니다. 도커를 사용하면 특정 OS와 필요한 모든 개발 도구가 포함된 이미지를 사용하여 빌드를 수행할 수 있습니다.
 
도커를 사용하면 다음과 같은 이점이 있습니다.
1. 각 외부 서비스의 특정 버전을 로컬 컴퓨터에 설치, 구성하여 실행할 필요가 없으며 로컬 네트워크에서 해당 서비스에 접근할 필요가 없기에 Debezium에서는 도커를 이용합니다.
1. 여러 버전의 외부 서비스를 테스트할 수 있습니다. 각 모듈은 필요한 모든 컨테이너를 시작할 수 있기에 서로 다른 모듈이 서로 다른 버전의 서비스를 쉽게 사용할 수 있게 됩니다.
1. 모든 사용자가 로컬에서 전체 빌드를 실행할 수 있습니다. 설정된 환경에서 빌드를 실행하는 원격 CI 서버에 의존할 필요가 없습니다.
1. 모든 빌드가 동일하게 동작됩니다. 여러 개발자가 동일한 코드를 빌드할 경우 JDK, Maven 및 Docker 버전을 동일하게 사용하면 동일한 결과를 볼 수 있습니다. 이는 컨테이너가 동일한 운영체제에서 동일한 버전의 서비스를 실행하기 때문입니다. 또한 모든 테스트는 컨테이너에서 실행중인 시스템에 연결하도록 설계되었기에 자신의 로컬 환경과 연관된 설정 및 구성을 수정할 필요가 없습니다.
1. 서비스에 의해 데이터가 수정되고 로컬에 저장되더라도 서비스를 초기화 할 필요가 없습니다. 도커 이미지는 캐시되기에 다시 빠르게 작업할 수 있습니다. 하지만 도커 컨테이너는 재사용되지 않습니다. 도커 컨테이너는 항상 초기 상태로 시작되며 종료시 삭제됩니다. 통합 테스트는 컨테이너에 의존하기에 항상 자동으로 초기화됩니다.

### 도커 환경 구성

도커 메이븐 플러그인은 아래의 변수를 사용하여 도커 호스트를 확인합니다.

    export DOCKER_HOST=tcp://10.1.2.2:2376
    export DOCKER_CERT_PATH=/path/to/cdk/.vagrant/machines/default/virtualbox/.docker
    export DOCKER_TLS_VERIFY=1

도커 머신 및 유사한 것을 사용하는 경우 자동으로 설정할 수 있습니다.

### 코드 작성

Git 리포지토리를 복제하여 코드를 가져옵니다.

    $ git clone https://github.com/debezium/debezium.git
    $ cd debezium

그 후 메이븐을 사용하여 코드를 빌드합니다.

    $ mvn clean verify

빌드시 다양한 DBMS에 대해 여러 개의 도커 컨테이너를 사용합니다. 도커가 실행중이 아니거나 구성되지 않은 경우에는 오류가 발생할 수 있습니다. 이 경우 도커가 실행중인지 확인해야 합니다. `docker ps` 명령어를 사용하여 도커가 실행중인지 확인하세요.

### 로컬에서 도커를 사용하여 빌드를 하지 않습니까?

아래의 명령어를 사용하여 통합 테스트 및 도커 빌드를 건너뛸 수 있습니다.

    $ mvn clean verify -DskipITs

### 테스트, 체크스타일 및 기타 작업을 수행하지 않고 아티팩트만 빌드

“quick” 빌드 프로파일을 사용하여 필수가 아닌 모든 플러그인(테스트, 통합 테스트, 체크스타일, 포맷터, API 호환성 검사등)을 건너뛸 수 있습니다.

    $ mvn clean verify -Dquick

위의 옵션을 사용하면 QA관련 메이븐 플러그인을 실행하지 않고 아웃푹 아티팩트만 빠르게 생성할 수 있습니다. 이 기능은 커넥터 JAR 및 아카이브를 빨리 생성할 때 매우 유용합니다. (예: 카프카 커넥터를 수동 테스트 할 경우)

### wal2json 또는 pgoutput 논리 디코딩 플러그인을 사용하여 Postgres 커넥터 테스트 수행

Postgres 커넥터는 DB서버에서 커넥터로 변경 사항을 스트리밍하기 위한 세 가지 논리 디코딩 플러그인(decoderbufs(디폴트), wal2json, pgoutput)을 지원합니다. wal2json을 사용하여 PG 커넥터의 통합 테스트를 수행하려면 `wal2json-decoder` 빌드 프로파일을 사용합니다.

    $ mvn clean install -pl :debezium-connector-postgres -Pwal2json-decoder
    
pgoutput을 사용하여 PG 커넥터의 통합 테스트를 수행하려면 `pgoutput-decoder` 및 `postgres-10` 빌드 프로파일을 활성화 해야 합니다.

    $ mvn clean install -pl :debezium-connector-postgres -Ppgoutput-decoder,postgres-10

wal2json 플러그인을 사용할 때 현재 일부 테스트를 통과하지 못합니다.

이런 부분에 대해서는 `io.debezium.connector.postgresql.DecoderDifferences`에 정의된 유형에 대해 참조하세요.

### 특정 Apicurio 버전으로 Postgres 커넥터 테스트 수행

특정 버전의 Apicurio에서 wal2json 또는 pgoutput 논리 디코딩 플러그인을 사용하여 PG 커넥터 테스트를 수행하려면 테스트 속성을 아래와 같이 전달 해야 합니다.

    $ mvn clean install -pl debezium-connector-postgres -Pwal2json-decoder 
          -Ddebezium.test.apicurio.version=1.3.1.Final

속성이 없으면 안정적인 버전의 Apicurio를 가져옵니다.

### 외부 데이터베이스에 대한 Postgres 커넥터 수행 (예: Amazon RDS)

RDS 클러스터를 사용하지 않은 상황에 대해 테스트하려면 해당 유저가 복제뿐만 아니라 `pg_hda.conf`의 모든 데이터베이스에 로그인할 수 있는 권한을 가진 슈퍼유저여야 합니다. 또한 대상 서버에서 postgis 패키지를 사용할 수 있어야 테스트를 수행할 수 있습니다.

    $ mvn clean install -pl debezium-connector-postgres -Pwal2json-decoder \
         -Ddocker.skip.build=true -Ddocker.skip.run=true -Dpostgres.host=<your PG host> \
         -Dpostgres.user=<your user> -Dpostgres.password=<your password> \
         -Ddebezium.test.records.waittime=10

필요에 따라 타임아웃 값을 조정합니다.
테스트할 RDS 데이터베이스 설정에 대한 자세한 내용은 [PostgreSQL on Amazon RDS](debezium-connector-postgres/RDS.md)를 참고하세요.

### 오라클 XStream을 사용하여 오라클 커넥터 테스트 수행

    $ mvn clean install -pl debezium-connector-oracle -Poracle,xstream -Dinstantclient.dir=<path-to-instantclient>

### non-CDB 데이터베이스에서 오라클 커넥터 테스트 수행

    $ mvn clean install -pl debezium-connector-oracle -Poracle -Dinstantclient.dir=<path-to-instantclient> -Ddatabase.pdb.name=

### IDE에서 몽고DB oplog 캡처를 사용하여 몽고DB 테스트 수행

메이븐 없이 테스트를 실행할 때는 올바른 매개 변수를 전달했는지 확인해야 합니다. `.github/workflows/mongodb-oplog-workflow.yml`에서 올바른 매개 변수를 찾아서 JVM 실행 매개 변수에 추가하고 `debezium.test`를 접두사로 추가한 후, 몽고DB 커넥터 디렉토리에서 수동으로 몽고DB 컨테이너를 시작해야 합니다

    $ mvn docker:start -B -am -Passembly -Dcheckstyle.skip=true -Dformat.skip=true -Drevapi.skip -Dcapture.mode=oplog -Dversion.mongo.server=3.6 -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn -Dmaven.wagon.http.pool=false -Dmaven.wagon.httpconnectionManager.ttlSeconds=120 -Dcapture.mode=oplog -Dmongo.server=3.6

해당 부분은 아래의 명령어와 유사합니다.

    java -ea -Ddebezium.test.capture.mode=oplog -Ddebezium.test.version.mongo.server=3.6 -Djava.awt.headless=true -Dconnector.mongodb.members.auto.discover=false -Dconnector.mongodb.name=mongo1 -DskipLongRunningTests=true [...]

## 기여 하기

Debezium 커뮤니티는 문제점 보고, 문서 지원, 버그 수정, 테스트 또는 새로운 기능 구현을 위한 코드 변경등 어떤 방식으로든 도움을 주고 싶어하는 모든 사람을 환영합니다. 자세한 내용은 이 [문서](CONTRIBUTE.md)를 참조하세요.
 
모든 Debezium 기여자에게 큰 감사를 드립니다!

<a href="https://github.com/debezium/debezium/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=debezium/debezium" />
</a>
