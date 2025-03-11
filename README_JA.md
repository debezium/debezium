[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.debezium/debezium-parent/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22io.debezium%22)
[![User chat](https://img.shields.io/badge/chat-users-brightgreen.svg)](https://debezium.zulipchat.com/#narrow/stream/302529-users)
[![Developer chat](https://img.shields.io/badge/chat-devs-brightgreen.svg)](https://debezium.zulipchat.com/#narrow/stream/302533-dev)
[![Google Group](https://img.shields.io/:mailing%20list-debezium-brightgreen.svg)](https://groups.google.com/forum/#!forum/debezium)
[![Stack Overflow](http://img.shields.io/:stack%20overflow-debezium-brightgreen.svg)](http://stackoverflow.com/questions/tagged/debezium)

Copyright Debezium Authors.
Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
The Antlr grammars within the debezium-ddl-parser module are licensed under the [MIT License](https://opensource.org/licenses/MIT).

[English](README.md) | [Chinese](README_ZH.md) | Japanese | [Korean](README_KO.md)

# Debezium

Debezium は変更データキャプチャ (Change Data Capture; CDC) のための低遅延データストリーミングプラットフォームを提供するオープンソースプロジェクトです。データベースを監視するようにDebezium をセットアップ・設定すると、データベース上で実行された各行レベルの変更をアプリケーションで利用する事ができます。コミットされた変更だけが見えるため、アプリケーションでトランザクションやロールバックされた変更について悩む必要はありません。Debezium は全ての変更について単一のモデルを提供するので、アプリケーションは様々な種類のデータベースソフトウェアの複雑さを気にする必要がありません。更に、Debezium は変更データキャプチャを永続的で複製されたログに記録します。変更データキャプチャを処理するアプリケーションはいつでも停止・再開することができ、その場合でも停止中に発生したイベントは全て再開したタイミングで処理されるため、全てのイベントが正しく完全に処理されることが保証されます。

データベースを監視して、変更された場合に通知することは、いつの時代も複雑なタスクでした。リレーショナルデータベースのトリガーは便利ですが、各データベースソフトウェアに固有で、データベース内のテーブルを更新する事に制限されています（他のプロセスと通信できません）。いくつかのデータベースは変更を監視するための API やフレームワークを提供していますが、標準化された方法は無くて、多くの知識と特殊化されたコードを必要とします。それをしてもなお、データベースに与える影響を最小限にしながら、データベースに適用された順番を保ちながら全ての変更を取得することは難しい作業です。

Debezium はこのような作業を行うためのモジュールを提供します。いくつかのモジュールは汎用のもので複数のデータベースソフトウェアで動作しますが、機能は制限され、パフォーマンスは低いです。他のモジュールはそれぞれのデータベースソフトウェアに合わせて作られており、各ソフトウェアに固有の機能を使うことで高い能力を持っています。

## 基本的なアーキテクチャ

Debezium は、Kafka と Kafka Connect を再利用する事で、永続性、信頼性および耐障害性を達成した変更データキャプチャ (Change Data Capture; CDC) のプラットフォームです。Kafka Connect 上の分散されて、スケーラブルで、耐障害性のあるサービスに展開された各コネクタが、それぞれ1つのデータベースサーバーを監視して、全ての変更を取得し、1つまたは複数の Kafka topic に保存します（典型的には、データベーステーブル毎に1つの topic を利用）。Kafka は、これらのデータが更新されたイベントを順序を保ったままレプリケーションして、上流サーバーへの影響を押さえて、このイベントを複数のクライアントが冪等に利用 (consume) することを可能にしてくれます。更に、クライアントは任意のタイミングで処理を止めて、またその場所から再開することができます。各クライアントは全てのデータ変更イベントを『正確に1回』または『少なくとも1回』のどちらで受け取るのかを決めることができて、各データベース/テーブルのデータ変更イベントは、データベースで起こったのと同じ順番で配送されます。

このレベルの耐障害性・パフォーマンス・スケーラビリティおよび信頼性を必要としない、あるいは求めないアプリケーションは、代わりに Debeium の *embedded connector engine* を直接アプリケーション内で実行する事もできます。変更イベントの中身は同じですが、Kafka での永続化を経由せずに、connector が直接アプリケーションにイベントを送信することになります。

## よくある利用例

Debezium が非常に価値あるものとなる状況は多くあります。ここでは、一般的なものをいくつか紹介します。


### キャッシュの無効化

キャッシュエントリーの record が変更・削除された時に、自動的にキャッシュエントリーを無効にできます。キャッシュが別のプロセス（例えば Redis, Memcache, Infinispan など）で実装されている場合、シンプルなキャッシュの無効化ロジックを別のプロセスやサービスとして切り出す事ができるので、主なアプリケーションをシンプルにできます。場合によっては、ロジックを洗煉させて、変更イベントの中にある更新後のデータを利用して関係のあるキャッシュエントリーを更新しておくこともできます。

### モノリシックアプリケーションをシンプルに

多くのアプリケーションは、データベースを更新してコミットした後に追加の作業を実行します: 検索 Index を更新する、キャッシュをアップデートする、通知を送る、ビジネスロジックを実行する、などです。アプリケーションが単一のトランザクションの枠を越えて複数のシステムに書き込みを行う事から、これらはよく『デュアルライト (dual-writes)』と呼ばれます。アプリケーションのロジックを複雑にして変更を難しくする以外にも、デュアルライトには問題点があります。それは、アプリケーションが一部のデータだけをコミットして残りの更新中にクラッシュした場合に、データを失ったりシステムが全体として不整合になるリスクがあることです。変更データキャプチャを利用することで、これら他の処理はオリジナルのデータベースでコミットされた後に、別のスレッドや別のプロセス・サービスで実行することができます。このアプローチは耐障害性に優れており、イベントを失う事無く、スケーラブルで、アップグレードや運用オペレーションを簡単にサポートできます。

### データベースの共有

複数のアプリケーションが1つのデータベースを共有する場合に、あるアプリケーションが他のアプリケーションによってコミットされた変更を知るのは容易ではありません。1つのアプローチはメッセージバスを使うことですが、トランザクションの範囲外にあるメッセージバスは先ほど触れたデュアルライトと同じ問題に行き当たります。しかし、 Debezium を使えばこの問題は非常に簡単に解決します、それぞれのアプリケーションはデータベースの変更を監視してイベントに反応することができます。

### データの統合

データは複数の場所に保存されることが多く、特にそれぞれが異なる目的・少し異なる形態で保存されていることもあります。複数のシステムを同期するのは難しいことですが、単純な ETL に基づいた解決策は Debezium と簡単なイベント処理ロジックで素早く実装する事ができます。

### コマンドクエリ責任分離

コマンドクエリ責任分離 ([Command Query Responsibility Separation; CQRS](http://martinfowler.com/bliki/CQRS.html)) はデータの更新と読み取りに異なるデータモデルを利用するアーキテクチャパターンです。変更は更新側で記録された後、様々な読み取り表現を更新するのに活用されることになります。結果として、特に信頼性と処理の順序を保つ必要がある場合において、 CQRS アプリケーションはより複雑になります。Debezium と変更データキャプチャはこの問題を解決してくれます：書き込みを通常通り保存するだけで、Debezium が変更を失われないように、順序付けされたストリームとしてキャプチャするので、コンシューマはこれを非同期に読み取って読み取り専用ビューの更新に使う事ができます。書き込み側のテーブル上でドメイン指向のエンティティを表現することもできますし、 CQRS が [Event Sourcing](http://martinfowler.com/eaaDev/EventSourcing.html) と共に使われる場合、書き込み側のテーブルはコマンドの追記専用のログであったりします。

## Debezium をビルドするには

Debezium のコードベースでを編集・ビルドする為には以下に示すソフトウェアが必要です。

* [Git](https://git-scm.com) 2.2.1 or later
* JDK 21 or later, e.g. [OpenJDK](http://openjdk.java.net/projects/jdk/)
* [Apache Maven](https://maven.apache.org/index.html) 3.9.8
* [Docker Engine](https://docs.docker.com/engine/install/) or [Docker Desktop](https://docs.docker.com/desktop/) 1.9 or later

インストール手順については上記のリンクを参照して下さい。インストールされているバージョンを確認する為には以下のコマンドを実行します:

    $ git --version
    $ javac -version
    $ mvn -version
    $ docker --version

### Docker が必要な理由

多くのオープンソースソフトウェアプロジェクトが Git, Java そして Maven を必須としていますが、Docker が必要なのは一般的ではありません。これには理由があります。Debezium は様々な種類のデータベースソフトウェアやサービスといった、多くの外部システムと連携して動くソフトウェアで、結合テストはこれが正しく動くことを確認します。しかしこれらのシステムがローカルにインストールされている事を期待するのではなく、Debezium のビルドシステムは Docker を使って自動的に必要なイメージをダウンロードまたは作成して、コンテナをスタートします。結合テストで Debezium の挙動が期待通りかを確認したら、Debezium のビルドシステムはテスト終了後にコンテナを自動的に終了します。

また、Debezium はいくつかの Java 以外の言語で書かれたモジュールも持っていて、それらをビルドするのにはターゲットとなるOSが必要になります。Docker を使う事で、ビルドはこれらの OS のイメージと必要な開発ツールを使うことができます。

Docker の利用にはいくつもの利点があります

1. 外部サービスの特定のバージョンをインストール・設定・実行する手間が省けます。もしローカルにインストールされているバージョンがあっても、Debezium のビルドはそれを利用しません。
1. 外部サービスの複数のバージョンをテストすることができます。それぞれのモジュールは必要な時にいつでも起動する事ができて、異なるモジュールが別々のバージョンを必要としても問題ありません。
1. 全てのユーザーが完全なビルドをローカルで実行できます。必要な外部サービスを全てインストールしたリモートの CI サーバーに頼る必要がありません。
1. 全てのビルドが一貫性のある状態になります。複数の開発者が同じコードをビルドしたときに同じ結果が得られるということです（同じ JDK, Maven そして Docker バージョンを使う限り）。コンテナは同じOS上で同じバージョンのサービスを実行しているためです。すべてのテストは、コンテナ内で稼働しているシステムに接続するように設計されており、接続プロパティやローカル環境に特有のカスタム構成をいじる必要はありません。
1. サービスがデータを変更・保存した場合にも、サーバーをクリーンアップする必要がありません。Docker *image* はキャッシュされ、コンテナの起動を高速化するために再利用されます。しかし、Docker *containers* が再利用される事は無く、常に初期状態で起動して、終了時に破棄されます。結合テストはコンテナに依存しているため、初期化は自動的に行われているのです。


### Docker 環境のセットアップ

Docker Maven Plugin は Docker host を以下の環境変数を探す事によって解決します。

    export DOCKER_HOST=tcp://10.1.2.2:2376
    export DOCKER_CERT_PATH=/path/to/cdk/.vagrant/machines/default/virtualbox/.docker
    export DOCKER_TLS_VERIFY=1

これらは Docker Machine や同種の何かを利用している場合には自動的に設定されています。

### コードのビルド

まずは Git repository を clone してコードを取得します

    $ git clone https://github.com/debezium/debezium.git
    $ cd debezium

ビルドには Maven を利用します

    $ mvn clean verify

このビルドは異なるデータベースソフトウェアのためにいくつかの Docker container を開始します。Docker が起動していない・設定されていない場合、恐らく難解なエラーが表示されます —— そのような場合は、常にDockerが起動していることを確認してください。 

### ビルドのために Docker を実行したくない場合

結合テストや Docker build は以下のコマンドでスキップできます。

    $ mvn clean verify -DskipITs

### テストや CheckStyle を行わずに成果物だけをビルドする

`quick` ビルドプロファイルを利用する事で、必須ではない plugin (tests, integration tests, CheckStyle, formatter, API compatibility check, etc.) をスキップすることができます

    $ mvn clean verify -Dquick

これは、品質保証に関連した Maven plugin の実行せずに、ビルド結果だけを生成する一番速い方法です。これは connector JAR やアーカイブをできるだけ速く出力したい場合に便利です。特に、Kafka Connect を手動テストする場合などに利用できます。

### 論理デコードプラグインとして、wal2json または pgoutput を利用して Postgres connector をテストする  

Postgres connector は、データベースの変更ストリームを論理デコードする3つの異なるプラグインをサポートしています： decoderbufs （デフォルト）, wal2json, および pgoutput です。Postgres connector を wal2json を使ってテストしたい場合、"wal2json-decoder" ビルドプロファイルを指定します。 

    $ mvn clean install -pl :debezium-connector-postgres -Pwal2json-decoder

pgoutput を利用してテストするには、 "pgoutput-decoder" と "postgres-10" ビルドプロファイルを有効にします:

    $ mvn clean install -pl :debezium-connector-postgres -Ppgoutput-decoder,postgres-10

いくつかのテストは、wal2json プラグインを利用した場合パスしません。そのようなクラスは `io.debezium.connector.postgresql.DecoderDifferences` クラスへの参照から見つける事ができます。

### 特定の Apicurio バージョンで Postgres connector をテストする

wal2json または pgoutput 論理デコードプラグインを使う場合、Apicurio のバージョンを選択する事ができます:

    $ mvn clean install -pl debezium-connector-postgres -Pwal2json-decoder 
          -Ddebezium.test.apicurio.version=1.3.1.Final

このプロパティが存在しない場合、安定バージョンの Apicurio が利用されます

### 外部データベースを利用して Postgres connector をテストする（例：Amazon RDS）

*RDS ではない* cluster に対してテストを実行したい場合、テストのユーザー名 (`<your user>`)として `replication` 権限だけでなく `pg_hba.conf` で全てのデータベースにログインできる権限を持ったスーパーユーザーを指定する必要があります。また、いくつかのテストのために、サーバー上で `postgis` パッケージが必要です。

    $ mvn clean install -pl debezium-connector-postgres -Pwal2json-decoder \
         -Ddocker.skip.build=true -Ddocker.skip.run=true -Dpostgres.host=<your PG host> \
         -Dpostgres.user=<your user> -Dpostgres.password=<your password> \
         -Ddebezium.test.records.waittime=10

必要に応じてタイムアウト値も調整してください。

RDS データベースを設定してテストする方法ついて詳しくは [PostgreSQL on Amazon RDS](debezium-connector-postgres/RDS.md) をご覧下さい。


### Oracle XStream を利用して Oracle connector をテストする

    $ mvn clean install -pl debezium-connector-oracle -Poracle-xstream,oracle-tests -Dinstantclient.dir=<path-to-instantclient>

### non-CDB データベースで Oracle connector をテストする

    $ mvn clean install -pl debezium-connector-oracle -Poracle-tests -Dinstantclient.dir=<path-to-instantclient> -Ddatabase.pdb.name=

## Contributing

Debezium コミュニティは、問題の報告、文書作成の支援、バグ修正、テストの追加、新機能の実装のためのコード変更の貢献など、全ての形の支援を歓迎します。詳細は [CONTRIBUTE.md](CONTRIBUTE.md) を参照してください。

Debezium の貢献者の皆さんに感謝します。

<a href="https://github.com/debezium/debezium/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=debezium/debezium" />
</a>
