#!/usr/bin/env groovy
/*
 * Copyright [2019 - 2020] Confluent Inc.
 */
def config = jobConfig {}

common {
  slackChannel = '#connect-warn'
  nodeLabel = 'docker-debian-jdk17'
  downStreamValidate = false
  timeoutHours = 4
  mavenProfiles = 'assembly'
  mavenFlags = '-U -Dmaven.wagon.http.retryHandler.count=10 --batch-mode -pl debezium-connector-mysql,debezium-connector-postgres,debezium-connector-sqlserver -am'
  mvnSkipDeploy = true
}
