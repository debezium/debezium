pipeline {
    agent {
        label 'Slave'
    }
    stages {
        stage('Start') {
            parallel {
                stage('Invoke_db2') {
                    when {
                        expression { params.DB2_TEST }
                    }
                    steps {
                        build job: 'connector-debezium-db2-matrix-test', parameters: [
                                string(name: 'REPOSITORY_DB2', value: params.REPOSITORY_DB2),
                                string(name: 'BRANCH', value: params.BRANCH),
                                string(name: 'SOURCE_URL', value: params.SOURCE_URL),
                                booleanParam(name: 'PRODUCT_BUILD', value: params.PRODUCT_BUILD)
                        ]
                    }
                }

                stage('Invoke_mongodb') {
                    when {
                        expression { params.MONGODB_TEST }
                    }
                    steps {
                        build job: 'connector-debezium-mongodb-matrix-test', parameters: [
                                string(name: 'REPOSITORY_CORE', value: params.REPOSITORY_CORE),
                                string(name: 'BRANCH', value: params.BRANCH),
                                string(name: 'SOURCE_URL', value: params.SOURCE_URL),
                                booleanParam(name: 'PRODUCT_BUILD', value: params.PRODUCT_BUILD)
                        ]
                    }
                }

                stage('Invoke_mysql') {
                    when {
                        expression { params.MYSQL_TEST }
                    }
                    steps {
                        build job: 'connector-debezium-mysql-matrix-test', parameters: [
                                string(name: 'REPOSITORY_CORE', value: params.REPOSITORY_CORE),
                                string(name: 'BRANCH', value: params.BRANCH),
                                string(name: 'SOURCE_URL', value: params.SOURCE_URL),
                                booleanParam(name: 'PRODUCT_BUILD', value: params.PRODUCT_BUILD),
                        ]
                    }
                }

                stage('Invoke_oracle') {
                    when {
                        expression { params.ORACLE_TEST }
                    }
                    steps {
                        build job: 'connector-debezium-oracle-matrix-test', parameters: [
                                string(name: 'QUAY_CREDENTIALS', value: params.QUAY_CREDENTIALS),
                                string(name: 'REPOSITORY_CORE', value: params.REPOSITORY_CORE),
                                string(name: 'BRANCH', value: params.BRANCH),
                                string(name: 'SOURCE_URL', value: params.SOURCE_URL),
                                booleanParam(name: 'PRODUCT_BUILD', value: params.PRODUCT_BUILD)
                        ]
                    }
                }

                stage('Invoke_postgresql') {
                    when {
                        expression { params.POSTGRESQL_TEST }
                    }
                    steps {
                        build job: 'connector-debezium-postgresql-matrix-test', parameters: [
                                string(name: 'REPOSITORY_CORE', value: params.REPOSITORY_CORE),
                                string(name: 'BRANCH', value: params.BRANCH),
                                string(name: 'SOURCE_URL', value: params.SOURCE_URL),
                                booleanParam(name: 'PRODUCT_BUILD', value: params.PRODUCT_BUILD)
                        ]
                    }
                }

                stage('Invoke_sqlserver') {
                    when {
                        expression { params.SQLSERVER_TEST }
                    }
                    steps {
                        build job: 'connector-debezium-sqlserver-matrix-test', parameters: [
                                string(name: 'REPOSITORY_CORE', value: params.REPOSITORY_CORE),
                                string(name: 'BRANCH', value: params.BRANCH),
                                string(name: 'SOURCE_URL', value: params.SOURCE_URL),
                                booleanParam(name: 'PRODUCT_BUILD', value: params.PRODUCT_BUILD)
                        ]
                    }
                }
            }
        }
    }

    post {
        always {
            script {
                def version = ""

                for (db in ['db2', 'mongodb', 'mysql', 'oracle', 'postgresql', 'sqlserver']) {
                    if (!params["${db.toUpperCase()}_TEST"]) {
                        continue
                    }
                    def jobName = "connector-debezium-${db}-matrix-test"
                    def build = jenkins.model.Jenkins.instance.getItem(jobName).getLastCompletedBuild()

                    if (!build) {
                        println "No build of ${jobName} found!"
                        return
                    }

                    label = "#${build.number} parent: #${currentBuild.number}"

                    if (params.LABEL) {
                        label += " ${params.LABEL}"
                    }
                    // if label param is not set and product build, try parsing label from source url/branch
                    else if (params.PRODUCT_BUILD) {
                        def versionMatch = params.SOURCE_URL =~ /.*\/debezium-(.+)-src.zip$/
                        if (!version && versionMatch && versionMatch[0][1]) {
                            version = versionMatch[0][1].toString()
                            label += " version: ${version}"
                        } else {
                            println "Debezium version of product build couldn't be parsed from SOURCE_URL"
                            continue
                        }
                    }

                    // set label
                    build.displayName = label
                }
                 // set parent job label
                if (params.LABEL) {
                    currentBuild.displayName = "#${currentBuild.number} ${params.LABEL}"
                } else if (version) {
                    currentBuild.displayName = "#${currentBuild.number} version: ${version}"
                }
                // if not a product build and no custom label set, keep default parent build name
            }
        }
    }
}
