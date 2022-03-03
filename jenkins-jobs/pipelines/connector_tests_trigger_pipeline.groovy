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
                def label = params.LABEL
                def current_build_label = label
                for (db in ['db2', 'mongodb', 'mysql', 'oracle', 'postgresql', 'sqlserver']) {
                    if (!params["${db.toUpperCase()}_TEST"]) {
                        continue
                    }
                    def build = jenkins.model.Jenkins.instance.getItem("connector-debezium-${db}-matrix-test").lastBuild

                    // if label param is not set, try parsing label from source url/branch
                    if (!label && params.PRODUCT_BUILD) {
                        def version_match = params.SOURCE_URL =~ /.*\/debezium-(.+)-src.zip$/
                        if (version_match && version_match[0][1]) {
                            def version = version_match[0][1].toString()
                            label = version
                            current_build_label = version
                        } else {
                            println "Debezium version of product build couldn't be parsed from SOURCE_URL"
                            continue
                        }
                    }
                    // if no label is set and not a product build, add branch name
                    else if (!label && !params.PRODUCT_BUILD) {
                        label = "#${build.number} ${params.BRANCH}"
                        current_build_label = "#${currentBuild.number} ${params.BRANCH}"
                    }

                    // set label
                    build.displayName = label
                }
                if (label){
                    currentBuild.displayName = current_build_label
                }
            }
        }
    }
}
