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
                                string(name: 'MAIL_TO', value: params.MAIL_TO),
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
                def databases = ['db2', 'mongodb', 'mysql', 'oracle', 'postgresql', 'sqlserver']
                def label = params.LABEL
                def current_build_label = label
                for (db in databases) {
                    def run_test_arg = db.toUpperCase() + "_TEST"
                    def job_name = "connector-debezium-" + db + "-matrix-test"
                    if (!params[run_test_arg]) {
                        continue
                    }
                    def build = jenkins.model.Jenkins.instance.getItem(job_name).lastBuild

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
                    else if (!label && !params.PRODUCT_BUILD) {
                        def branch = params.BRANCH
                        label = "#" + build.getNumber() + " " + branch
                        current_build_label = "#" + currentBuild.getNumber() + " " + branch
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
