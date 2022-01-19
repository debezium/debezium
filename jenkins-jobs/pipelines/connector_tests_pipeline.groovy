pipeline {
    stages {
        stage('Invoke_pipeline') {
            steps {
                build job: 'jenkins-jobs/job-dsl/connector-debezium-mysql-matrix-test', parameters: [
                        string(name: 'REPOSITORY', value: "${REPOSITORY}"),
                        string(name: 'BRANCH', value: "${BRANCH}"),
                        string(name: 'SOURCE_URL', value: "${SOURCE_URL}"),
                        string(name: 'RPODUCT_BUILD', value: "${PRODUCT_BUILD}")
                ]
            }
        }
    }
}