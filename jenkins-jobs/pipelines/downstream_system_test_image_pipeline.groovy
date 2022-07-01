pipeline {
    agent {
        label 'Slave'
    }

    stages {
        stage('CleanWorkspace') {
            steps {
                cleanWs()
            }
        }

        stage('Checkout') {
            steps {
                checkout([
                        $class           : 'GitSCM',
                        branches         : [[name: "${PARENT_DBZ_GIT_BRANCH}"]],
                        userRemoteConfigs: [[url: "${PARENT_DBZ_GIT_REPOSITORY}"]],
                        extensions       : [[$class           : 'RelativeTargetDirectory',
                                             relativeTargetDir: 'debezium']],
                ])
                sh '''
                set -x
                '''
            }
        }

        stage('Copy apicurio artifacts - latest') {
            when {
                expression { !params.APICURIO_PREPARE_BUILD_NUMBER }
            }
            steps {
                copyArtifacts projectName: 'ocp-downstream-apicurio-prepare-job', target: 'debezium/jenkins-jobs/docker/debezium-testing-system/downstream' ,filter: 'apicurio-registry-install-examples.zip', selector: lastSuccessful()
            }
        }
        stage('Copy apicurio artifacts') {
            when {
                expression { params.APICURIO_PREPARE_BUILD_NUMBER }
            }
            steps {
                copyArtifacts projectName: 'ocp-downstream-apicurio-prepare-job', target: 'debezium/jenkins-jobs/docker/debezium-testing-system/downstream' , filter: 'apicurio-registry-install-examples.zip', selector: specific(params.APICURIO_PREPARE_BUILD_NUMBER)
            }
        }

        stage('Copy strimzi artifacts - latest') {
            when {
                expression { !params.STRIMZI_PREPARE_BUILD_NUMBER }
            }
            steps {
                copyArtifacts projectName: 'ocp-downstream-strimzi-prepare-job', target: 'debezium/jenkins-jobs/docker/debezium-testing-system/downstream' , filter: 'amq-streams-install-examples.zip', selector: lastSuccessful()
            }
        }
        stage('Copy strimzi artifacts') {
            when {
                expression { params.STRIMZI_PREPARE_BUILD_NUMBER }
            }
            steps {
                copyArtifacts projectName: 'ocp-downstream-strimzi-prepare-job', target: 'debezium/jenkins-jobs/docker/debezium-testing-system/downstream' , filter: 'amq-streams-install-examples.zip', selector: specific(params.STRIMZI_PREPARE_BUILD_NUMBER)
            }
        }

        stage('Build') {
            steps {
                withCredentials([
                    usernamePassword(credentialsId: "${QUAY_CREDENTIALS}", usernameVariable: 'QUAY_USERNAME', passwordVariable: 'QUAY_PASSWORD'),
                ]) {

                    sh '''
                    cd debezium/jenkins-jobs/docker/debezium-testing-system
                    docker build --build-arg branch=${DBZ_GIT_BRANCH} --build-arg repository=${DBZ_GIT_REPOSITORY} -t testsuite-base:latest .

                    cd downstream
                    docker build -t testsuite:latest .
                    docker tag testsuite:latest quay.io/rh_integration/dbz-testing-system:${TAG}
                    docker login -u ${QUAY_USERNAME} -p ${QUAY_PASSWORD} quay.io
                    docker push quay.io/rh_integration/dbz-testing-system:${TAG}
                    '''
                }
            }
        }
    }
}
