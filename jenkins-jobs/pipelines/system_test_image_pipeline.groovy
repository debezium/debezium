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
                        branches         : [[name: "${DBZ_GIT_BRANCH}"]],
                        userRemoteConfigs: [[url: "${DBZ_GIT_REPOSITORY}"]],
                        extensions       : [[$class           : 'RelativeTargetDirectory',
                                             relativeTargetDir: 'debezium']],
                ])
                sh '''
                set -x
                '''
            }
        }

        stage('Build') {
            steps {
                withCredentials([
                    usernamePassword(credentialsId: "${QUAY_CREDENTIALS}", usernameVariable: 'QUAY_USERNAME', passwordVariable: 'QUAY_PASSWORD'),
                ]) {
                    sh '''
                    pushd debezium/jenkins-jobs/docker/debezium-testing-system
                    DOCKER_IMAGE=quay.io/rh_integration/dbz-testing-system:${TAG}
                    docker build --build-arg branch=${DBZ_GIT_BRANCH} --build-arg repository=${DBZ_GIT_REPOSITORY} --target base -t ${DOCKER_IMAGE} .
                    docker login -u ${QUAY_USERNAME} -p ${QUAY_PASSWORD} quay.io
                    docker push ${DOCKER_IMAGE}
                '''
                }
            }
        }
    }
}
