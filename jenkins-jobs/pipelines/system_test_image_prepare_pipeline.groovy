pipeline {
    agent {
        label 'NodeXL'
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

        stage('Build') {
            steps {
                withCredentials([
                    usernamePassword(credentialsId: "${QUAY_CREDENTIALS}", usernameVariable: 'QUAY_USERNAME', passwordVariable: 'QUAY_PASSWORD'),
                    file(credentialsId: "${PULL_SECRET}", variable: 'SECRET_PATH'),
                ]) {

                    sh '''
                    cp ${SECRET_PATH} debezium/jenkins-jobs/docker/debezium-testing-system/secret.yml
                    pushd debezium/jenkins-jobs/docker/debezium-testing-system
                    docker build --build-arg branch=${DBZ_GIT_BRANCH} --build-arg repository=${DBZ_GIT_REPOSITORY} -t testsuite:docker-test .
                    docker tag testsuite:docker-test quay.io/rh_integration/dbz-testing-system:${TAG}
                    docker login -u ${QUAY_USERNAME} -p ${QUAY_PASSWORD} quay.io
                    docker push quay.io/rh_integration/dbz-testing-system:${TAG}
                    rm debezium/jenkins-jobs/docker/debezium-testing-system/secret.yml
                '''
                }
            }
        }
    }
}