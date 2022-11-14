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
            }
        }

        stage('Build Debezium Connect Image') {
            steps {
                withCredentials([
                        usernamePassword(credentialsId: "${QUAY_CREDENTIALS}", usernameVariable: 'QUAY_USERNAME', passwordVariable: 'QUAY_PASSWORD'),
                ]) {
                    sh '''
                    set -x
                    cd "${WORKSPACE}/debezium"
                    ./jenkins-jobs/scripts/build-rhel-kafka-image.sh                \\
                        --dir="${WORKSPACE}"                                        \\
                        --archive-urls="${DBZ_CONNECTOR_ARCHIVE_URLS}"              \\
                        --kafka-url="${KAFKA_URL}"                                  \\
                        --dbz-scripts="${DBZ_SCRIPTS_VERSION}"                      \\
                        --libs="${DBZ_EXTRA_LIBS}"                                  \\
                        --image="${RHEL_IMAGE}"                                     \\
                        --tags="${EXTRA_IMAGE_TAGS}"                                \\
                        --auto-tag="${AUTO_TAG}"                                    \\
                        --registry="quay.io" --organisation="${QUAY_ORGANISATION}"  \\
                        --dest-login="${QUAY_USERNAME}"                             \\
                        --dest-pass="${QUAY_PASSWORD}"                              \\
                        --img-output="${WORKSPACE}/published_image_dbz.txt"
                    '''
                    zip(archive: true, zipFile: 'rhel-kafka-all.zip', glob: 'published_image_dbz.txt')
                }
            }
        }
    }

    post {
        always {
            mail to: params.MAIL_TO, subject: "Rhel downstream preparation #${env.BUILD_NUMBER} finished", body: """
            ${currentBuild.projectName} run ${env.BUILD_URL} finished with result: ${currentBuild.currentResult}
            """
        }
        success {
            archiveArtifacts "**/published_image*.txt"
        }
    }
}
