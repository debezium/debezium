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

        stage('Process connectors and extra libs') {
            steps {
                withCredentials([
                        usernamePassword(credentialsId: "${QUAY_CREDENTIALS}", usernameVariable: 'QUAY_USERNAME', passwordVariable: 'QUAY_PASSWORD'),
                ]) {
                    sh '''
                    set -x
                    cd "${WORKSPACE}/debezium"
                    ./jenkins-jobs/scripts/copy-plugins.sh                          \\
                        --dir="${WORKSPACE}"                                        \\
                        --archive-urls="${DBZ_CONNECTOR_ARCHIVE_URLS}"              \\
                        --libs="${DBZ_EXTRA_LIBS}"                                  \\
                        --tags="${EXTRA_IMAGE_TAGS}"                                \\
                        --auto-tag="${AUTO_TAG}"                                    \\
                        --registry="quay.io" --organisation="${QUAY_ORGANISATION}"  \\
                        --dest-login="${QUAY_USERNAME}"                             \\
                        --dest-pass="${QUAY_PASSWORD}"                              \\
                        --img-output="${WORKSPACE}/published_image_dbz.txt"
                    '''
                    zip(archive: true, zipFile: 'artifact-server-all.zip', glob: 'published_image_dbz.txt')
                }
            }
        }
    }

    post {
        always {
            mail to: params.MAIL_TO, subject: "Debezium artifact server preparation #${env.BUILD_NUMBER} finished with ${currentBuild.currentResult}", body: """
${currentBuild.projectName} run ${env.BUILD_URL} finished with result: ${currentBuild.currentResult}
"""
        }
        success {
            archiveArtifacts "**/published_image*.txt"
        }
    }
}
