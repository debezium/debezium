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

                script {
                    env.STRZ_RESOURCES = "${env.WORKSPACE}/strimzi/install/cluster-operator"
                }
                sh '''
                set -x
                curl -OJs ${STRZ_RESOURCES_ARCHIVE_URL} && unzip amq-streams-*-install-examples.zip -d strimzi
                '''
            }
        }

        stage('Copy Images & Process Resource') {
            steps {
                withCredentials([
                        usernamePassword(credentialsId: "${QUAY_CREDENTIALS}", usernameVariable: 'QUAY_USERNAME', passwordVariable: 'QUAY_PASSWORD'),

                ]) {
                    sh '''
                    set -x
                    cd "${WORKSPACE}/debezium"
                    ./jenkins-jobs/scripts/copy-images.sh                           \\
                        --dir="${STRZ_RESOURCES}"                                   \\
                        --images="${STRZ_IMAGES}"                                   \\
                        --registry="quay.io" --organisation="${QUAY_ORGANISATION}"  \\
                        --dest-login="${QUAY_USERNAME}"                             \\
                        --dest-pass="${QUAY_PASSWORD}"                              \\
                        --deployment-desc="${STRZ_RESOURCES_DEPLOYMENT_DESCRIPTOR}" \\
                        --img-output="${WORKSPACE}/published_images.txt"            
                    '''
                    zip(archive: true, zipFile: 'amq-streams-install-examples.zip', dir: 'strimzi')
                }
            }
        }

        stage('Build Debezium Connect Image') {
            when {
                expression { params.DBZ_CONNECT_BUILD }
            }
            steps {
                withCredentials([
                        usernamePassword(credentialsId: "${QUAY_CREDENTIALS}", usernameVariable: 'QUAY_USERNAME', passwordVariable: 'QUAY_PASSWORD'),
                ]) {
                    sh '''
                    set -x
                    cd "${WORKSPACE}/debezium"
                    ./jenkins-jobs/scripts/build-connect-image.sh                   \\
                        --dir="${WORKSPACE}"                                        \\
                        --archive-urls="${DBZ_CONNECTOR_ARCHIVE_URLS}"              \\
                        --libs="${DBZ_EXTRA_LIBS}"                                  \\
                        --images="${STRZ_IMAGES}"                                   \\
                        --registry="quay.io" --organisation="${QUAY_ORGANISATION}"  \\
                        --dest-login="${QUAY_USERNAME}"                             \\
                        --dest-pass="${QUAY_PASSWORD}"                              \\
                        --img-output="${WORKSPACE}/published_images_dbz.txt"
                    '''
                }
            }
        }
    }

    post {
        always {
            mail to: 'jcechace@redhat.com', subject: "Debezium OpenShift test run #${BUILD_NUMBER} finished", body: """
${currentBuild.projectName} run ${BUILD_URL} finished with result: ${currentBuild.currentResult}
"""
        }
        success {
            archiveArtifacts "**/published_images*.txt"
        }
    }
}
