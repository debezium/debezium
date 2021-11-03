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
                    env.APIC_RESOURCES = "${env.WORKSPACE}/apicurio/install"
                }
                sh '''
                set -x
                curl -OJs ${APIC_RESOURCES_ARCHIVE_URL} &&
                unzip service-registry-*-install-examples.zip -d apicurio &&
                mv apicurio/apicurio-registry-*/* apicurio/ &&
                rm -r apicurio/apicurio-registry-*
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
                        --dir="${APIC_RESOURCES}"                                   \\
                        --images="${APIC_IMAGES}"                                   \\
                        --registry="quay.io" --organisation="${QUAY_ORGANISATION}"  \\
                        --dest-login="${QUAY_USERNAME}"                             \\
                        --dest-pass="${QUAY_PASSWORD}"                              \\
                        --deployment-desc="${APIC_RESOURCES_DEPLOYMENT_DESCRIPTOR}" \\
                        --img-output="${WORKSPACE}/published_images.txt"            \\
                        `if [ $PUSH_IMAGES = false ]; then echo " -s"; fi`            
                    '''
                    zip(archive: true, zipFile: 'apicurio-registry-install-examples.zip', dir: 'apicurio')
                }
            }
        }
    }

    post {
        always {
            mail to: MAIL_TO, subject: "Downstream apicurio preparation #${BUILD_NUMBER} finished", body: """
${currentBuild.projectName} run ${BUILD_URL} finished with result: ${currentBuild.currentResult}
"""
        }
        success {
            archiveArtifacts "**/published_images*.txt"
        }
    }
}
