import groovy.json.*
import java.util.*

IMAGES_DIR = 'images'
GIT_CREDENTIALS_ID = 'debezium-github'
DOCKER_CREDENTIALS_ID = 'debezium-dockerhub'
QUAYIO_CREDENTIALS_ID = 'debezium-quay'

node('Slave') {
    catchError {
        stage('Initialize') {
            dir('.') {
                deleteDir()
            }
            checkout([$class                           : 'GitSCM',
                      branches                         : [[name: params.IMAGES_BRANCH]],
                      doGenerateSubmoduleConfigurations: false,
                      extensions                       : [[$class: 'RelativeTargetDirectory', relativeTargetDir: IMAGES_DIR]],
                      submoduleCfg                     : [],
                      userRemoteConfigs                : [[url: "https://$params.IMAGES_REPOSITORY", credentialsId: GIT_CREDENTIALS_ID]]
            ]
            )
            withCredentials([usernamePassword(credentialsId: DOCKER_CREDENTIALS_ID, passwordVariable: 'DOCKER_PASSWORD', usernameVariable: 'DOCKER_USERNAME')]) {
                sh """
                    docker login -u $DOCKER_USERNAME -p $DOCKER_PASSWORD
                """
            }
            withCredentials([string(credentialsId: QUAYIO_CREDENTIALS_ID, variable: 'USERNAME_PASSWORD')]) {
                def credentials = USERNAME_PASSWORD.split(':')
                sh """
                    set +x
                    docker login -u ${credentials[0]} -p ${credentials[1]} quay.io
                """
            }
        }
        stage('master') {
            echo "Building debezium tool images"
            dir(IMAGES_DIR) {
                sh "PUSH_IMAGES=true TAG=$TAG ./build-tool-images.sh"
            }
        }
    }

    mail to: params.MAIL_TO, subject: "${env.JOB_NAME} run #${env.BUILD_NUMBER} finished with ${currentBuild.currentResult}", body: "Run ${env.BUILD_URL} finished with result: ${currentBuild.currentResult}"
}
