import groovy.json.*
import java.util.*

IMAGES_DIR = 'images'
GIT_CREDENTIALS_ID = 'debezium-github'
DOCKER_CREDENTIALS_ID = 'debezium-dockerhub'

node('Slave') {
    try {
        stage('Initialize') {
            dir('.') {
                deleteDir()
            }
            checkout([$class                           : 'GitSCM',
                      branches                         : [[name: IMAGES_BRANCH]],
                      doGenerateSubmoduleConfigurations: false,
                      extensions                       : [[$class: 'RelativeTargetDirectory', relativeTargetDir: IMAGES_DIR]],
                      submoduleCfg                     : [],
                      userRemoteConfigs                : [[url: "https://$IMAGES_REPOSITORY", credentialsId: GIT_CREDENTIALS_ID]]
            ]
            )
            withCredentials([usernamePassword(credentialsId: DOCKER_CREDENTIALS_ID, passwordVariable: 'DOCKER_PASSWORD', usernameVariable: 'DOCKER_USERNAME')]) {
                sh """
                    docker login -u $DOCKER_USERNAME -p $DOCKER_PASSWORD
                """
            }
        }
        stage('master') {
            echo "Building debezium tool images"
            dir(IMAGES_DIR) {
                sh "PUSH_IMAGES=true TAG=$TAG ./build-tool-images.sh"
            }
        }
    } finally {
        mail to: MAIL_TO, subject: "${JOB_NAME} run #${BUILD_NUMBER} finished", body: "Run ${BUILD_URL} finished with result: ${currentBuild.currentResult}"
    }
}
