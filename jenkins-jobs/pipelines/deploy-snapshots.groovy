import groovy.json.*
import java.util.stream.*

if (
    !DEBEZIUM_REPOSITORY ||
    !DEBEZIUM_BRANCH ||
    !DEBEZIUM_ADDITIONAL_REPOSITORIES
) {
    error 'Input parameters not provided'
}

GIT_CREDENTIALS_ID = 'debezium-github'

DEBEZIUM_DIR = 'debezium'
HOME_DIR = '/home/centos'

ORACLE_ARTIFACT_DIR = "$HOME_DIR/oracle-libs/21.1.0.0.0"
ORACLE_ARTIFACT_VERSION = '21.1.0.0'

def additionalDirs = []
node('Slave') {
    try {
        stage('Initialize') {
            dir('.') {
                deleteDir()
            }
            checkout([$class                           : 'GitSCM',
                      branches                         : [[name: "*/$DEBEZIUM_BRANCH"]],
                      doGenerateSubmoduleConfigurations: false,
                      extensions                       : [[$class: 'RelativeTargetDirectory', relativeTargetDir: DEBEZIUM_DIR]],
                      submoduleCfg                     : [],
                      userRemoteConfigs                : [[url: "https://$DEBEZIUM_REPOSITORY", credentialsId: GIT_CREDENTIALS_ID]]
            ]
            )
            DEBEZIUM_ADDITIONAL_REPOSITORIES.split().each {
                def (id, repository, branch) = it.split('#')
                checkout([$class                           : 'GitSCM',
                          branches                         : [[name: "*/$branch"]],
                          doGenerateSubmoduleConfigurations: false,
                          extensions                       : [[$class: 'RelativeTargetDirectory', relativeTargetDir: id]],
                          submoduleCfg                     : [],
                          userRemoteConfigs                : [[url: "https://$repository", credentialsId: GIT_CREDENTIALS_ID]]
                ]
                )
                additionalDirs << id
            }
            dir(ORACLE_ARTIFACT_DIR) {
                sh "mvn install:install-file -DgroupId=com.oracle.instantclient -DartifactId=ojdbc8 -Dversion=$ORACLE_ARTIFACT_VERSION -Dpackaging=jar -Dfile=ojdbc8.jar"
                sh "mvn install:install-file -DgroupId=com.oracle.instantclient -DartifactId=xstreams -Dversion=$ORACLE_ARTIFACT_VERSION -Dpackaging=jar -Dfile=xstreams.jar"
            }
        }

        stage('Build and deploy Debezium') {
            dir(DEBEZIUM_DIR) {
                sh "mvn clean deploy -U -s $HOME/.m2/settings-snapshots.xml -DdeployAtEnd=true -DskipITs -DskipTests -Passembly,oracle"
            }
        }

        additionalDirs.each { id ->
            stage("Build and deploy Debezium ${id.capitalize()}") {
                dir(id) {
                    sh "mvn clean deploy -U -s $HOME/.m2/settings-snapshots.xml -DdeployAtEnd=true -DskipITs -DskipTests -Passembly"
                }
            }
        }
    } finally {
        mail to: MAIL_TO, subject: "${JOB_NAME} run #${BUILD_NUMBER} finished", body: "Run ${BUILD_URL} finished with result: ${currentBuild.currentResult}"
    }
}
