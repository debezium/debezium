import groovy.json.*
import java.util.stream.*

if (
    !params.DEBEZIUM_REPOSITORY ||
    !params.DEBEZIUM_BRANCH ||
    !params.DEBEZIUM_ADDITIONAL_REPOSITORIES
) {
    error 'Input parameters not provided'
}

GIT_CREDENTIALS_ID = 'debezium-github'

DEBEZIUM_DIR = 'debezium'
HOME_DIR = '/home/centos'

def additionalDirs = []
node('Slave') {
    catchError {
        stage('Initialize') {
            dir('.') {
                deleteDir()
            }
            checkout([$class                           : 'GitSCM',
                      branches                         : [[name: "*/$params.DEBEZIUM_BRANCH"]],
                      doGenerateSubmoduleConfigurations: false,
                      extensions                       : [[$class: 'RelativeTargetDirectory', relativeTargetDir: DEBEZIUM_DIR]],
                      submoduleCfg                     : [],
                      userRemoteConfigs                : [[url: "https://$params.DEBEZIUM_REPOSITORY", credentialsId: GIT_CREDENTIALS_ID]]
            ]
            )
            params.DEBEZIUM_ADDITIONAL_REPOSITORIES.split().each {
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

            dir(DEBEZIUM_DIR) {
                ORACLE_ARTIFACT_VERSION = (readFile('pom.xml') =~ /(?ms)<version.oracle.driver>(.+)<\/version.oracle.driver>/)[0][1]
                ORACLE_ARTIFACT_DIR = "$HOME_DIR/oracle-libs/${ORACLE_ARTIFACT_VERSION}.0"
            }

            dir(ORACLE_ARTIFACT_DIR) {
                sh "mvn install:install-file -DgroupId=com.oracle.instantclient -DartifactId=ojdbc8 -Dversion=$ORACLE_ARTIFACT_VERSION -Dpackaging=jar -Dfile=ojdbc8.jar"
                sh "mvn install:install-file -DgroupId=com.oracle.instantclient -DartifactId=xstreams -Dversion=$ORACLE_ARTIFACT_VERSION -Dpackaging=jar -Dfile=xstreams.jar"
            }
        }

        stage('Build and deploy Debezium') {
            dir(DEBEZIUM_DIR) {
                sh "mvn clean deploy -U -s $env.HOME/.m2/settings-snapshots.xml -DdeployAtEnd=true -DskipITs -DskipTests -Passembly,oracle-all,docs  -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn -Dmaven.wagon.http.pool=false -Dmaven.wagon.httpconnectionManager.ttlSeconds=120 -Dmaven.wagon.rto=20000 -Dmaven.wagon.http.retryHandler.count=1 -Dmaven.wagon.http.serviceUnavailableRetryStrategy.retryInterval=5000"
            }
        }

        additionalDirs.each { id ->
            stage("Build and deploy Debezium ${id.capitalize()}") {
                dir(id) {
                    // Execute a dependency installation script if provided by the repository
                    sh "if [ -f install-artifacts.sh ]; then ./install-artifacts.sh; fi"

                    sh "mvn clean deploy -s $env.HOME/.m2/settings-snapshots.xml -DdeployAtEnd=true -DskipITs -DskipTests -Passembly,docs -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn -Dmaven.wagon.http.pool=false -Dmaven.wagon.httpconnectionManager.ttlSeconds=120 -Dmaven.wagon.rto=20000 -Dmaven.wagon.http.retryHandler.count=1 -Dmaven.wagon.http.serviceUnavailableRetryStrategy.retryInterval=5000"
                }
            }
        }
    }

    mail to: params.MAIL_TO, subject: "${env.JOB_NAME} run #${env.BUILD_NUMBER} finished with ${currentBuild.currentResult}", body: "Run ${env.BUILD_URL} finished with result: ${currentBuild.currentResult}"
}
