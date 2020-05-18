import groovy.json.*
import java.util.stream.*

if (
    !DEBEZIUM_REPOSITORY ||
    !DEBEZIUM_BRANCH ||
    !DEBEZIUM_INCUBATOR_REPOSITORY ||
    !DEBEZIUM_INCUBATOR_BRANCH
) {
    error 'Input parameters not provided'
}

GIT_CREDENTIALS_ID = 'debezium-github'

DEBEZIUM_DIR = 'debezium'
INCUBATOR_DIR = 'debezium-incubator'
HOME_DIR = '/home/cloud-user'

ORACLE_ARTIFACT_DIR = "$HOME_DIR/oracle-libs/12.2.0.1.0"
ORACLE_ARTIFACT_VERSION = '12.2.0.1'

node('Slave') {

    stage ('Initialize') {
        dir('.') {
            deleteDir()
        }
        checkout([$class: 'GitSCM', 
            branches: [[name: "*/$DEBEZIUM_BRANCH"]], 
            doGenerateSubmoduleConfigurations: false, 
            extensions: [[$class: 'RelativeTargetDirectory', relativeTargetDir: DEBEZIUM_DIR]], 
                submoduleCfg: [], 
                userRemoteConfigs: [[url: "https://$DEBEZIUM_REPOSITORY", credentialsId: GIT_CREDENTIALS_ID]]
            ]
        )
        checkout([$class: 'GitSCM', 
            branches: [[name: "*/$DEBEZIUM_INCUBATOR_BRANCH"]], 
            doGenerateSubmoduleConfigurations: false, 
            extensions: [[$class: 'RelativeTargetDirectory', relativeTargetDir: INCUBATOR_DIR]], 
                submoduleCfg: [], 
                userRemoteConfigs: [[url: "https://$DEBEZIUM_INCUBATOR_REPOSITORY", credentialsId: GIT_CREDENTIALS_ID]]
            ]
        )
    }

    stage ('Build and deploy Debezium') {
        dir(DEBEZIUM_DIR) {
            sh "mvn clean deploy -U -s $HOME/.m2/settings-snapshots.xml -DdeployAtEnd=true -DskipITs -DskipTests -Passembly"
        }
    }

    stage ('Build and deploy Debezium Incubator') {
        dir(ORACLE_ARTIFACT_DIR) {
            sh "mvn install:install-file -DgroupId=com.oracle.instantclient -DartifactId=ojdbc8 -Dversion=$ORACLE_ARTIFACT_VERSION -Dpackaging=jar -Dfile=ojdbc8.jar"
            sh "mvn install:install-file -DgroupId=com.oracle.instantclient -DartifactId=xstreams -Dversion=$ORACLE_ARTIFACT_VERSION -Dpackaging=jar -Dfile=xstreams.jar"
        }
        dir(INCUBATOR_DIR) {
            sh "mvn clean deploy -U -s $HOME/.m2/settings-snapshots.xml -DdeployAtEnd=true -DskipITs -DskipTests -Passembly,oracle"
        }
    }
}
