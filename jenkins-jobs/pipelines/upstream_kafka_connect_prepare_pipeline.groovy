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

        stage('Checkout - Debezium DB2') {
            steps {
                checkout([
                        $class           : 'GitSCM',
                        branches         : [[name: "${DBZ_GIT_BRANCH_DB2}"]],
                        userRemoteConfigs: [[url: "${DBZ_GIT_REPOSITORY_DB2}"]],
                        extensions       : [[$class           : 'RelativeTargetDirectory',
                                             relativeTargetDir: 'debezium-connector-db2']],
                ])
            }
        }

        stage('Configure') {
            steps {
                script {
//                    Configure images if provided
                    env.IMAGE_TAG_SUFFIX="${BUILD_NUMBER}"

//                    Oracle Configuration
                    env.ORACLE_ARTIFACT_VERSION='21.1.0.0'
                    env.ORACLE_ARTIFACT_DIR = "${env.HOME}/oracle-libs/21.1.0.0.0"
//                    Apicurio version
                    env.APICURIO_ARTIFACT_VERSION="${APICURIO_VERSION}"
                }
                withCredentials([
                        usernamePassword(credentialsId: "${QUAY_CREDENTIALS}", usernameVariable: 'QUAY_USERNAME', passwordVariable: 'QUAY_PASSWORD'),

                ]) {
                    sh '''
                    set -x
                    mvn org.apache.maven.plugins:maven-dependency-plugin:2.8:get -Dartifact=io.apicurio:apicurio-registry-distro-connect-converter:${APICURIO_ARTIFACT_VERSION}:zip
                    cd ${ORACLE_ARTIFACT_DIR}
                    mvn install:install-file -DgroupId=com.oracle.instantclient -DartifactId=ojdbc8 -Dversion=${ORACLE_ARTIFACT_VERSION} -Dpackaging=jar -Dfile=ojdbc8.jar
                    mvn install:install-file -DgroupId=com.oracle.instantclient -DartifactId=xstreams -Dversion=${ORACLE_ARTIFACT_VERSION} -Dpackaging=jar -Dfile=xstreams.jar
                    '''
                }
            }
        }

        stage('Build debezium') {
            steps {
//              Build core & parent
                sh '''
                set -x
                cd "${WORKSPACE}/debezium"
                mvn clean install -DskipTests -DskipITs
                '''
//              Build Oracle connector
                sh '''
                set -x
                cd ${WORKSPACE}/debezium
                mvn install -Passembly,oracle -DskipTests -DskipITs 
                '''
//              Build DB2 Connector
                sh '''
                set -x
                cd ${WORKSPACE}/debezium-connector-db2
                mvn clean install -DskipTests -DskipITs -Passembly
                '''
            }
        }

        stage('Login to private Quay repository'){
            when(){
                expression { params.ORACLE_INCLUDED }
            }
            steps {
                withCredentials([
                        usernamePassword(credentialsId: "${PRIVATE_QUAY_CREDENTIALS}", usernameVariable: 'PRIVATE_QUAY_USERNAME', passwordVariable: 'PRIVATE_QUAY_PASSWORD'),
                ]){
                    sh '''
                    set -x
                    docker login -u ${PRIVATE_QUAY_USERNAME} -p     ${PRIVATE_QUAY_PASSWORD} quay.io
                    '''
                }
            }
        }

        stage('Build and push image') {
            steps {
                withCredentials([
                        usernamePassword(credentialsId: "${QUAY_CREDENTIALS}", usernameVariable: 'QUAY_USERNAME', passwordVariable: 'QUAY_PASSWORD'),
                ]) {
                    sh '''
                    set -x
                    cd "${WORKSPACE}/debezium"
                    ./jenkins-jobs/scripts/upstream-kafka-connect-prepare.sh      \\
                        --dir="${WORKSPACE}"                                        \\
                        --tags="${EXTRA_IMAGE_TAGS}"                                \\
                        --auto-tag="${AUTO_TAG}"                                    \\
                        --registry="quay.io" --organisation="${QUAY_ORGANISATION}"  \\
                        --dest-login="${QUAY_USERNAME}"                             \\
                        --dest-pass="${QUAY_PASSWORD}"                              \\
                        --kc-base-tag="${KC_BASE_TAG}"                              \\
                        --kafka-version="${KAFKA_VERSION}"                          \\
                        --img-output="${WORKSPACE}/published_image_dbz.txt"         \\
                        --oracle-included="${ORACLE_INCLUDED}"
                    '''
                }
            }
        }
    }

    post {
        always {
            mail to: MAIL_TO, subject: "Debezium upstream kafka connect image preparation #${BUILD_NUMBER} finished", body: """
${currentBuild.projectName} run ${BUILD_URL} finished with result: ${currentBuild.currentResult}
"""
        }
        success {
            archiveArtifacts "**/published_image*.txt"
        }
    }
}
