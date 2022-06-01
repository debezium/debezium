pipeline {
    agent {
        label 'Slave'
    }

    stages {
        stage('Matrix') {
            matrix {
                axes {
                    axis {
                        name 'ORACLE_VERSION'
                        values '21.3.0', '19.3.0', '12.2.0.1'
                    }
                }

                agent {
                    label 'NodeXL'
                }

                stages {
                    stage('Checkout - Upstream') {
                        when {
                            expression { !params.PRODUCT_BUILD }
                        }
                        steps {
                            checkout([
                                    $class           : 'GitSCM',
                                    branches         : [[name: "${BRANCH}"]],
                                    userRemoteConfigs: [[url: "${REPOSITORY}"]],
                                    extensions       : [[$class           : 'RelativeTargetDirectory',
                                                         relativeTargetDir: 'debezium']],
                            ])
                        }
                    }

                    stage('Checkout - Downstream') {
                        when {
                            expression { params.PRODUCT_BUILD }
                        }
                        steps {
                            script {
                                env.MVN_PROFILE = '-Ppnc'
                                env.MAVEN_OPTS = '-Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true -Dmaven.wagon.http.ssl.ignore.validity.dates=true'
                            }

                            sh '''
                            curl -OJs $SOURCE_URL && unzip debezium-*-src.zip               
                            pushd debezium-*-src
                            mv $(ls | grep -P 'debezium-[^-]+.Final') ${WORKSPACE}/debezium
                            popd
                            rm -rf debezium-*-src
                            '''
                        }
                    }

                    stage('Run Database') {
                        steps {
                            withCredentials([
                                    usernamePassword(credentialsId: "${QUAY_CREDENTIALS}", usernameVariable: 'QUAY_USERNAME', passwordVariable: 'QUAY_PASSWORD'),
                            ]) {
                                sh '''
                                docker login -u=${QUAY_USERNAME} -p=${QUAY_PASSWORD} quay.io
                                docker run --name oracledb -d -p 1521:1521  quay.io/rh_integration/dbz-oracle:${ORACLE_VERSION}
                                until docker inspect oracledb | grep '"Status": "healthy"'  2>/dev/null
                                do
                                    echo "waiting for database container to start" 
                                    sleep 10
                                done 
                                '''
                            }
                        }
                    }

                    stage('Prepare Tests') {
                        steps {
                            script {
                                env.MVN_PROP_PDB_NAME = env.ORACLE_VERSION.endsWith('noncbd') ? '-Ddatabase.pdb.name=' : ''
                            }
                            dir(env.ORACLE_ARTIFACT_DIR) {
                                sh '''
                                set -x
                                ORACLE_ARTIFACT_VERSION=$(cat ${WORKSPACE}/debezium/pom.xml | grep "^        <version\\.oracle\\.driver>.*</version\\.oracle\\.driver>$" | awk -F'[><]' '{print $3}')
                                ORACLE_ARTIFACT_DIR="${HOME}/oracle-libs/${ORACLE_ARTIFACT_VERSION}.0"
                                
                                mvn org.apache.maven.plugins:maven-dependency-plugin:2.8:get -Dartifact=io.apicurio:apicurio-registry-distro-connect-converter:${APICURIO_ARTIFACT_VERSION}:zip
                                cd ${ORACLE_ARTIFACT_DIR}
                                mvn install:install-file -DgroupId=com.oracle.instantclient -DartifactId=ojdbc8 -Dversion=${ORACLE_ARTIFACT_VERSION} -Dpackaging=jar -Dfile=ojdbc8.jar
                                mvn install:install-file -DgroupId=com.oracle.instantclient -DartifactId=xstreams -Dversion=${ORACLE_ARTIFACT_VERSION} -Dpackaging=jar -Dfile=xstreams.jar
                                '''
                            }
                        }
                    }

                    stage('Run Test') {
                        steps {
                            sh '''
                            cd ${WORKSPACE}/debezium
                            mvn clean install -U -s $HOME/.m2/settings-snapshots.xml -pl debezium-connector-oracle -am -fae \\
                                -Ddatabase.hostname=0.0.0.0                 \\
                                -Ddatabase.admin.hostname=0.0.0.0           \\
                                -Poracle,logminer                           \\
                                -Dinstantclient.dir=${HOME}/oracle-libs     \\
                                -Dmaven.test.failure.ignore=true            \\
                                -Dinsecure.repositories=WARN                \\
                                ${MVN_PROP_PDB_NAME}                        \\
                                ${MVN_PROFILE}
                            '''
                        }
                    }
                }

                post{
                    always {
                        sh '''
                        docker stop $(docker ps -a -q) || true
                        docker rm $(docker ps -a -q) || true
                        '''
                    }
                    success {
                        archiveArtifacts '**/target/*-reports/*.xml'
                        junit '**/target/*-reports/*.xml'
                    }
                }
            }
        }
    }

    post {
        always {
            mail to: MAIL_TO, subject: "Debezium OpenShift test run #${BUILD_NUMBER} finished", body: """
OpenShift interoperability test run ${BUILD_URL} finished with result: ${currentBuild.currentResult}
"""
        }
    }
}