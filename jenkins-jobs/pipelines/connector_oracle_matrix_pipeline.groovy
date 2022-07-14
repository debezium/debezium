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
                        values '21.3.0', '19.3.0', '19.3.0-noncdb', '12.2.0.1'
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
                            sh '''
                            set -x
                            cd ${WORKSPACE}/debezium
                            ORACLE_ARTIFACT_VERSION=$(mvn -s $HOME/.m2/settings-snapshots.xml -q -DforceStdout help:evaluate -Dexpression=version.oracle.driver ${MVN_PROFILE})
                            ORACLE_ARTIFACT_DIR="${HOME}/oracle-libs/${ORACLE_ARTIFACT_VERSION}.0"
                          
                            cd ${ORACLE_ARTIFACT_DIR}
                            mvn install:install-file -DgroupId=com.oracle.instantclient -DartifactId=ojdbc8 -Dversion=${ORACLE_ARTIFACT_VERSION} -Dpackaging=jar -Dfile=ojdbc8.jar
                            mvn install:install-file -DgroupId=com.oracle.instantclient -DartifactId=xstreams -Dversion=${ORACLE_ARTIFACT_VERSION} -Dpackaging=jar -Dfile=xstreams.jar
                            '''
                        }
                    }

                    stage('Run Test') {
                        steps {
                            script {
                                env.MVN_PROP_PDB_NAME = env.ORACLE_VERSION.endsWith('noncdb') ? '-Ddatabase.pdb.name=' : ''
                                env.MVN_PROP_DATABASE_NAME = env.ORACLE_VERSION.endsWith('noncdb') ? '-Ddatabase.dbname=ORCLCDB' : ''
                                env.MVN_PROP_USER_NAME = env.ORACLE_VERSION.endsWith('noncdb') ? 'dbzuser' : 'c##dbzuser'
                            }

                            sh '''
                            cd ${WORKSPACE}/debezium
                            mvn clean install -U -s $HOME/.m2/settings-snapshots.xml -pl debezium-connector-oracle -am -fae \\
                                -Poracle-tests                              \\
                                -Ddatabase.hostname=0.0.0.0                 \\
                                -Ddatabase.admin.hostname=0.0.0.0           \\
                                -Dinstantclient.dir=${HOME}/oracle-libs     \\
                                -Dmaven.test.failure.ignore=true            \\
                                -Dinsecure.repositories=WARN                \\
                                -Ddatabase.user=${MVN_PROP_USER_NAME}       \\
                                ${MVN_PROP_PDB_NAME}                        \\
                                ${MVN_PROP_DATABASE_NAME}                   \\
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
