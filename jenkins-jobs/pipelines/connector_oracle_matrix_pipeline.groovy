pipeline {
    agent {
        label 'Slave'
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

        stage('Matrix') {
            matrix {
                axes {
                    axis {
                        name 'ORACLE_VERSION'
                        values '19.3.0'
                    }
                }
                stages {
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
                                env.ORACLE_ARTIFACT_VERSION='21.1.0.0'
                                env.ORACLE_ARTIFACT_DIR = "${env.HOME}/oracle-libs/21.1.0.0.0"
                                env.MVN_PROP_PDB_NAME = env.ORACLE_VERSION.endsWith('noncbd') ? '-Ddatabase.pdb.name=' : ''
                            }
                            dir(env.ORACLE_ARTIFACT_DIR) {
                                sh "mvn install:install-file -DgroupId=com.oracle.instantclient -DartifactId=ojdbc8 -Dversion=${env.ORACLE_ARTIFACT_VERSION} -Dpackaging=jar -Dfile=ojdbc8.jar"
                                sh "mvn install:install-file -DgroupId=com.oracle.instantclient -DartifactId=xstreams -Dversion=${env.ORACLE_ARTIFACT_VERSION} -Dpackaging=jar -Dfile=xstreams.jar"
                            }
                        }
                    }

                    stage('Run Test') {
                        steps {
                            sh '''
                            cd debezium
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

            }
        }
    }

    post {
        always {
            sh '''
            docker stop $(docker ps -a -q)
            docker rm $(docker ps -a -q)
            '''
            archiveArtifacts '**/target/failsafe-reports/*.xml'
            junit '**/target/failsafe-reports/*.xml'

            mail to: MAIL_TO, subject: "Debezium OpenShift test run #${BUILD_NUMBER} finished", body: """
OpenShift interoperability test run ${BUILD_URL} finished with result: ${currentBuild.currentResult}
"""
        }
    }
}