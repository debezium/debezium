pipeline {
    environment {
        OSIA_GIT_SECRET = "ocp-deployment-repo"
        OSIA_GIT_BRANCH = "persistence"
        ANSIBLE_VAULT_PASSWORD = "ansible-vault-password"
        GITLAB_CREDENTIALS = "gitlab-debeziumci-ssh"
    }
    agent {
        label 'debezium-ci-tools'
    }
    stages {
        stage('Checkout and prepare git') {
            steps {
                container("debezium-ci-tools") {
                    script {
                            withCredentials([
                                    string(credentialsId: "${env.OSIA_GIT_SECRET}", variable: 'OSIA_GIT_REPOSITORY')
                            ]) {
                                checkout([
                                        $class           : 'GitSCM',
                                        branches         : [[name: "${env.OSIA_GIT_BRANCH}"]],
                                        userRemoteConfigs: [[url: "${OSIA_GIT_REPOSITORY}",
                                                             credentialsId: "${env.GITLAB_CREDENTIALS}"]],
                                        extensions       : [[$class: 'CleanCheckout'],
                                                            [$class: 'RelativeTargetDirectory',
                                                             relativeTargetDir: 'OSIA-dbz']] +
                                                [[$class: 'CloneOption', noTags: false, depth: 1, reference: '', shallow: true]],
                                        submoduleCfg     : [],
                                        doGenerateSubmoduleConfigurations: false,
                                ])
                            }
                    }
                    script {
                        withCredentials([
                                sshUserPrivateKey(credentialsId: "${env.GITLAB_CREDENTIALS}", keyFileVariable: 'TMP_SSH_KEY_FILE')
                        ]){
                            sh '''
                                cd "${WORKSPACE}/OSIA-dbz/"
                                ls
                                git status
                                git config --local user.name "Debezium CI"
                                git config --local user.email "debezium-qe@redhat.com"
                                cp ${TMP_SSH_KEY_FILE} SSH_KEY_FILE
                                git config --add --local core.sshCommand "ssh -i $(readlink -f SSH_KEY_FILE)"
                                git checkout ${OSIA_GIT_BRANCH}
                            '''
                        }
                    }
                }
            }
        }
        stage("Setup AWS credentials") {
            steps {
                container("debezium-ci-tools") {
                    withCredentials([string(credentialsId: "awscredentials", variable: "AWS_SHARED_CREDENTIALS_FILE")]) {
                        sh(script: "mkdir -p /home/jenkins/.aws")
                        sh(script: "echo -e \"${AWS_SHARED_CREDENTIALS_FILE}\" > /home/jenkins/.aws/credentials")
                    }
                }
            }
        }
        stage('Deploy_cluster') {
            when() {
                expression { !params.REMOVE_CLUSTER }
            }
            steps {
                container("debezium-ci-tools") {
                    withCredentials([
                            string(credentialsId: "${env.ANSIBLE_VAULT_PASSWORD}", variable: 'ANSIBLE_PASSWORD')
                    ]) {
                        sh '''
                            set -ex
                            cd "${WORKSPACE}/OSIA-dbz/secrets"
                            echo "${ANSIBLE_PASSWORD}" > ../password.txt
                            ansible-vault decrypt --vault-password-file ../password.txt *
                            cd ..
                            mv ./secrets/* ./
                            if [[ $CLOUD == "openstack" ]] ; then export CLOUD_ENV=" --cloud-env psi" ; else export CLOUD_ENV="" ; fi
                            if [[ $CLOUD == "openstack" ]] ; then export DNS_PROVIDER="--dns-provider route53" ; else export DNS_PROVIDER="" ; fi
                            export AWS_SHARED_CREDENTIALS_FILE=/home/jenkins/.aws/credentials && osia install --cluster-name ${CLUSTER_NAME} --cloud ${CLOUD} --installer-version ${INSTALLER_VERSION} ${DNS_PROVIDER} ${CLOUD_ENV}
                            export KUBECONFIG="${WORKSPACE}/OSIA-dbz/${CLUSTER_NAME}/auth/kubeconfig"
                            oc create secret generic htpass-secret --from-file=htpasswd=ocp-users.htpasswd -n openshift-config
                            oc apply -f htpasswd.cr.yaml -n openshift-config
                            oc adm policy add-cluster-role-to-user cluster-admin debezium
                        '''
                    }
                }
            }
        }

        stage('Remove_cluster') {
            when() {
                expression { params.REMOVE_CLUSTER }
            }
            steps {
                container("debezium-ci-tools") {
                    withCredentials([
                            string(credentialsId: "${env.ANSIBLE_VAULT_PASSWORD}", variable: 'ANSIBLE_PASSWORD')
                    ]) {
                        sh '''
                            set -ex
                            cd "${WORKSPACE}/OSIA-dbz/secrets"
                            echo "${ANSIBLE_PASSWORD}" > ../password.txt
                            ansible-vault decrypt --vault-password-file ../password.txt *
                            cd ..
                            mv ./secrets/* ./
                            export AWS_SHARED_CREDENTIALS_FILE=/home/jenkins/.aws/credentials && osia clean --cluster-name ${CLUSTER_NAME} --installer-version ${INSTALLER_VERSION}
                        '''
                    }
                }
            }
        }
    }

    post {
        always {
            mail to: 'debezium-qe@redhat.com', subject: "OCP cluster deployment/removal #${env.BUILD_NUMBER} finished with ${currentBuild.currentResult}", body: """
                ${currentBuild.projectName} run ${env.BUILD_URL} finished with result: ${currentBuild.currentResult}
            """
        }
    }
}
