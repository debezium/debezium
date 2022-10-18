pipeline {
    agent {
        label 'Fresh'
    }

    stages {
        stage('CleanWorkspace') {
            steps {
                cleanWs()
            }
        }

        stage('Checkout and prepare git') {
            steps {
                script {
                    if (params.OSIA_GIT_REPOSITORY.isEmpty()) {
                        withCredentials([
                                string(credentialsId: "${params.OSIA_GIT_SECRET}", variable: 'TMP_OSIA_GIT_REPOSITORY')
                        ]){env.OSIA_GIT_REPOSITORY = TMP_OSIA_GIT_REPOSITORY}
                    }
                }
                checkout([
                        $class           : 'GitSCM',
                        branches         : [[name: "${params.OSIA_GIT_BRANCH}"]],
                        userRemoteConfigs: [[url: "${params.OSIA_GIT_REPOSITORY}",
                                             credentialsId: "${params.GITLAB_CREDENTIALS}"]],
                        extensions       : [[$class: 'CleanCheckout'],
                                            [$class: 'RelativeTargetDirectory',
                                             relativeTargetDir: 'OSIA-dbz']] +
                                [[$class: 'CloneOption', noTags: false, depth: 1, reference: '', shallow: true]],
                        submoduleCfg     : [],
                        doGenerateSubmoduleConfigurations: false,
                ])
                script {
                    withCredentials([
                            sshUserPrivateKey(credentialsId: "${params.GITLAB_CREDENTIALS}", keyFileVariable: 'TMP_SSH_KEY_FILE')
                    ]){
                        sh '''
                        git config --global user.name "Debezium CI"
                        git config --global user.email "debezium-qe@redhat.com"
                        cp ${TMP_SSH_KEY_FILE} SSH_KEY_FILE
                        git config --add --global core.sshCommand "ssh -i $(readlink -f SSH_KEY_FILE)"
                        cd "${WORKSPACE}/OSIA-dbz/"
                        git checkout ${OSIA_GIT_BRANCH}
                        '''
                    }
                }
            }
        }

        stage('Deploy_cluster') {
            when() {
                expression { !params.REMOVE_CLUSTER }
            }
            steps {
                withCredentials([
                        string(credentialsId: "${params.ANSIBLE_VAULT_PASSWORD}", variable: 'ANSIBLE_PASSWORD')
                ]) {
                    sh '''
                    set -ex
                    cd "${WORKSPACE}/OSIA-dbz/secrets"
                    echo "${ANSIBLE_PASSWORD}" > ../password.txt
                    ansible-vault decrypt --vault-password-file ../password.txt *
                    cd ..
                    mv ./secrets/* ./
                    osia install --cluster-name ${CLUSTER_NAME} --cloud aws --installer-version ${INSTALLER_VERSION} --cloud-env dbz-aws
                    export KUBECONFIG="${WORKSPACE}/OSIA-dbz/${CLUSTER_NAME}/auth/kubeconfig"
                    oc create secret generic htpass-secret --from-file=htpasswd=ocp-users.htpasswd -n openshift-config
                    oc apply -f htpasswd.cr.yaml -n openshift-config
                    oc adm policy add-cluster-role-to-user cluster-admin debezium
                    '''
                }
            }
        }

        stage('Remove_cluster') {
            when() {
                expression { params.REMOVE_CLUSTER }
            }
            steps {
                withCredentials([
                        string(credentialsId: "${params.ANSIBLE_VAULT_PASSWORD}", variable: 'ANSIBLE_PASSWORD')
                ]) {
                    sh '''
                    set -ex
                    cd "${WORKSPACE}/OSIA-dbz/secrets"
                    echo "${ANSIBLE_PASSWORD}" > ../password.txt
                    ansible-vault decrypt --vault-password-file ../password.txt *
                    cd ..
                    mv ./secrets/* ./
                    osia clean --cluster-name ${CLUSTER_NAME} --installer-version ${INSTALLER_VERSION}
                    '''
                }
            }
        }
    }

    post {
        always {
            mail to: 'debezium-qe@redhat.com', subject: "OCP cluster deployment/removal #${env.BUILD_NUMBER} finished", body: """
${currentBuild.projectName} run ${env.BUILD_URL} finished with result: ${currentBuild.currentResult}
"""
        }
    }
}
