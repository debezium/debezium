pipeline {
    agent {
        label 'Core'
    }

    stages {
        stage('CleanWorkspace') {
            steps {
                cleanWs()
            }
        }

        stage('Checkout') {
            steps {
                script {
                    if (ANS_GIT_REPOSITORY.isEmpty()) {
                        withCredentials([
                                string(credentialsId: "${ANS_GIT_SECRET}", variable: 'TMP_ANS_GIT_REPOSITORY')
                        ]){env.ANS_GIT_REPOSITORY = TMP_ANS_GIT_REPOSITORY}
                    }
                }
                checkout([
                        $class           : 'GitSCM',
                        branches         : [[name: "${ANS_GIT_BRANCH}"]],
                        userRemoteConfigs: [[url: "${ANS_GIT_REPOSITORY}",
                                             credentialsId: "${GITLAB_CREDENTIALS}"]],
                        extensions       : [[$class           : 'RelativeTargetDirectory',
                                             relativeTargetDir: 'ci-jenkins-node']],
                ])
            }
        }

        stage('Build image snapshot') {
            steps {
                withCredentials([
                        string(credentialsId: "${ANSIBLE_VAULT_PASSWORD}", variable: 'ANSIBLE_PASSWORD')
                ]) {
                    sh '''
                    set -x  
                    cd "${WORKSPACE}/ci-jenkins-node"
                    echo "${ANSIBLE_PASSWORD}" > password.txt
                    ansible-vault decrypt --vault-password-file ./password.txt clouds.yaml
                    sudo cp roles/os_node_snapshot/files/CA-RH-NEW.crt /etc/pki/ca-trust/source/anchors/CA-RH-NEW.crt
                    sudo update-ca-trust
                    ansible-galaxy collection install -r requirements.yml
                    ansible-galaxy install -r requirements.yml
                    export ANSIBLE_HOST_KEY_CHECKING=false
                    ansible-playbook create_jenkins_node_snapshot.yml --vault-password-file ./password.txt \\
                        --extra-vars \\
                        "snapshot_name="${SNAPSHOT_NAME}" \\
                         os_base_image="${BASE_IMAGE}" \\
                         os_name="${INSTANCE_NAME}" \\
                         slave_user="${INSTANCE_USER}" os_keypair="${KEYPAIR}" \\
                         ssh_keypair_path="~/.ssh/id_rsa" \\
                         git_repo="${ANS_GIT_REPOSITORY}" \\
                         os_cloud="${CLOUD_NAME}" \\
                         git_branch="${ANS_GIT_BRANCH}""
                    '''
                }
            }
        }
    }

    post {
        always {
            mail to: 'debezium-qe@redhat.com', subject: "Jenkins node image snapshot #${env.BUILD_NUMBER} finished with ${currentBuild.currentResult}", body: """
${currentBuild.projectName} run ${env.BUILD_URL} finished with result: ${currentBuild.currentResult}
"""
        }
    }
}
