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
                        usernamePassword(credentialsId: "${DOCKERHUB_CREDENTIALS}", usernameVariable: 'DOCKERHUB_USERNAME', passwordVariable: 'DOCKERHUB_PASSWORD'),
                        file(credentialsId: "${OPENSTACK_CREDENTIALS}",variable: 'CLOUDS_YAML')
                ]) {
                    sh '''
                    set -x
                    cp "${CLOUDS_YAML}" "${WORKSPACE}/ci-jenkins-node/clouds.yaml"
                    cd "${WORKSPACE}/ci-jenkins-node"
                    ansible-galaxy collection install -r requirements.yml
                    ansible-galaxy install -r requirements.yml
                    export ANSIBLE_HOST_KEY_CHECKING=false
                    ansible-playbook create_jenkins_node_snapshot.yml --extra-vars \\
                        "snapshot_name="${SNAPSHOT_NAME}" \\
                         os_base_image="${BASE_IMAGE}" \\
                         os_name="${INSTANCE_NAME}" \\
                         slave_user="${INSTANCE_USER}" os_keypair="${KEYPAIR}" \\
                         ssh_keypair_path="~/.ssh/id_rsa" \\
                         git_repo="${ANS_GIT_REPOSITORY}" \\
                         dockerhub_login_name="${DOCKERHUB_USERNAME}" dockerhub_login_pass="${DOCKERHUB_PASSWORD}" \\
                         os_cloud="${CLOUD_NAME}" \\
                         git_branch="${ANS_GIT_BRANCH}""
                    '''
                }
            }
        }
    }

    post {
        always {
            mail to: 'debezium-qe@redhat.com', subject: "Jenkins node image snapshot #${BUILD_NUMBER} finished", body: """
${currentBuild.projectName} run ${BUILD_URL} finished with result: ${currentBuild.currentResult}
"""
        }
    }
}
