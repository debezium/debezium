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
                checkout([
                        $class           : 'GitSCM',
                        branches         : [[name: "${DBZ_GIT_BRANCH}"]],
                        userRemoteConfigs: [[url: "${DBZ_GIT_REPOSITORY}"]],
                        extensions       : [[$class           : 'RelativeTargetDirectory',
                                             relativeTargetDir: 'debezium']],
                ])
            }
        }

        stage('Delete old snapshots') {
            steps {
                withCredentials([
                        file(credentialsId: "$OPENSTACK_AUTH", variable: 'OS_AUTH')
                ]) {
                    sh '''
                    cp ${OS_AUTH} ./clouds.yaml
                    cp ${WORKSPACE}/debezium/jenkins-jobs/scripts/cleanup_images_ansible.yml ./cleanup_images_ansible.yml
                    ansible-galaxy collection install openstack.cloud
                    ansible-playbook cleanup_images_ansible.yml --extra-vars "cloud_name="${CLOUD_NAME}" snapshot_name="${SNAPSHOT_NAME}""
                   '''
                }
            }
        }

    }
    post {
        always {
            mail to: 'debezium-qe@redhat.com', subject: "Jenkins node image snapshot history cleanup #${env.BUILD_NUMBER} finished with ${currentBuild.currentResult}", body: """
                ${currentBuild.projectName} run ${env.BUILD_URL} finished with result: ${currentBuild.currentResult}
                """
        }
    }
}
