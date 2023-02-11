pipeline {
    agent {
        label 'debezium-ci-tools'
    }
    environment {
        ARO_TEMPLATE_PATH = "${env.WORKSPACE}/ARO-dbz/aro-configuration/template.json"
        ARO_PARAMETERS_PATH = "${env.WORKSPACE}/ARO-dbz/aro-configuration/parameters.json"
        SR_CREDENTIALS = "aro-service-account"
    }
    stages {
        stage('Login to Azure') {
            steps {
                container("debezium-ci-tools") {
                    withCredentials([
                            usernamePassword(credentialsId: "${SR_CREDENTIALS}", passwordVariable: 'password', usernameVariable: 'username')
                    ]) {
                        sh('az login --service-principal -u ${username} -p ${password} --tenant 520cf09d-78ff-44ed-a731-abd623e73b09')
                    }
                }
            }
        }
        stage('Delete cluster') {
            steps {
                container("debezium-ci-tools") {
                    script {
                        sh("az aro delete --name ${env.CLUSTER_NAME} --resource-group ${env.RESOURCE_GROUP} -y")
                        sh("az network vnet delete --name dbz-ARO-vnet --resource-group ${env.RESOURCE_GROUP}")
                        sh("az network watcher configure --resource-group NetworkWatcherRG --locations eastus --enabled false")
                    }
                }
            }
        }
    }
    post {
        always {
            script {
                mail to: 'debezium-qe@redhat.com', subject: "ARO cluster teardown #${env.BUILD_NUMBER} finished with ${currentBuild.currentResult}",
                        body: """
                        ${currentBuild.projectName} run ${env.BUILD_URL} finished with result: ${currentBuild.currentResult}
                        """
            }
        }
    }
}

