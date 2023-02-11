pipeline {
    agent {
        label 'debezium-ci-tools'
    }
    environment {
        ARO_TEMPLATE_PATH = "${env.WORKSPACE}/ARO-dbz/aro-configuration/template.json"
        ARO_PARAMETERS_PATH = "${env.WORKSPACE}/ARO-dbz/aro-configuration/parameters.json"
        ARO_GIT_SECRET = "ocp-deployment-repo"
        ARO_GIT_BRANCH = "main"
        GITLAB_CREDENTIALS = "gitlab-debeziumci-ssh"
        SR_CREDENTIALS = "aro-service-account"
        PULL_SECRET = "ocp-pull-secret-json"
        ANSIBLE_VAULT_PASSWORD = "ansible-vault-password"
    }
    stages {
        stage("Checkout ARO configuration repo") {
            steps {
                container("debezium-ci-tools") {
                    script {
                        withCredentials([
                                string(credentialsId: "${ARO_GIT_SECRET}", variable: 'TMP_ARO_GIT_REPOSITORY')
                        ]) { env.ARO_GIT_REPOSITORY = TMP_ARO_GIT_REPOSITORY }
                    }
                    checkout([
                            $class                           : 'GitSCM',
                            branches                         : [[name: "${env.ARO_GIT_BRANCH}"]],
                            userRemoteConfigs                : [[url: "${env.ARO_GIT_REPOSITORY}",
                                                                 credentialsId: "${GITLAB_CREDENTIALS}"]],
                            extensions                       : [[$class: 'CleanCheckout'],
                                                                [$class: 'RelativeTargetDirectory',
                                                                 relativeTargetDir: 'ARO-dbz']] +
                                    [[$class: 'CloneOption', noTags: false, depth: 1, reference: '', shallow: true]],
                            submoduleCfg                     : [],
                            doGenerateSubmoduleConfigurations: false,
                    ])
                }
            }
        }
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
        stage('Install cluster') {
            steps {
                container("debezium-ci-tools") {
                    script {
                        withCredentials([
                                usernamePassword(credentialsId: "${SR_CREDENTIALS}", passwordVariable: 'password', usernameVariable: 'username'),
                                file(credentialsId: "${PULL_SECRET}", variable: 'secret_path')
                        ]) {
                            sh '''
                                az group create -l eastus -n ${RESOURCE_GROUP}
                                az deployment group create \
                                --name ${CLUSTER_NAME} \
                                --template-file ${ARO_TEMPLATE_PATH} \
                                --parameters ${ARO_PARAMETERS_PATH} \
                                --resource-group ${RESOURCE_GROUP} \
                                --parameters aadClientSecret=${password} pullSecret="$(cat ${secret_path})" \
                                clusterName=${CLUSTER_NAME} domain=${DOMAIN} aadClientId=${username}
                            '''
                        }
                    }
                }
            }
        }
        stage('Get cluster description') {
            steps {
                container("debezium-ci-tools") {
                    script {
                        sh("az aro show --name ${env.CLUSTER_NAME} --resource-group ${env.RESOURCE_GROUP} > aro_info.txt")
                    }
                }
            }
        }
        stage('Get admin credentials and URLs') {
            steps {
                container("debezium-ci-tools") {
                    script {
                        env.ADMIN_USER = "kubeadmin"
                        env.ADMIN_PASS = sh(script: "az aro list-credentials --name ${env.CLUSTER_NAME} " +
                                "--resource-group ${env.RESOURCE_GROUP} | jq -r '.kubeadminPassword'", returnStdout: true).trim()
                        env.API_URL = sh(script: "cat aro_info.txt | jq -r '.apiserverProfile.url'", returnStdout: true).trim()
                        env.CONSOLE_URL = sh(script: "cat aro_info.txt | jq -r '.consoleProfile.url'", returnStdout: true).trim()

                        println("[INFO] username: ${env.ADMIN_USER}")
                        println("[INFO] password: ${env.ADMIN_PASS}")
                        println("[INFO] API url: ${env.API_URL}")
                        println("[INFO] Console url: ${env.CONSOLE_URL}")
                    }
                }
            }
        }
        stage('Setup Debezium credentials') {
            steps {
                container("debezium-ci-tools") {
                    withCredentials([
                            string(credentialsId: "${env.ANSIBLE_VAULT_PASSWORD}", variable: 'ANSIBLE_PASSWORD')
                    ]) {
                        sh '''
                        set -ex
                        cd "${WORKSPACE}/ARO-dbz/secrets"
                        echo "${ANSIBLE_PASSWORD}" > ../password.txt
                        ansible-vault decrypt --vault-password-file ../password.txt *
                        cd ..
                        mv ./secrets/* ./
                        oc login ${API_URL} -u ${ADMIN_USER} -p ${ADMIN_PASS} --insecure-skip-tls-verify=true
                        oc create secret generic htpass-secret --from-file=htpasswd=ocp-users.htpasswd -n openshift-config
                        oc apply -f htpasswd.cr.yaml -n openshift-config
                        oc adm policy add-cluster-role-to-user cluster-admin debezium
                        '''
                    }
                }
            }
        }
    }
    post {
        failure {
            build job: 'ocp-aro-teardown', parameters: [
                    string(name: 'CLUSTER_NAME', value: params.CLUSTER_NAME),
                    string(name: 'RESOURCE_GROUP', value: params.RESOURCE_GROUP),
            ]
        }
        always {
            script {
                mail to: 'debezium-qe@redhat.com', subject: "ARO cluster deployment #${env.BUILD_NUMBER} finished with ${currentBuild.currentResult}",
                        body: """
                        ${currentBuild.projectName} run ${env.BUILD_URL} finished with result: ${currentBuild.currentResult}
                        """
                archiveArtifacts "**/aro_info.txt"
            }
        }
    }
}

