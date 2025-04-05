import groovy.json.*
import java.util.stream.*

import com.cloudbees.groovy.cps.NonCPS

@Library("dbz-libs") _

if (
        !RELEASE_VERSION ||
        !DEBEZIUM_OPERATOR_REPOSITORY ||
        !DEBEZIUM_OPERATOR_BRANCH ||
        !DEBEZIUM_PLATFORM_REPOSITORY ||
        !DEBEZIUM_PLATFORM_BRANCH ||
        !DEBEZIUM_CHART_REPOSITORY ||
        !DEBEZIUM_CHART_BRANCH ||
        !OCI_ARTIFACT_REPO_URL
) {
    error 'Input parameters not provided'
}

DRY_RUN = common.getDryRun()

GIT_CREDENTIALS_ID = 'debezium-github'
QUAYIO_CREDENTIALS_ID = 'debezium-charts-quay'
HOME_DIR = '/home/cloud-user'
GPG_DIR = 'gpg'
GITHUB_CLI_VERSION= '2.67.0'

// Helm uses the semantic version format 3.1.0-final instead of the one used by Debezium 3.1.0.Final
RELEASE_SEM_VERSION=RELEASE_VERSION.replaceAll(/\.(?=[^.]*$)/, '-').toLowerCase()

MAVEN_CENTRAL = 'https://repo1.maven.org/maven2'

DEBEZIUM_OPERATOR_DIR='operator'
DEBEZIUM_PLATFORM_DIR='platform'
DEBEZIUM_CHARTS_DIR='charts'
HELM_CHART_OUTPUT_DIR='charts-output'
DEBEZIUM_CHART_URL='charts.debezium.io'


node('Slave') {
    catchError {
        stage('Validate parameters') {
            common.validateVersionFormat(RELEASE_VERSION)
        }

        stage('Initialize') {
            dir('.') {
                deleteDir()
                sh "git config user.email || git config --global user.email \"debezium@gmail.com\" && git config --global user.name \"Debezium Builder\""
                sh "ssh-keyscan github.com >> $HOME_DIR/.ssh/known_hosts"

                sh "mkdir ${WORKSPACE}/${HELM_CHART_OUTPUT_DIR}"
                sh "mkdir ${WORKSPACE}/${HELM_CHART_OUTPUT_DIR}/debezium-operator"
                sh "mkdir ${WORKSPACE}/${HELM_CHART_OUTPUT_DIR}/debezium-platform"
            }
            dir(GPG_DIR) {
                withCredentials([
                        string(credentialsId: 'debezium-ci-gpg-passphrase', variable: 'PASSPHRASE'),
                        [$class: 'FileBinding', credentialsId: 'debezium-ci-secret-key', variable: 'SECRET_KEY_FILE']]) {
                    echo 'Creating GPG directory'
                    def gpglog = sh(script: "gpg --import --batch --passphrase $PASSPHRASE --homedir . $SECRET_KEY_FILE", returnStdout: true).trim()
                    echo gpglog
                }
            }
            checkout([$class                           : 'GitSCM',
                      branches                         : [[name: "*/$DEBEZIUM_OPERATOR_BRANCH"]],
                      doGenerateSubmoduleConfigurations: false,
                      extensions                       : [[$class: 'RelativeTargetDirectory', relativeTargetDir: DEBEZIUM_OPERATOR_DIR], [$class: 'CloneOption', noTags: false, depth: 1]],
                      submoduleCfg                     : [],
                      userRemoteConfigs                : [[url: "https://$DEBEZIUM_OPERATOR_REPOSITORY", credentialsId: GIT_CREDENTIALS_ID]]
            ]
            )

            checkout([$class                           : 'GitSCM',
                      branches                         : [[name: "*/$DEBEZIUM_PLATFORM_BRANCH"]],
                      doGenerateSubmoduleConfigurations: false,
                      extensions                       : [[$class: 'RelativeTargetDirectory', relativeTargetDir: DEBEZIUM_PLATFORM_DIR], [$class: 'CloneOption', noTags: false, depth: 1]],
                      submoduleCfg                     : [],
                      userRemoteConfigs                : [[url: "https://$DEBEZIUM_PLATFORM_REPOSITORY", credentialsId: GIT_CREDENTIALS_ID]]
            ]
            )

            checkout([$class                           : 'GitSCM',
                      branches                         : [[name: "*/$DEBEZIUM_CHART_BRANCH"]],
                      doGenerateSubmoduleConfigurations: false,
                      extensions                       : [[$class: 'RelativeTargetDirectory', relativeTargetDir: DEBEZIUM_CHARTS_DIR], [$class: 'CloneOption', noTags: false, depth: 1]],
                      submoduleCfg                     : [],
                      userRemoteConfigs                : [[url: "https://$DEBEZIUM_CHART_REPOSITORY", credentialsId: GIT_CREDENTIALS_ID]]
            ]
            )
        }

        stage("Install helm") {
            sh 'curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3'
            sh 'chmod 700 get_helm.sh'
            sh './get_helm.sh'
            sh 'helm version'
        }

        stage("Install GitHub CLI") {
            sh "curl -fLjsO https://github.com/cli/cli/releases/download/v${GITHUB_CLI_VERSION}/gh_${GITHUB_CLI_VERSION}_linux_amd64.tar.gz"
            sh "tar -xvzf gh_${GITHUB_CLI_VERSION}_linux_amd64.tar.gz --one-top-level=gh-cli --strip-components=1"
            sh 'sudo mv gh-cli/bin/gh /usr/local/bin'
            sh 'gh --version'
        }

        def TMP_WORKDIR = sh(script: 'mktemp -d', returnStdout: true).trim()

        stage('Release operator chart') {
            echo "=== Downloading Debezium operator chart ==="
            def INPUT_URL = "$MAVEN_CENTRAL/io/debezium/debezium-operator-dist/$RELEASE_VERSION/debezium-operator-dist-$RELEASE_VERSION-helm-chart.tar.gz"

            dir(TMP_WORKDIR) {
                sh(
                        label: 'Download and verify helm chart',
                        script: """
                            echo "Input url: $INPUT_URL"
                            curl -fLjs -o "debezium-operator-${RELEASE_SEM_VERSION}.tar.gz" "$INPUT_URL"
                        """
                )

                sh(     label: 'Unzip and repackage',
                        script: """
                            tar -xvzf debezium-operator-${RELEASE_SEM_VERSION}.tar.gz --one-top-level=debezium-operator-${RELEASE_SEM_VERSION} --strip-components=1
                            helm package debezium-operator-${RELEASE_SEM_VERSION}
                            cp debezium-operator-${RELEASE_SEM_VERSION}.tgz ${WORKSPACE}/${HELM_CHART_OUTPUT_DIR}/debezium-operator
                        """
                )

            }

            stage('Create a GH release') {
                dir(DEBEZIUM_OPERATOR_DIR) {
                    if (!DRY_RUN) {
                        withCredentials([usernamePassword(credentialsId: GIT_CREDENTIALS_ID, passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME')]) {
                            withEnv(["GH_TOKEN=$GIT_PASSWORD"]) {
                                sh "gh release create v${RELEASE_VERSION} --verify-tag  -t 'Debezium Operator Chart v${RELEASE_VERSION}' --latest '$TMP_WORKDIR/debezium-operator-${RELEASE_SEM_VERSION}.tgz'"
                            }
                        }
                    }
                }
            }

            stage('Pushing chart to quay.io') {
                withCredentials([string(credentialsId: QUAYIO_CREDENTIALS_ID, variable: 'USERNAME_PASSWORD')]) {
                    def credentials = USERNAME_PASSWORD.split(':')
                    sh """
                        set +x
                        helm registry login -u ${credentials[0]} -p ${credentials[1]} quay.io
                    """
                }
                if (!DRY_RUN) {
                    sh "helm push $TMP_WORKDIR/debezium-operator-${RELEASE_SEM_VERSION}.tgz $OCI_ARTIFACT_REPO_URL"
                }
            }

        }

        stage('Release platform chart') {

            dir(DEBEZIUM_PLATFORM_DIR) {
                echo "Update version for chart dependency"
                dir("helm/charts/database") {
                    fileUtils.modifyFile("Chart.yaml") {
                        it.replaceFirst(/version: .*/, "version: \"${RELEASE_SEM_VERSION}\"")
                    }
                }

                dir("helm") {
                    def modifyVersions = { content ->
                        def updatedContent = content

                        // Replace operator version
                        updatedContent = updatedContent.replaceAll(
                                /(name: debezium-operator.*?\n\s+version: )".*?"/,
                                "\$1\"${RELEASE_SEM_VERSION}\""
                        )

                        // Replace database version
                        updatedContent = updatedContent.replaceAll(
                                /(name: database.*?\n\s+version: ).*/,
                                "\$1\"${RELEASE_SEM_VERSION}\""
                        )

                        return updatedContent
                    }
                    fileUtils.modifyFile("Chart.yaml", modifyVersions)

                    def modifyImages = { content ->

                        return content.replaceAll(
                                /nightly/,
                                "${RELEASE_VERSION}"
                        )

                    }

                    fileUtils.modifyFile("values.yaml", modifyImages)
                }


                sh "mv $TMP_WORKDIR/debezium-operator-${RELEASE_SEM_VERSION}.tgz helm/charts"
                sh "helm show chart helm/charts/debezium-operator-${RELEASE_SEM_VERSION}.tgz"
                sh "helm package --app-version=${RELEASE_SEM_VERSION} --version=${RELEASE_SEM_VERSION} helm/"
                sh "cp debezium-platform-${RELEASE_SEM_VERSION}.tgz ${WORKSPACE}/${HELM_CHART_OUTPUT_DIR}/debezium-platform"

                stage('Create a GH release') {
                    if (!DRY_RUN) {
                        withCredentials([usernamePassword(credentialsId: GIT_CREDENTIALS_ID, passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME')]) {
                            withEnv(["GH_TOKEN=$GIT_PASSWORD"]) {
                                sh "gh release create v${RELEASE_VERSION} --verify-tag  -t 'Debezium Platform Chart v${RELEASE_VERSION}' --latest 'debezium-platform-${RELEASE_SEM_VERSION}.tgz'"
                            }
                        }
                    }
                }

                stage('Pushing chart to quay.io') {
                    withCredentials([string(credentialsId: QUAYIO_CREDENTIALS_ID, variable: 'USERNAME_PASSWORD')]) {
                        def credentials = USERNAME_PASSWORD.split(':')
                        sh """
                            set +x
                            helm registry login -u ${credentials[0]} -p ${credentials[1]} quay.io
                        """
                    }
                    if (!DRY_RUN) {
                        sh "helm push debezium-platform-${RELEASE_SEM_VERSION}.tgz $OCI_ARTIFACT_REPO_URL"
                    }
                }
            }
        }

        stage("Publish charts to ${DEBEZIUM_CHART_URL}") {

            dir(DEBEZIUM_CHARTS_DIR) {
                sh "helm repo index ${WORKSPACE}/${HELM_CHART_OUTPUT_DIR}/debezium-operator --merge ./index.yaml --url https://github.com/debezium/debezium-operator/releases/download/v${RELEASE_VERSION}"
                sh "cp ${WORKSPACE}/${HELM_CHART_OUTPUT_DIR}/debezium-operator/index.yaml index.yaml"
                sh "helm repo index ${WORKSPACE}/${HELM_CHART_OUTPUT_DIR}/debezium-platform --merge ./index.yaml --url https://github.com/debezium/debezium-platform/releases/download/v${RELEASE_VERSION}"
                sh "cp ${WORKSPACE}/${HELM_CHART_OUTPUT_DIR}/debezium-platform/index.yaml index.yaml"
                if (!DRY_RUN) {
                    withCredentials([usernamePassword(credentialsId: GIT_CREDENTIALS_ID, passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME')]) {
                        sh "git commit -a -m '[release] Stable $RELEASE_VERSION for Debezium Charts'"
                        sh "git push https://\${GIT_USERNAME}:\${GIT_PASSWORD}@${DEBEZIUM_CHART_REPOSITORY} HEAD:${DEBEZIUM_CHART_BRANCH}"
                        sh "git tag v$RELEASE_VERSION && git push \"https://\${GIT_USERNAME}:\${GIT_PASSWORD}@${DEBEZIUM_CHART_REPOSITORY}\" v$RELEASE_VERSION"
                    }
                }
            }
        }
    }

    mail to: MAIL_TO, subject: "${JOB_NAME} run #${BUILD_NUMBER} finished with ${currentBuild.currentResult}", body: "Run ${BUILD_URL} finished with result: ${currentBuild.currentResult}"
}