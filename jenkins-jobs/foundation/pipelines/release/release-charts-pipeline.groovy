import groovy.json.*
import groovy.transform.Field
import java.util.stream.*

import com.cloudbees.groovy.cps.NonCPS

@Library('dbz-libs') _

properties([
    parameters([
        string(name: 'RELEASE_VERSION'),
        string(name: 'DEBEZIUM_OPERATOR_REPOSITORY'),
        string(name: 'DEBEZIUM_OPERATOR_BRANCH'),
        string(name: 'DEBEZIUM_PLATFORM_REPOSITORY'),
        string(name: 'DEBEZIUM_PLATFORM_BRANCH'),
        string(name: 'DEBEZIUM_CHART_REPOSITORY'),
        string(name: 'DEBEZIUM_CHART_BRANCH'),
        string(name: 'OCI_ARTIFACT_REPO_URL'),
        string(name: 'ZULIP_TO'),
        booleanParam(name: 'DRY_RUN')
    ])
])

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

// Configure 1Password CLI, the service account and secrets provided
@Field final ONE_PASSWORD_CONFIG = [
    serviceAccountCredentialId: 'sa-onepassword',
    opCLIPath: '/usr/bin'
]

@Field final SECRETS = [
    [envVar: 'GPG_PRIVATE_KEY', secretRef: 'op://Debezium Secrets Limited/Maven secret key/add more/GPG Private key'],
    [envVar: 'GPG_PASSPHRASE', secretRef: 'op://Debezium Secrets Limited/Maven secret key/password'],
    [envVar: 'GITHUB_USERNAME', secretRef: 'op://Debezium Secrets Limited/GitHub/username'],
    [envVar: 'GITHUB_PASSWORD', secretRef: 'op://Debezium Secrets Limited/GitHub/write token'],
    [envVar: 'QUAYIO_USERNAME', secretRef: 'op://Debezium Secrets Limited/Quay.io Charts/username'],
    [envVar: 'QUAYIO_PASSWORD', secretRef: 'op://Debezium Secrets Limited/Quay.io Charts/password'],
    [envVar: 'ZULIPBOT_USERNAME', secretRef: 'op://Debezium Secrets Limited/Zulip Jenkins Bot/username'],
    [envVar: 'ZULIPBOT_TOKEN', secretRef: 'op://Debezium Secrets Limited/Zulip Jenkins Bot/password']
]

@Field final GIT_CREDENTIALS_ID = 'debezium-github'
@Field final QUAYIO_CREDENTIALS_ID = 'debezium-charts-quay'
@Field final HOME_DIR = '/var/lib/jenkins'
@Field final GPG_DIR = 'gpg'
@Field final GITHUB_CLI_VERSION = '2.67.0'

@Field final DEBEZIUM_OPERATOR_DIR = 'operator'
@Field final DEBEZIUM_PLATFORM_DIR = 'platform'
@Field final DEBEZIUM_CHARTS_DIR = 'charts'
@Field final HELM_CHART_OUTPUT_DIR = 'charts-output'
@Field final DEBEZIUM_CHART_URL = 'charts.debezium.io'

@Field final MAVEN_CENTRAL = 'https://repo1.maven.org/maven2'

@Field DRY_RUN
@Field RELEASE_VERSION
@Field RELEASE_SEM_VERSION
@Field VERSION_TAG
@Field ZULIP_TO

@Field final ZULIP_URL = 'https://debezium.zulipchat.com/api/v1'

def executeShell(directory, script) {
    def evaluatedScript = ""
    dir(directory) {
        withSecrets(config: ONE_PASSWORD_CONFIG, secrets: SECRETS) {
            def engine = new groovy.text.SimpleTemplateEngine()
            def binding = [
                'GPG_PRIVATE_KEY': GPG_PRIVATE_KEY,
                'GPG_PASSPHRASE': GPG_PASSPHRASE,
                'GITHUB_USERNAME': GITHUB_USERNAME,
                'GITHUB_PASSWORD': GITHUB_PASSWORD,
                'QUAYIO_USERNAME': QUAYIO_USERNAME,
                'QUAYIO_PASSWORD': QUAYIO_PASSWORD,
                'ZULIPBOT_USERNAME': ZULIPBOT_USERNAME,
                'ZULIPBOT_TOKEN': ZULIPBOT_TOKEN
            ]
            evaluatedScript = engine.createTemplate(script).make(binding).toString()
        }
        sh(script: evaluatedScript, returnStdout: false)
    }
}

def sendZulipNotification(message) {
    if (!ZULIP_TO) {
        return
    }

    executeShell('.',
"""
    curl -sSf -u "\$ZULIPBOT_USERNAME:\$ZULIPBOT_TOKEN" \
      --data-urlencode type=private \
      --data-urlencode 'to=[$ZULIP_TO]' \
      --data-urlencode content="$message" \
      "$ZULIP_URL/messages"
"""
    )
}

node {
    catchError {
        stage('Validate parameters') {
            common.validateVersionFormat(RELEASE_VERSION)
        }

        stage('Initialize') {
            DRY_RUN = common.getDryRun()
            RELEASE_SEM_VERSION = common.convertToSemver(RELEASE_VERSION)

            dir('.') {
                deleteDir()
                sh "git config user.email || git config --global user.email \"debezium@gmail.com\" && git config --global user.name \"Debezium Builder\""
                sh "ssh-keyscan github.com >> $HOME_DIR/.ssh/known_hosts"

                sh "mkdir ${WORKSPACE}/${HELM_CHART_OUTPUT_DIR}"
                sh "mkdir ${WORKSPACE}/${HELM_CHART_OUTPUT_DIR}/debezium-operator"
                sh "mkdir ${WORKSPACE}/${HELM_CHART_OUTPUT_DIR}/debezium-platform"
            }

            echo "Configuring GPG in '${GPG_DIR}'"
            executeShell(GPG_DIR, '''gpg --import --batch --passphrase ${GPG_PASSPHRASE} --homedir . <<-EOF
${GPG_PRIVATE_KEY}
EOF''')

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

            // Determine chart structure based on version (3.6+ uses new structure)
            def versionParts = RELEASE_VERSION.tokenize('.')
            def majorVersion = versionParts[0].toInteger()
            def minorVersion = versionParts[1].tokenize(/[^0-9]/)[0].toInteger()
            def useNewStructure = (majorVersion > 3) || (majorVersion == 3 && minorVersion >= 6)

            dir(TMP_WORKDIR) {

                sh(
                        label: 'Download and verify helm chart',
                        script: """
                            echo "Input url: $INPUT_URL"
                            curl -fLjs -o "debezium-operator-${RELEASE_SEM_VERSION}.tar.gz" "$INPUT_URL"
                        """
                )

                sh(label: 'Unzip',
                        script: """
                            tar -xvzf debezium-operator-${RELEASE_SEM_VERSION}.tar.gz --one-top-level=debezium-operator-${RELEASE_SEM_VERSION} --strip-components=1
                            """
                )

                // Set chart path based on structure
                def chartPath = useNewStructure ?
                    "debezium-operator-${RELEASE_SEM_VERSION}/kubernetes/debezium-operator" :
                    "debezium-operator-${RELEASE_SEM_VERSION}"

                dir(chartPath) {
                    fileUtils.modifyFile("values.yaml", { content ->
                        // Old structure uses quoted values, new structure uses unquoted
                        if (useNewStructure) {
                            return content.replaceAll(
                                    /(image:\s*[^:]+:)[^\s]+/,
                                    "\$1${RELEASE_SEM_VERSION}"
                            )
                        } else {
                            return content.replaceAll(
                                    /(image:\s*"[^:]+:)[^"]+(")/,
                                    "\$1${RELEASE_SEM_VERSION}\$2"
                            )
                        }
                    })

                }

                sh(label: 'Repackage',
                        script: """
                            helm package --app-version=${RELEASE_SEM_VERSION} --version=${RELEASE_SEM_VERSION} ${chartPath}
                            cp debezium-operator-${RELEASE_SEM_VERSION}.tgz ${WORKSPACE}/${HELM_CHART_OUTPUT_DIR}/debezium-operator
                        """
                )
            }

            stage('Create a GH release') {
                dir(DEBEZIUM_OPERATOR_DIR) {
                    if (!DRY_RUN) {
                        executeShell('.', """
                            export GH_TOKEN=\${GITHUB_PASSWORD}
                            gh release create v${RELEASE_VERSION} --verify-tag -t 'Debezium Operator Chart v${RELEASE_VERSION}' --latest '$TMP_WORKDIR/debezium-operator-${RELEASE_SEM_VERSION}.tgz'
                        """)
                    }
                }
            }

            stage('Pushing chart to quay.io') {
                executeShell('.', '''
                    set +x
                    helm registry login -u ${QUAYIO_USERNAME} -p ${QUAYIO_PASSWORD} quay.io
                ''')
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
                                "${RELEASE_SEM_VERSION}"
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
                        executeShell('.', """
                            export GH_TOKEN=\${GITHUB_PASSWORD}
                            gh release create v${RELEASE_VERSION} --verify-tag -t 'Debezium Platform Chart v${RELEASE_VERSION}' --latest 'debezium-platform-${RELEASE_SEM_VERSION}.tgz'
                        """)
                    }
                }

                stage('Pushing chart to quay.io') {
                    executeShell('.', '''
                        set +x
                        helm registry login -u ${QUAYIO_USERNAME} -p ${QUAYIO_PASSWORD} quay.io
                    ''')
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
                    executeShell('.', """
                        git commit -a -m '[release] Stable $RELEASE_VERSION for Debezium Charts'
                        git push \"https://\${GITHUB_USERNAME}:\${GITHUB_PASSWORD}@${DEBEZIUM_CHART_REPOSITORY}\" HEAD:${DEBEZIUM_CHART_BRANCH}
                        git tag v$RELEASE_VERSION && git push \"https://\${GITHUB_USERNAME}:\${GITHUB_PASSWORD}@${DEBEZIUM_CHART_REPOSITORY}\" v$RELEASE_VERSION
                    """)
                }
            }
        }
    }

    sendZulipNotification("${JOB_NAME} run #${BUILD_NUMBER} finished with ${currentBuild.currentResult}. Run ${BUILD_URL} finished with result: ${currentBuild.currentResult}")
}
