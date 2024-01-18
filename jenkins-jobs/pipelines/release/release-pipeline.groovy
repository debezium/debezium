import groovy.json.*
import java.util.stream.*

import com.cloudbees.groovy.cps.NonCPS

if (
    !RELEASE_VERSION ||
    !DEVELOPMENT_VERSION ||
    !DEBEZIUM_REPOSITORY ||
    !DEBEZIUM_BRANCH ||
    !DEBEZIUM_ADDITIONAL_REPOSITORIES ||
    !IMAGES_REPOSITORY ||
    !IMAGES_BRANCH ||
    !UI_REPOSITORY ||
    !UI_BRANCH ||
    !POSTGRES_DECODER_REPOSITORY ||
    !POSTGRES_DECODER_BRANCH ||
    !MAVEN_CENTRAL_SYNC_TIMEOUT
) {
    error 'Input parameters not provided'
}

if (DRY_RUN == null) {
    DRY_RUN = false
}
else if (DRY_RUN instanceof String) {
    DRY_RUN = Boolean.valueOf(DRY_RUN)
}
echo "Dry run: ${DRY_RUN}"

if (IGNORE_SNAPSHOTS == null) {
    IGNORE_SNAPSHOTS = false
}
else if (IGNORE_SNAPSHOTS instanceof String) {
    IGNORE_SNAPSHOTS = Boolean.valueOf(IGNORE_SNAPSHOTS)
}
echo "Ignore snapshots: ${IGNORE_SNAPSHOTS}"

if (CHECK_BACKPORTS == null) {
    CHECK_BACKPORTS = false
}
else if (CHECK_BACKPORTS instanceof String) {
    CHECK_BACKPORTS = Boolean.valueOf(CHECK_BACKPORTS)
}

GIT_CREDENTIALS_ID = 'debezium-github'
JIRA_CREDENTIALS_ID = 'debezium-jira-pat'
HOME_DIR = '/home/centos'
GPG_DIR = 'gpg'

DEBEZIUM_DIR = 'debezium'
IMAGES_DIR = 'images'
UI_DIR = 'ui'
POSTGRES_DECODER_DIR = 'postgres-decoder'

INSTALL_ARTIFACTS_SCRIPT = 'install-artifacts.sh'

VERSION_TAG = "v$RELEASE_VERSION"
VERSION_PARTS = RELEASE_VERSION.split('\\.')
VERSION_MAJOR_MINOR = "${VERSION_PARTS[0]}.${VERSION_PARTS[1]}"
IMAGE_TAG = VERSION_MAJOR_MINOR
CANDIDATE_BRANCH = "candidate-$RELEASE_VERSION"

POSTGRES_TAGS = ['9.6', '9.6-alpine', '10', '10-alpine', '11', '11-alpine', '12', '12-alpine', '13', '13-alpine', '14', '14-alpine', '15', '15-alpine', '16', '16-alpine']
CONNECTORS_PER_VERSION = [
    '0.8' : ['mongodb', 'mysql', 'postgres', 'oracle'],
    '0.9' : ['mongodb', 'mysql', 'postgres', 'sqlserver', 'oracle'],
    '0.10': ['mongodb', 'mysql', 'postgres', 'sqlserver', 'oracle'],
    '1.0' : ['mongodb', 'mysql', 'postgres', 'sqlserver', 'oracle', 'cassandra'],
    '1.1' : ['mongodb', 'mysql', 'postgres', 'sqlserver', 'oracle', 'cassandra', 'db2'],
    '1.2' : ['mongodb', 'mysql', 'postgres', 'sqlserver', 'oracle', 'cassandra', 'db2'],
    '1.3' : ['mongodb', 'mysql', 'postgres', 'sqlserver', 'oracle', 'cassandra', 'db2'],
    '1.4' : ['mongodb', 'mysql', 'postgres', 'sqlserver', 'oracle', 'cassandra', 'db2', 'vitess'],
    '1.5' : ['mongodb', 'mysql', 'postgres', 'sqlserver', 'oracle', 'cassandra', 'db2', 'vitess'],
    '1.6' : ['mongodb', 'mysql', 'postgres', 'sqlserver', 'oracle', 'cassandra', 'db2', 'vitess'],
    '1.7' : ['mongodb', 'mysql', 'postgres', 'sqlserver', 'oracle', 'cassandra', 'db2', 'vitess'],
    '1.8' : ['mongodb', 'mysql', 'postgres', 'sqlserver', 'oracle', 'cassandra', 'db2', 'vitess'],
    '1.9' : ['mongodb', 'mysql', 'postgres', 'sqlserver', 'oracle', 'cassandra-3', 'cassandra-4', 'db2', 'vitess'],
    '2.0' : ['mongodb', 'mysql', 'postgres', 'sqlserver', 'oracle', 'cassandra-3', 'cassandra-4', 'db2', 'vitess'],
    '2.1' : ['mongodb', 'mysql', 'postgres', 'sqlserver', 'oracle', 'cassandra-3', 'cassandra-4', 'db2', 'vitess', 'spanner'],
    '2.2' : ['mongodb', 'mysql', 'postgres', 'sqlserver', 'oracle', 'cassandra-3', 'cassandra-4', 'db2', 'vitess', 'spanner', 'jdbc'],
    '2.3' : ['mongodb', 'mysql', 'postgres', 'sqlserver', 'oracle', 'cassandra-3', 'cassandra-4', 'db2', 'vitess', 'spanner', 'jdbc'],
    '2.4' : ['mongodb', 'mysql', 'postgres', 'sqlserver', 'oracle', 'cassandra-3', 'cassandra-4', 'db2', 'vitess', 'spanner', 'jdbc'],
    '2.5' : ['mongodb', 'mysql', 'postgres', 'sqlserver', 'oracle', 'cassandra-3', 'cassandra-4', 'db2', 'vitess', 'spanner', 'jdbc', 'informix'],
    '2.6' : ['mongodb', 'mysql', 'postgres', 'sqlserver', 'oracle', 'cassandra-3', 'cassandra-4', 'db2', 'vitess', 'spanner', 'jdbc', 'informix']
]

CONNECTORS = CONNECTORS_PER_VERSION[VERSION_MAJOR_MINOR]
if (CONNECTORS == null) {
    error "List of connectors not available"
}
echo "Connectors to be released: $CONNECTORS"

ADDITIONAL_REPOSITORIES = [:]
DEBEZIUM_ADDITIONAL_REPOSITORIES.split().each {
    def (id, repository, branch) = it.split('#')
    ADDITIONAL_REPOSITORIES[id] = ['git': repository, 'branch': branch]
    echo "Additional repository $repository will be used"
}

IMAGES = ['connect', 'connect-base', 'examples/mysql', 'examples/mysql-gtids', 'examples/mysql-replication/master', 'examples/mysql-replication/replica', 'examples/postgres', 'examples/mongodb', 'kafka', 'server', 'zookeeper', 'operator', 'ui']
MAVEN_CENTRAL = 'https://repo1.maven.org/maven2'
STAGING_REPO = 'https://s01.oss.sonatype.org/content/repositories'
STAGING_REPO_ID = null
ADDITIONAL_STAGING_REPO_ID = [:]
LOCAL_MAVEN_REPO = "$HOME_DIR/.m2/repository"

withCredentials([string(credentialsId: JIRA_CREDENTIALS_ID, variable: 'PAT')]) {
    JIRA_PAT = PAT
    JIRA_BASE_URL = "https://issues.redhat.com/rest/api/2"
}

JIRA_PROJECT = 'DBZ'
JIRA_VERSION = RELEASE_VERSION

JIRA_CLOSE_ISSUE = """
    {
        "update": {
            "comment": [
                {
                    "add": {
                        "body": "Released"
                    }
                }
            ]
        },
        "transition": {
            "id": "701"
        }
    }
"""
JIRA_CLOSE_RELEASE = """
    {
        "released": true,
        "releaseDate": "${new Date().format('yyyy-MM-dd')}"
    }
"""


def modifyFile(filename, modClosure) {
    echo "========================================================================"
    echo "Modifying file $filename"
    echo "========================================================================"
    def originalFile = readFile(filename)
    echo "Content to be modified:\n$originalFile"
    echo "========================================================================"
    def updatedFile = modClosure.call(originalFile)
    echo "Content after modification:\n$updatedFile"
    echo "========================================================================"
    writeFile(
        file: filename,
        text: updatedFile
    )
}

@NonCPS
def jiraURL(path, params = [:]) {
    def url = "$JIRA_BASE_URL/$path"
    if (params) {
        url <<= '?' << params.collect {k, v -> "$k=${URLEncoder.encode(v, 'US-ASCII')}"}.join('&')
    }
    return url.toString().toURL()
}

@NonCPS
def jiraGET(path, params = [:]) {
    jiraURL(path, params).openConnection().with {
        doOutput = true
        requestMethod = 'GET'
        setRequestProperty('Content-Type', 'application/json')
        setRequestProperty('Authorization', "Bearer $JIRA_PAT")
        new JsonSlurper().parse(new StringReader(content.text))
    }
}

@NonCPS
def jiraUpdate(path, payload, method = 'POST') {
    path.toURL().openConnection().with {
        doOutput = true
        requestMethod = method
        setRequestProperty('Content-Type', 'application/json')
        setRequestProperty('Authorization', "Bearer $JIRA_PAT")
        outputStream.withWriter { writer ->
            writer << payload
        }
        println content.text
    }
}

@NonCPS
def unresolvedIssuesFromJira() {
    jiraGET('search', [
        'jql': "project=$JIRA_PROJECT AND fixVersion=$JIRA_VERSION AND status NOT IN ('Resolved', 'Closed')",
        'fields': 'key'
    ]).issues.collect { it.key }
}

@NonCPS
def issuesWithoutComponentsFromJira() {
    jiraGET('search', [
        'jql': "project=$JIRA_PROJECT AND fixVersion=$JIRA_VERSION AND component IS EMPTY",
        'fields': 'key'
    ]).issues.collect { it.key }
}

@NonCPS
def resolvedIssuesFromJira() {
    jiraGET('search', [
        'jql': "project=$JIRA_PROJECT AND fixVersion=$JIRA_VERSION AND status='Resolved'",
        'fields': 'key'
    ]).issues.collect { it.self }
}

@NonCPS
def closeJiraIssues() {
    resolvedIssuesFromJira().each { issue -> jiraUpdate("${issue}/transitions", JIRA_CLOSE_ISSUE) }
}

@NonCPS
def findVersion(jiraVersion) {
    jiraGET('project/DBZ/versions').find { it.name == jiraVersion }
}

@NonCPS
def closeJiraRelease() {
    jiraUpdate(findVersion(JIRA_VERSION).self, JIRA_CLOSE_RELEASE, 'PUT')
}

def mvnRelease(repoDir, repoName, branchName, buildArgs = '') {
    def repoId = null
    dir(repoDir) {
        // Debezium Server must always ignore snapshots as it depends on Debezium Server BOM
        // and it is not possible to override it with a stable version
        def ignoreSnaphots = (repoDir == 'server') ? true : IGNORE_SNAPSHOTS
        sh "mvn release:clean release:prepare -DreleaseVersion=$RELEASE_VERSION -Dtag=$VERSION_TAG -DdevelopmentVersion=$DEVELOPMENT_VERSION -DpushChanges=${!DRY_RUN} -DignoreSnapshots=$ignoreSnaphots -Darguments=\"-DskipTests -DskipITs -Passembly $buildArgs\" $buildArgs"
        if (!DRY_RUN) {
            withCredentials([usernamePassword(credentialsId: GIT_CREDENTIALS_ID, passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME')]) {
                sh "git push \"https://\${GIT_USERNAME}:\${GIT_PASSWORD}@${repoName}\" HEAD:$branchName --follow-tags"
            }
        }
        withCredentials([
            string(credentialsId: 'debezium-ci-gpg-passphrase', variable: 'GPG_PASSPHRASE'),
            usernamePassword(credentialsId: GIT_CREDENTIALS_ID, passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME')]) {
            def mvnlog = sh(script: "mvn release:perform -DstagingProgressTimeoutMinutes=30 -DlocalCheckout=$DRY_RUN -DconnectionUrl=scm:git:https://\${GIT_USERNAME}:\${GIT_PASSWORD}@${repoName} -Darguments=\"-s $HOME/.m2/settings-snapshots.xml -DstagingProgressTimeoutMinutes=30 -Dgpg.homedir=\$WORKSPACE/$GPG_DIR -Dgpg.passphrase=$GPG_PASSPHRASE -DskipTests -DskipITs $buildArgs\" $buildArgs", returnStdout: true).trim()
            echo mvnlog
            def match = mvnlog =~ /Created staging repository with ID \"(iodebezium-.+)\"/
            if (!match[0]) {
                error 'Could not find staging repository ID'
            }
            repoId = match[0][1]
            echo "Using staging repository $repoId"
        }
        sh "mvn clean install -DskipTests -DskipITs -Passembly $buildArgs"
    }
    return repoId
}

node('release-node') {
    catchError {
        stage('Validate parameters') {
            if (!(RELEASE_VERSION ==~ /\d+\.\d+.\d+\.(Final|(Alpha|Beta|CR)\d+)/)) {
                error "Release version '$RELEASE_VERSION' is not of the required format x.y.z.suffix"
            }
            if (!(DEVELOPMENT_VERSION ==~ /\d+\.\d+.\d+\-SNAPSHOT/)) {
                error "Development version '$DEVELOPMENT_VERSION' is not of the required format x.y.z-SNAPSHOT"
            }
        }

        stage('Initialize') {
            dir('.') {
                deleteDir()
                sh "git config user.email || git config --global user.email \"debezium@gmail.com\" && git config --global user.name \"Debezium Builder\""
                sh "ssh-keyscan github.com >> $HOME_DIR/.ssh/known_hosts"
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
                      branches                         : [[name: "*/$DEBEZIUM_BRANCH"]],
                      doGenerateSubmoduleConfigurations: false,
                      extensions                       : [[$class: 'RelativeTargetDirectory', relativeTargetDir: DEBEZIUM_DIR]],
                      submoduleCfg                     : [],
                      userRemoteConfigs                : [[url: "https://$DEBEZIUM_REPOSITORY", credentialsId: GIT_CREDENTIALS_ID]]
            ]
            )
            ADDITIONAL_REPOSITORIES.each { id, repo ->
                checkout([$class                           : 'GitSCM',
                          branches                         : [[name: "*/${repo.branch}"]],
                          doGenerateSubmoduleConfigurations: false,
                          extensions                       : [[$class: 'RelativeTargetDirectory', relativeTargetDir: id]],
                          submoduleCfg                     : [],
                          userRemoteConfigs                : [[url: "https://${repo.git}", credentialsId: GIT_CREDENTIALS_ID]]
                ]
                )
            }
            checkout([$class                           : 'GitSCM',
                      branches                         : [[name: "*/$IMAGES_BRANCH"]],
                      doGenerateSubmoduleConfigurations: false,
                      extensions                       : [[$class: 'RelativeTargetDirectory', relativeTargetDir: IMAGES_DIR]],
                      submoduleCfg                     : [],
                      userRemoteConfigs                : [[url: "https://$IMAGES_REPOSITORY", credentialsId: GIT_CREDENTIALS_ID]]
            ]
            )
            checkout([$class                           : 'GitSCM',
                      branches                         : [[name: "*/$UI_BRANCH"]],
                      doGenerateSubmoduleConfigurations: false,
                      extensions                       : [[$class: 'RelativeTargetDirectory', relativeTargetDir: UI_DIR]],
                      submoduleCfg                     : [],
                      userRemoteConfigs                : [[url: "https://$UI_REPOSITORY", credentialsId: GIT_CREDENTIALS_ID]]
            ]
            )
            checkout([$class                           : 'GitSCM',
                      branches                         : [[name: "*/$POSTGRES_DECODER_BRANCH"]],
                      doGenerateSubmoduleConfigurations: false,
                      extensions                       : [[$class: 'RelativeTargetDirectory', relativeTargetDir: POSTGRES_DECODER_DIR]],
                      submoduleCfg                     : [],
                      userRemoteConfigs                : [[url: "https://$POSTGRES_DECODER_REPOSITORY", credentialsId: GIT_CREDENTIALS_ID]]
            ]
            )
            echo "Images tagged with $IMAGE_TAG will be used"

            dir(DEBEZIUM_DIR) {
                ORACLE_ARTIFACT_VERSION = (readFile('pom.xml') =~ /(?ms)<version.oracle.driver>(.+)<\/version.oracle.driver>/)[0][1]
                ORACLE_ARTIFACT_DIR = "$HOME_DIR/oracle-libs/${ORACLE_ARTIFACT_VERSION}.0"
            }
            dir(ORACLE_ARTIFACT_DIR) {
                sh "mvn install:install-file -DgroupId=com.oracle.instantclient -DartifactId=ojdbc8 -Dversion=$ORACLE_ARTIFACT_VERSION -Dpackaging=jar -Dfile=ojdbc8.jar"
                sh "mvn install:install-file -DgroupId=com.oracle.instantclient -DartifactId=xstreams -Dversion=$ORACLE_ARTIFACT_VERSION -Dpackaging=jar -Dfile=xstreams.jar"
            }
        }

        stage('Check Contributors') {
            if (!DRY_RUN) {
                dir(DEBEZIUM_DIR) {
                    def rc = sh(script: "jenkins-jobs/scripts/check-contributors.sh", returnStatus: true)
                    if (rc != 0) {
                        error "Error, not all contributors have been added to COPYRIGHT.txt.  See log for details."
                    }
                }
            }
        }

        stage('Check missing backports') {
            if (!DRY_RUN && CHECK_BACKPORTS) {
                if (!BACKPORT_FROM_TAG || !BACKPORT_TO_TAG) {
                    error "Backport from/to tags must be provided to perform backport checks"
                }
                dir(DEBEZIUM_DIR) {
                    def rc = sh(script: "github-support/list-missing-commits-by-issue-key.sh $JIRA_VERSION $BACKPORT_FROM_TAG $BACKPORT_TO_TAG $JIRA_PAT", returnStatus: true)
                    if (rc != 0) {
                        error "Error, there are some missing backport commits."
                    }
                }
            }
        }

        stage('Check Jira') {
            if (!DRY_RUN) {
                if (findVersion(JIRA_VERSION) == null) {
                    error "Requested release does not exist"
                }

                unresolvedIssues = unresolvedIssuesFromJira()
                issuesWithoutComponents = issuesWithoutComponentsFromJira()
                if (!resolvedIssuesFromJira()) {
                    error "Error, there are no resolved issues for the release"
                }
                if (unresolvedIssues) {
                    error "Error, issues ${unresolvedIssues.toString()} must be resolved"
                }
                if (issuesWithoutComponents) {
                    error "Error, issues ${issuesWithoutComponents.toString()} must have component set"
                }
            }
        }

        stage('Check changelog') {
            if (!DRY_RUN) {
                if (!new URL("https://raw.githubusercontent.com/debezium/debezium/$DEBEZIUM_BRANCH/CHANGELOG.md").text.contains(RELEASE_VERSION) ||
                        !new URL("https://raw.githubusercontent.com/debezium/debezium.github.io/develop/_data/releases/$VERSION_MAJOR_MINOR/${RELEASE_VERSION}.yml").text.contains('summary:') ||
                        !new URL("https://raw.githubusercontent.com/debezium/debezium.github.io/develop/releases/$VERSION_MAJOR_MINOR/release-notes.asciidoc").text.contains(RELEASE_VERSION)
                ) {
                    error 'Changelog was not modified to include release information'
                }
            }
        }

        stage('Dockerfiles present') {
            def missingImages = []
            for (i = 0; i < IMAGES.size(); i++) {
                def image = IMAGES[i]
                if (!fileExists("$IMAGES_DIR/$image/$IMAGE_TAG/Dockerfile")) {
                    missingImages << image
                }
            }
            if (missingImages) {
                error "Dockerfile(s) not present for $missingImages tag $IMAGE_TAG"
            }
        }

        stage('Prepare release') {
            dir(DEBEZIUM_DIR) {
                sh "git checkout -b $CANDIDATE_BRANCH"
                sh "mvn clean install -DskipTests -DskipITs -Poracle-all"
                modifyFile('debezium-testing/debezium-testing-system/pom.xml') {
                    it.replaceFirst('<version.debezium.connector>.+</version.debezium.connector>', "<version.debezium.connector>$RELEASE_VERSION</version.debezium.connector>")
                }
                sh "git commit -a -m '[release] Stable $RELEASE_VERSION for testing module deps'"
            }
            STAGING_REPO_ID = mvnRelease(DEBEZIUM_DIR, DEBEZIUM_REPOSITORY, CANDIDATE_BRANCH, '-Poracle-all')
            dir(DEBEZIUM_DIR) {
                modifyFile('debezium-testing/debezium-testing-system/pom.xml') {
                    it.replaceFirst('<version.debezium.connector>.+</version.debezium.connector>', '<version.debezium.connector>\\${project.version}</version.debezium.connector>')
                }
                sh "git commit -a -m '[release] Development version for testing module deps'"
                if (!DRY_RUN) {
                    withCredentials([usernamePassword(credentialsId: GIT_CREDENTIALS_ID, passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME')]) {
                        sh """
                           git push https://\${GIT_USERNAME}:\${GIT_PASSWORD}@${DEBEZIUM_REPOSITORY} HEAD:${CANDIDATE_BRANCH}
                        """
                    }
                }
            }
            ADDITIONAL_REPOSITORIES.each { id, repo ->
                // Additional repositories (like Debezium Server) can have their own BOM
                def repoBom = "debezium-${id}-bom/pom.xml"
                def buildArgs = "-Dversion.debezium=$RELEASE_VERSION"
                dir(id) {
                    sh "git checkout -b $CANDIDATE_BRANCH"
                    // Obtain dependecies not available in Maven Central (introduced for Cassandra Enerprise)
                    if (fileExists(INSTALL_ARTIFACTS_SCRIPT)) {
                        sh "./$INSTALL_ARTIFACTS_SCRIPT"
                    }
                    sh "mvn clean install -DskipTests -DskipITs -Passembly"
                    modifyFile("pom.xml") {
                        it.replaceFirst('<version>.+</version>\n    </parent>', "<version>$RELEASE_VERSION</version>\n    </parent>")
                    }
                    if (fileExists(repoBom)) {
                        modifyFile(repoBom) {
                            it.replaceFirst('<version>.+</version>\n    </parent>', "<version>$RELEASE_VERSION</version>\n    </parent>")
                        }
                    }
                    if(id == "operator") {
                        // Update k8 resources and  generate manifests for operator
                        def profiles = "stable,k8update"
                        def releaseProfiles = "stable"
                        if (LATEST_SERIES) {
                            profiles += ",olmLatest"
                            releaseProfiles += ",olmLatest"
                        }
                        buildArgs +=" -P$releaseProfiles"
                        sh "mvn clean install -P$profiles -DskipTests -DskipITs"
                    }
                    sh "git commit -a -m '[release] Stable parent $RELEASE_VERSION for release'"
                    if(id != "operator") {
                        // Don't repeat build for operator
                        sh "mvn clean install -DskipTests -DskipITs"
                    }
                }
                ADDITIONAL_REPOSITORIES[id].mavenRepoId = mvnRelease(id, repo.git, CANDIDATE_BRANCH, buildArgs)
                dir(id) {
                    modifyFile("pom.xml") {
                        it.replaceFirst('<version>.+</version>\n    </parent>', "<version>$DEVELOPMENT_VERSION</version>\n    </parent>")
                    }
                    if (fileExists(repoBom)) {
                        modifyFile(repoBom) {
                            it.replaceFirst('<version>.+</version>\n    </parent>', "<version>$DEVELOPMENT_VERSION</version>\n    </parent>")
                        }
                    }
                    sh "git commit -a -m '[release] New parent $DEVELOPMENT_VERSION for development'"
                    if (!DRY_RUN) {
                        withCredentials([usernamePassword(credentialsId: GIT_CREDENTIALS_ID, passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME')]) {
                            sh """
                               git push https://\${GIT_USERNAME}:\${GIT_PASSWORD}@${repo.git} HEAD:${CANDIDATE_BRANCH}
                            """
                        }
                    }
                }
            }
        }

        stage('Verify images') {
            def sums = [:]
            for (i = 0; i < CONNECTORS.size(); i++) {
                def connector = CONNECTORS[i]
                dir("$LOCAL_MAVEN_REPO/io/debezium/debezium-connector-$connector/$RELEASE_VERSION") {
                    def md5sum = sh(script: "md5sum -b debezium-connector-${connector}-${RELEASE_VERSION}-plugin.tar.gz | awk '{print \$1}'", returnStdout: true).trim()
                    sums["${connector.toUpperCase()}"] = md5sum
                }
            }
            echo "MD5 sums calculated: ${sums}"
            def serverSum = sh(script: "md5sum -b $LOCAL_MAVEN_REPO/io/debezium/debezium-server-dist/$RELEASE_VERSION/debezium-server-dist-${RELEASE_VERSION}.tar.gz | awk '{print \$1}'", returnStdout: true).trim()
            def operatorSum = sh(script: "md5sum -b $LOCAL_MAVEN_REPO/io/debezium/debezium-operator-dist/$RELEASE_VERSION/debezium-operator-dist-${RELEASE_VERSION}.tar.gz | awk '{print \$1}'", returnStdout: true).trim()
            sums['SCRIPTING'] = sh(script: "md5sum -b $LOCAL_MAVEN_REPO/io/debezium/debezium-scripting/$RELEASE_VERSION/debezium-scripting-${RELEASE_VERSION}.tar.gz | awk '{print \$1}'", returnStdout: true).trim()
            sums['KCRESTEXT'] = sh(script: "md5sum -b $LOCAL_MAVEN_REPO/io/debezium/debezium-connect-rest-extension/$RELEASE_VERSION/debezium-connect-rest-extension-${RELEASE_VERSION}.tar.gz | awk '{print \$1}'", returnStdout: true).trim()
            dir("$IMAGES_DIR/connect/$IMAGE_TAG") {
                echo "Modifying main Dockerfile"
                def additionalRepoList = ADDITIONAL_REPOSITORIES.collect({ id, repo -> "${id.toUpperCase()}=$STAGING_REPO/${repo.mavenRepoId}" }).join(' ')
                modifyFile('Dockerfile') {
                    def ret = it
                            .replaceFirst('DEBEZIUM_VERSION="\\S+"', "DEBEZIUM_VERSION=\"$RELEASE_VERSION\"")
                            .replaceFirst('MAVEN_REPO_CENTRAL="[^"]*"', "MAVEN_REPO_CENTRAL=\"$STAGING_REPO/$STAGING_REPO_ID/\"")
                            .replaceFirst('MAVEN_REPOS_ADDITIONAL="[^"]*"', "MAVEN_REPOS_ADDITIONAL=\"$additionalRepoList\"")
                    for (entry in sums) {
                        ret = ret.replaceFirst("${entry.key}_MD5=\\S+", "${entry.key}_MD5=${entry.value}")
                    }
                    return ret
                }
                modifyFile('Dockerfile.local') {
                    it
                            .replaceFirst('DEBEZIUM_VERSION=\\S+', "DEBEZIUM_VERSION=$RELEASE_VERSION")
                }
            }
            echo "Modifying snapshot Dockerfile"
            dir("$IMAGES_DIR/connect/snapshot") {
                modifyFile('Dockerfile') {
                    it.replaceFirst('DEBEZIUM_VERSION=\\S+', "DEBEZIUM_VERSION=$DEVELOPMENT_VERSION")
                }
            }
            echo "Modifying Server Dockerfile"
            dir("$IMAGES_DIR/server/$IMAGE_TAG") {
                def serverStagingRepoId = ADDITIONAL_REPOSITORIES['server']?.mavenRepoId
                if (serverStagingRepoId == null) {
                    serverStagingRepoId = STAGING_REPO_ID
                }
                modifyFile('Dockerfile') {
                    it
                            .replaceFirst('MAVEN_REPO_CENTRAL="[^"]*"', "MAVEN_REPO_CENTRAL=\"$STAGING_REPO/$serverStagingRepoId/\"")
                            .replaceFirst('DEBEZIUM_VERSION=\\S+', "DEBEZIUM_VERSION=$RELEASE_VERSION")
                            .replaceFirst('SERVER_MD5=\\S+', "SERVER_MD5=$serverSum")
                }
            }
            echo "Modifying Server snapshot Dockerfile"
            dir("$IMAGES_DIR/server/snapshot") {
                modifyFile('Dockerfile') {
                    it.replaceFirst('DEBEZIUM_VERSION=\\S+', "DEBEZIUM_VERSION=$DEVELOPMENT_VERSION")
                }
            }
            echo "Modifying Operator Dockerfile"
            dir("$IMAGES_DIR/operator/$IMAGE_TAG") {
                def operatorStagingRepoId = ADDITIONAL_REPOSITORIES['operator']?.mavenRepoId
                if (operatorStagingRepoId == null) {
                    operatorStagingRepoId = STAGING_REPO_ID
                }
                modifyFile('Dockerfile') {
                    it
                            .replaceFirst('MAVEN_REPO_CENTRAL="[^"]*"', "MAVEN_REPO_CENTRAL=\"$STAGING_REPO/$operatorStagingRepoId/\"")
                            .replaceFirst('DEBEZIUM_VERSION=\\S+', "DEBEZIUM_VERSION=$RELEASE_VERSION")
                            .replaceFirst('OPERATOR_MD5=\\S+', "OPERATOR_MD5=$operatorSum")
                }
            }
            echo "Modifying Operator snapshot Dockerfile"
            dir("$IMAGES_DIR/operator/snapshot") {
                modifyFile('Dockerfile') {
                    it.replaceFirst('DEBEZIUM_VERSION=\\S+', "DEBEZIUM_VERSION=$DEVELOPMENT_VERSION")
                }
            }
            echo "Modifying UI Dockerfile"
            dir("$IMAGES_DIR") {
                modifyFile("ui/$IMAGE_TAG/Dockerfile") {
                    it.replaceFirst('BRANCH=\\S+', "BRANCH=$VERSION_TAG")
                }
            }
            echo "Modifying container images build scripts"
            dir("$IMAGES_DIR") {
                modifyFile('build-all-multiplatform.sh') {
                    it.replaceFirst('DEBEZIUM_VERSION=\"\\S+\"', "DEBEZIUM_VERSION=\"$IMAGE_TAG\"")
                }
                modifyFile('build-all.sh') {
                    it.replaceFirst('DEBEZIUM_VERSION=\"\\S+\"', "DEBEZIUM_VERSION=\"$IMAGE_TAG\"")
                }
            }
            dir(IMAGES_DIR) {
                script {
                    env.DEBEZIUM_DOCKER_REGISTRY_PRIMARY_NAME='localhost:5500/debeziumquay'
                    env.DEBEZIUM_DOCKER_REGISTRY_SECONDARY_NAME='localhost:5500/debezium'
                }
                sh """
                    docker run --privileged --rm tonistiigi/binfmt --install all
                    ./setup-local-builder.sh
                    docker compose -f local-registry/docker-compose.yml up -d
                    env SKIP_UI=true ./build-all-multiplatform.sh
                """
            }
            sh """
                docker rm -f connect zookeeper kafka mysql || true
                docker run -it -d --name mysql -p 53306:3306 -e MYSQL_ROOT_PASSWORD=debezium -e MYSQL_USER=mysqluser -e MYSQL_PASSWORD=mysqlpw $DEBEZIUM_DOCKER_REGISTRY_PRIMARY_NAME/example-mysql:$IMAGE_TAG
                docker run -it -d --name zookeeper -p 2181:2181 -p 2888:2888 -p 3888:3888 $DEBEZIUM_DOCKER_REGISTRY_PRIMARY_NAME/zookeeper:$IMAGE_TAG
                sleep 10
                docker run -it -d --name kafka -p 9092:9092 --link zookeeper:zookeeper $DEBEZIUM_DOCKER_REGISTRY_PRIMARY_NAME/kafka:$IMAGE_TAG
                sleep 10
                docker run -it -d --name connect -p 8083:8083 -e GROUP_ID=1 -e CONFIG_STORAGE_TOPIC=my_connect_configs -e OFFSET_STORAGE_TOPIC=my_connect_offsets --link zookeeper:zookeeper --link kafka:kafka --link mysql:mysql $DEBEZIUM_DOCKER_REGISTRY_PRIMARY_NAME/connect:$IMAGE_TAG
                sleep 30
    
                curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '
                {
                    "name": "inventory-connector",
                    "config": {
                        "name": "inventory-connector",
                        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
                        "tasks.max": "1",
                        "database.hostname": "mysql",
                        "database.port": "3306",
                        "database.user": "debezium",
                        "database.password": "dbz",
                        "database.server.id": "184054",
                        "topic.prefix": "dbserver1",
                        "database.include.list": "inventory",
                        "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
                        "schema.history.internal.kafka.topic": "schema-changes.inventory"
                    }
                }
                '
                sleep 10
            """
            timeout(time: 2, unit: java.util.concurrent.TimeUnit.MINUTES) {
                def watcherlog = sh(script: "docker run --name watcher --rm --link zookeeper:zookeeper --link kafka:kafka $DEBEZIUM_DOCKER_REGISTRY_PRIMARY_NAME/kafka:$IMAGE_TAG watch-topic -a -k dbserver1.inventory.customers --max-messages 2 2>&1", returnStdout: true).trim()
                echo watcherlog
                sh 'docker rm -f connect zookeeper kafka mysql'
                if (!watcherlog.contains('Processed a total of 2 messages')) {
                    error 'Tutorial watcher did not reported messages'
                }
            }
            dir("$IMAGES_DIR/connect/$IMAGE_TAG") {
                modifyFile('Dockerfile') {
                    it
                            .replaceFirst('MAVEN_REPO_CENTRAL="[^"]+"', "MAVEN_REPO_CENTRAL=\"\"")
                            .replaceFirst('MAVEN_REPOS_ADDITIONAL="[^"]+"', "MAVEN_REPOS_ADDITIONAL=\"\"")
                }
                modifyFile('Dockerfile.local') {
                    it
                            .replaceFirst('DEBEZIUM_VERSION=\"\\S+\"', "DEBEZIUM_VERSION=\"$RELEASE_VERSION\"")
                }
            }
            dir("$IMAGES_DIR/server/$IMAGE_TAG") {
                modifyFile('Dockerfile') {
                    it
                            .replaceFirst('MAVEN_REPO_CENTRAL="[^"]*"', "MAVEN_REPO_CENTRAL=\"$MAVEN_CENTRAL\"")
                }
            }
            dir("$IMAGES_DIR/operator/$IMAGE_TAG") {
                modifyFile('Dockerfile') {
                    it
                            .replaceFirst('MAVEN_REPO_CENTRAL="[^"]*"', "MAVEN_REPO_CENTRAL=\"$MAVEN_CENTRAL\"")
                }
            }
        }

        stage('Push to Central') {
            echo '================================================================================='
            echo '|                                                                               |'
            echo '|                                                                               |'
            echo '|          Log in into the OSS Central and release the staging repo"            |'
            echo '|                                                                               |'
            echo '|                                                                               |'
            echo '================================================================================='
        }

        stage('Wait for Central sync') {
            if (!DRY_RUN) {
                timeout(time: MAVEN_CENTRAL_SYNC_TIMEOUT as int, unit: java.util.concurrent.TimeUnit.HOURS) {
                    while (true) {
                        failed = false
                        for (i = 0; i < CONNECTORS.size(); i++) {
                            def connector = CONNECTORS[i]
                            def curl = sh(returnStatus: true, script: "curl -IfskL -o /dev/null https://repo1.maven.org/maven2/io/debezium/debezium-connector-$connector/${RELEASE_VERSION}/debezium-connector-$connector-${RELEASE_VERSION}-plugin.tar.gz")
                            if (curl) {
                                echo "Connector $connector not yet in Maven Central"
                                failed = true
                            }
                        }
                        if (!failed) {
                            break
                        }
                        sleep 30
                    }
                }
            }
        }

        stage('Cleanup Jira') {
            if (!DRY_RUN) {
                closeJiraIssues()
                closeJiraRelease()
            }
        }

        stage('PostgreSQL Decoder') {
            if (!DRY_RUN) {
                dir(POSTGRES_DECODER_DIR) {
                    withCredentials([usernamePassword(credentialsId: GIT_CREDENTIALS_ID, passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME')]) {
                        sh "git tag $VERSION_TAG && git push \"https://\${GIT_USERNAME}:\${GIT_PASSWORD}@${POSTGRES_DECODER_REPOSITORY}\" $VERSION_TAG"
                    }
                }
            }
            dir("$IMAGES_DIR") {
                for (tag in POSTGRES_TAGS) {
                    modifyFile("postgres/$tag/Dockerfile") {
                        it.replaceFirst('PLUGIN_VERSION=\\S+', "PLUGIN_VERSION=$VERSION_TAG")
                    }
                }
            }
        }

        stage('Update images') {
            dir("$IMAGES_DIR") {
                // Change of major/minor version - need to provide a new image tag for next releases
                if (!DEVELOPMENT_VERSION.startsWith(IMAGE_TAG)) {
                    def version = DEVELOPMENT_VERSION.split('\\.')
                    def nextTag = "${version[0]}.${version[1]}"
                    for (i = 0; i < IMAGES.size(); i++) {
                        def image = IMAGES[i]
                        if ((new File("$image/$nextTag")).exists()) {
                            continue
                        }
                        sh "cp -r $image/$IMAGE_TAG $image/$nextTag && git add $image/$nextTag"
                    }
                    modifyFile('connect/snapshot/Dockerfile') {
                        it.replaceFirst('FROM \\S+', "FROM quay.io/debezium/connect-base:$nextTag")
                    }
                    modifyFile("connect/$nextTag/Dockerfile") {
                        it.replaceFirst('FROM \\S+', "FROM \\$DEBEZIUM_DOCKER_REGISTRY_PRIMARY_NAME/connect-base:$nextTag")
                    }
                    modifyFile("connect/$nextTag/Dockerfile.local") {
                        it
                                .replaceFirst('FROM \\S+', "FROM quay.io/debezium/connect-base:$nextTag")
                                .replaceFirst('DEBEZIUM_VERSION=\\S+', "DEBEZIUM_VERSION=${DEVELOPMENT_VERSION - '-SNAPSHOT'}")
                    }
                    modifyFile("connect-base/$nextTag/Dockerfile") {
                        it.replaceFirst('FROM \\S+', "FROM \\$DEBEZIUM_DOCKER_REGISTRY_PRIMARY_NAME/kafka:$nextTag")
                    }
                }
                if (!DRY_RUN) {
                    withCredentials([usernamePassword(credentialsId: GIT_CREDENTIALS_ID, passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME')]) {
                        sh """
                            git commit -a -m "Updated container images for release $RELEASE_VERSION" && git push https://\${GIT_USERNAME}:\${GIT_PASSWORD}@${
                            IMAGES_REPOSITORY
                        } HEAD:$IMAGES_BRANCH
                            git tag $VERSION_TAG && git push https://\${GIT_USERNAME}:\${GIT_PASSWORD}@${IMAGES_REPOSITORY} $VERSION_TAG
                        """
                    }
                }
            }
        }

        stage('Merge candidates to the branch') {
            if (!DRY_RUN) {
                dir(DEBEZIUM_DIR) {
                    withCredentials([usernamePassword(credentialsId: GIT_CREDENTIALS_ID, passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME')]) {
                        sh """
                           git pull --rebase https://\${GIT_USERNAME}:\${GIT_PASSWORD}@$DEBEZIUM_REPOSITORY $CANDIDATE_BRANCH && \\
                           git checkout $DEBEZIUM_BRANCH && \\
                           git rebase $CANDIDATE_BRANCH && \\
                           git push https://\${GIT_USERNAME}:\${GIT_PASSWORD}@$DEBEZIUM_REPOSITORY HEAD:$DEBEZIUM_BRANCH && \\
                           git push --delete https://\${GIT_USERNAME}:\${GIT_PASSWORD}@$DEBEZIUM_REPOSITORY $CANDIDATE_BRANCH
                        """
                    }
                }
                ADDITIONAL_REPOSITORIES.each { id, repo ->
                    dir(id) {
                        withCredentials([usernamePassword(credentialsId: GIT_CREDENTIALS_ID, passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME')]) {
                            sh """
                               git pull --rebase https://\${GIT_USERNAME}:\${GIT_PASSWORD}@${repo.git} $CANDIDATE_BRANCH && \\
                               git checkout ${repo.branch} && \\
                               git rebase $CANDIDATE_BRANCH && \\
                               git push https://\${GIT_USERNAME}:\${GIT_PASSWORD}@${repo.git} HEAD:${repo.branch} && \\
                               git push --delete https://\${GIT_USERNAME}:\${GIT_PASSWORD}@${repo.git} $CANDIDATE_BRANCH
                            """
                        }
                    }
                }
            }
        }

        stage('Debezium UI') {
            if (!DRY_RUN) {
                dir(UI_DIR) {
                    withCredentials([usernamePassword(credentialsId: GIT_CREDENTIALS_ID, passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME')]) {
                        sh "git tag $VERSION_TAG && git push \"https://\${GIT_USERNAME}:\${GIT_PASSWORD}@${UI_REPOSITORY}\" $VERSION_TAG"
                    }
                }
            }
        }

    }

    mail to: MAIL_TO, subject: "${JOB_NAME} run #${BUILD_NUMBER} finished with ${currentBuild.currentResult}", body: "Run ${BUILD_URL} finished with result: ${currentBuild.currentResult}"
}
