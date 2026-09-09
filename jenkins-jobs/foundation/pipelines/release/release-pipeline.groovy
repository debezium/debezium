import groovy.json.*
import groovy.transform.Field
import java.util.stream.*

@Library('dbz-libs') _

properties([
    parameters([
        string(name: 'RELEASE_VERSION'),
        string(name: 'DEVELOPMENT_VERSION'),
        string(name: 'SOURCE_BRANCH'),
        string(name: 'SOURCE_REPOSITORIES'),
        string(name: 'DESCRIPTOR_REPOSITORY'),
        string(name: 'DESCRIPTOR_BRANCH'),
        string(name: 'IMAGES_REPOSITORY'),
        string(name: 'IMAGES_BRANCH'),
        string(name: 'POSTGRES_DECODER_REPOSITORY'),
        string(name: 'POSTGRES_DECODER_BRANCH'),
        string(name: 'ZULIP_TO'),
        booleanParam(name: 'FROM_SCRATCH'),
        booleanParam(name: 'IGNORE_SNAPSHOTS'),
        booleanParam(name: 'CHECK_BACKPORTS'),
        booleanParam(name: 'LATEST_SERIES'),
        booleanParam(name: 'IGNORE_RELEASE_PLAN_INCONSISTENCIES'),
        string(name: 'DBZ_PR_URL'),
        string(name: 'WEBSITE_PR_URL')
    ])
])


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
    [envVar: 'MAVEN_USERNAME', secretRef: 'op://Debezium Secrets Limited/Maven Central/publishusername'],
    [envVar: 'MAVEN_TOKEN', secretRef: 'op://Debezium Secrets Limited/Maven Central/publishtoken'],
    [envVar: 'ZULIPBOT_USERNAME', secretRef: 'op://Debezium Secrets Limited/Zulip Jenkins Bot/username'],
    [envVar: 'ZULIPBOT_TOKEN', secretRef: 'op://Debezium Secrets Limited/Zulip Jenkins Bot/password']
]

@Field final GIT_CREDENTIALS_ID = 'debezium-github'
@Field final HOME_DIR = '/var/lib/jenkins'
@Field final GPG_DIR = 'gpg'

@Field final DEBEZIUM_DIR = 'debezium'
@Field final WEBSITE_DIR = 'website'
@Field final DESCRIPTORS_REPO_DIR = 'debezium-descriptors-registry'
@Field final IMAGES_DIR = 'images'
@Field final PLATFORM_STAGE_DIR = 'platform'
@Field final POSTGRES_DECODER_DIR = 'postgres-decoder'

@Field final INSTALL_ARTIFACTS_SCRIPT = 'install-artifacts.sh'
@Field final CONNECTORS_PER_VERSION = [
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
    '2.6' : ['mongodb', 'mysql', 'postgres', 'sqlserver', 'oracle', 'cassandra-3', 'cassandra-4', 'db2', 'vitess', 'spanner', 'jdbc', 'informix', 'ibmi'],
    '2.7' : ['mongodb', 'mysql', 'postgres', 'sqlserver', 'oracle', 'cassandra-3', 'cassandra-4', 'db2', 'vitess', 'spanner', 'jdbc', 'informix', 'ibmi', 'mariadb'],
    '3.0' : ['mongodb', 'mysql', 'postgres', 'sqlserver', 'oracle', 'cassandra-3', 'cassandra-4', 'db2', 'vitess', 'spanner', 'jdbc', 'informix', 'ibmi', 'mariadb'],
    '3.1' : ['mongodb', 'mysql', 'postgres', 'sqlserver', 'oracle', 'cassandra-3', 'cassandra-4', 'db2', 'vitess', 'spanner', 'jdbc', 'informix', 'ibmi', 'mariadb'],
    '3.2' : ['mongodb', 'mysql', 'postgres', 'sqlserver', 'oracle', 'cassandra-3', 'cassandra-4', 'db2', 'vitess', 'spanner', 'jdbc', 'informix', 'ibmi', 'mariadb'],
    '3.3' : ['mongodb', 'mysql', 'postgres', 'sqlserver', 'oracle', 'cassandra-3', 'cassandra-4', 'db2', 'vitess', 'spanner', 'jdbc', 'informix', 'ibmi', 'mariadb', 'cockroachdb'],
    '3.4' : ['mongodb', 'mysql', 'postgres', 'sqlserver', 'oracle', 'cassandra-3', 'cassandra-4', 'db2', 'vitess', 'spanner', 'jdbc', 'informix', 'ibmi', 'mariadb', 'cockroachdb'],
    '3.5' : ['mongodb', 'mysql', 'postgres', 'sqlserver', 'oracle', 'cassandra-3', 'cassandra-4', 'db2', 'vitess', 'spanner', 'jdbc', 'informix', 'ingres', 'ibmi', 'mariadb', 'cockroachdb'],
    '3.6' : ['mongodb', 'mysql', 'postgres', 'sqlserver', 'oracle', 'cassandra-3', 'cassandra-4', 'db2', 'vitess', 'spanner', 'jdbc', 'informix', 'ingres', 'ibmi', 'mariadb', 'cockroachdb', 'yashandb'],
    '3.7' : ['mongodb', 'mysql', 'postgres', 'sqlserver', 'oracle', 'cassandra-3', 'cassandra-4', 'db2', 'vitess', 'spanner', 'jdbc', 'informix', 'ingres', 'ibmi', 'mariadb', 'cockroachdb', 'yashandb', 'tidb']
]
@Field final ZULIP_URL = 'https://debezium.zulipchat.com/api/v1'

@Field final POSTGRES_TAGS = ['14', '14-alpine', '15', '15-alpine', '16', '16-alpine', '17', '17-alpine', '18', '18-alpine']
@Field final IMAGES = ['connect', 'connect-base', 'examples/mysql', 'examples/mysql-gtids', 'examples/mysql-replication/master', 'examples/mysql-replication/replica', 'examples/mariadb', 'examples/postgres', 'examples/mongodb', 'kafka', 'server', 'operator', 'platform-conductor', 'platform-stage']
@Field final MAVEN_CENTRAL = 'https://repo1.maven.org/maven2'
@Field final LOCAL_MAVEN_REPO = "$HOME_DIR/.m2/repository"

@Field final PUBLISH_TIMEOUT_MINUTES = 90
@Field final PUBLISH_POLL_INTERVAL_MS = 15000

@Field final MAVEN_REPOSITORIES = [:]

// Defines the release execution order for the 'Perform release' stage.
// Tiers are executed sequentially. Within each tier repos are released sequentially.
// After all uploads in a tier complete, the pipeline waits for Maven Central propagation
// of every uploaded repo before moving to the next tier, ensuring dependencies are resolvable.
//
// Ordering constraints:
//   - debezium (core) must be first — all other repos depend on it in the local Maven repo
//   - connectors (tier 2) need debezium in the local Maven repo only, not yet in Maven Central
//   - quarkus and operator need debezium available in Maven Central
//   - server needs operator available in Maven Central
//   - platform needs server and operator available in Maven Central
//
// To add a new repository:
//   1. Add it to SOURCE_REPOSITORIES parameter in common_parameters.groovy
//   2. Add it to the appropriate tier below (or add a new tier if it has new dependencies)
//   The pipeline validates at startup that RELEASE_PLAN and SOURCE_REPOSITORIES are in sync.
@Field final RELEASE_PLAN = [
    ['debezium'],
    ['cassandra', 'cockroachdb', 'db2', 'ibmi', 'informix', 'ingres', 'spanner', 'vitess', 'tidb', 'yashandb'],
    ['quarkus', 'operator'],
    ['server'],
    ['platform'],
]

@Field DRY_RUN
@Field FROM_SCRATCH
@Field IGNORE_SNAPSHOTS
@Field CHECK_BACKPORTS
@Field LATEST_SERIES
@Field IGNORE_RELEASE_PLAN_INCONSISTENCIES

@Field RELEASE_VERSION
@Field DEVELOPMENT_VERSION
@Field SOURCE_BRANCH
@Field SOURCE_REPOSITORIES
@Field DESCRIPTOR_REPOSITORY
@Field DESCRIPTOR_BRANCH
@Field IMAGES_BRANCH
@Field IMAGES_REPOSITORY
@Field POSTGRES_DECODER_BRANCH
@Field POSTGRES_DECODER_REPOSITORY
@Field DESCRIPTORS_OUTPUT_DIR
@Field DBZ_PR_URL
@Field WEBSITE_PR_URL

@Field CONNECTORS

@Field VERSION_TAG
@Field VERSION_PARTS
@Field VERSION_MAJOR_MINOR
@Field IMAGE_TAG
@Field CANDIDATE_BRANCH

@Field STAGING_REPO = ''

@Field ZULIP_TO

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
                'MAVEN_USERNAME': MAVEN_USERNAME,
                'MAVEN_TOKEN': MAVEN_TOKEN,
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

/**
 * Runs all pre-release content checks against the provided text strings and appends
 * human-readable issue descriptions to the supplied {@code issues} list.
 *
 * @param dbzContent         contents of debezium/CHANGELOG.md
 * @param copyrightContent   contents of debezium/COPYRIGHT.txt
 * @param releaseNotesContent contents of debezium.github.io releases/.../release-notes.asciidoc
 * @param versionYmlContent  contents of debezium.github.io _data/releases/.../${RELEASE_VERSION}.yml
 * @param issues             mutable list to which issue strings are appended
 * @param logPrefix          prefix for log lines: "WARNING" for pre-merge inspection, "ERROR" for post-merge check
 */
def checkPreReleaseContent(String dbzContent, String copyrightContent, String releaseNotesContent, String versionYmlContent, List issues, String logPrefix) {
    // Check COPYRIGHT.txt for unresolved placeholders
    copyrightContent.readLines().findAll { it.contains('# PLACEHOLDER') }.each {
        issues << "COPYRIGHT.txt: unresolved placeholder: ${it.trim()}"
        echo "$logPrefix: COPYRIGHT.txt unresolved placeholder: ${it.trim()}"
    }

    // Check CHANGELOG.md contains RELEASE_VERSION
    if (!dbzContent.contains(RELEASE_VERSION)) {
        issues << "CHANGELOG.md: does not contain $RELEASE_VERSION"
        echo "$logPrefix: CHANGELOG.md does not contain $RELEASE_VERSION"
    }

    // Check release-notes.asciidoc contains RELEASE_VERSION
    if (!releaseNotesContent.contains(RELEASE_VERSION)) {
        issues << "release-notes.asciidoc: does not contain $RELEASE_VERSION"
        echo "$logPrefix: release-notes.asciidoc does not contain $RELEASE_VERSION"
    }

    // Check release-notes.asciidoc for unresolved breaking-changes placeholders
    releaseNotesContent.readLines().findAll { it.contains('[Placeholder for Breaking changes text]') }.each {
        issues << "release-notes.asciidoc: unresolved breaking-changes placeholder: ${it.trim()}"
        echo "$logPrefix: release-notes.asciidoc unresolved breaking-changes placeholder: ${it.trim()}"
    }

    // Check VERSION.yml summary field is not empty or a placeholder
    def summaryLine = versionYmlContent.readLines().find { it.startsWith('summary:') }
    def summaryValue = summaryLine ? summaryLine.replaceFirst(/^summary:\s*/, '').trim() : ''
    if (!summaryValue || summaryValue.contains('PLACEHOLDER')) {
        issues << "Version YAML: summary field is empty or a placeholder"
        echo "$logPrefix: ${RELEASE_VERSION}.yml summary field is empty or a placeholder"
    }
}

@Field final BUILD_ARGS = [
   'debezium': '-Poracle-all',
 ]

// Debezium Server must always ignore snapshots as it depends on Debezium Server BOM
// and it is not possible to override it with a stable version
@Field final FORCE_IGNORE_SNAPSHOTS = [
   'server': true,
 ]

def buildArgsForRepo(repoDir) {
    BUILD_ARGS.getOrDefault(repoDir, "-Dversion.debezium=$RELEASE_VERSION") << ' -Dmaven.wagon.http.retryHandler.count=5'
}

@Field final TEST_ARTIFACTS = [
    'cassandra': 'debezium-connector-reactor-cassandra',
    'debezium': 'debezium-parent',
    'server': 'debezium-server',
    'operator': 'debezium-operator',
    'platform': 'debezium-platform-conductor',
    'quarkus': 'debezium-quarkus-extensions-parent'
]

def artifactExists(repoDir) {
    def artifactId = TEST_ARTIFACTS.getOrDefault(repoDir, "debezium-connector-${repoDir}")
    def url  = "https://repo1.maven.org/maven2/io/debezium/${artifactId}/$RELEASE_VERSION/${artifactId}-${RELEASE_VERSION}.pom"
    echo "Checking ${url}"
    sh(script: "curl -sSfI ${url} >/dev/null", returnStatus: true) == 0
}

// Commit messages written by the post-perform step functions below.
// The grep pattern in postPerformCommitExists() is derived from these constants
// update both together if a message ever changes.
@Field final POST_PERFORM_COMMIT_DEFAULT = '[release] New parent %s for development'
@Field final POST_PERFORM_COMMIT_DEBEZIUM = '[release] Development version for testing module deps'
@Field final POST_PERFORM_COMMIT_PATTERN = '\\[release\\] (New parent .+ for development|Development version for testing module deps)'

def branchExists(branchName) {
    sh(script: "git show-ref --verify --quiet refs/heads/${branchName}", returnStatus: true) == 0
}

def postPerformCommitExists() {
    sh(script: "git log --oneline | grep -qE '${POST_PERFORM_COMMIT_PATTERN}'", returnStatus: true) == 0
}

def gitPushCandidate(repoName) {
    if (!DRY_RUN) {
        echo "Pushing candidate branch to repository $repoName"
        withSecrets(config: ONE_PASSWORD_CONFIG, secrets: SECRETS) {
            sh "git push \"https://\${GITHUB_USERNAME}:\${GITHUB_PASSWORD}@${repoName}\" HEAD:${CANDIDATE_BRANCH} --follow-tags"
        }
    }
}

def gitPushTag(repoName) {
    if (!DRY_RUN) {
        echo "Pushing tag $VERSION_TAG to repository to $repoName"
        withSecrets(config: ONE_PASSWORD_CONFIG, secrets: SECRETS) {
            sh "git tag $VERSION_TAG && git push \"https://\${GITHUB_USERNAME}:\${GITHUB_PASSWORD}@${repoName}\" $VERSION_TAG"
        }
    }
}

def gitMergeAndDeleteCandidate(repoName, repoBranch) {
    if (!DRY_RUN) {
        echo "Merging candidate branch with $repoBranch in repository $repoName"
        executeShell('.',
        """
            git pull --rebase \"https://\${GITHUB_USERNAME}:\${GITHUB_PASSWORD}@$repoName\" $CANDIDATE_BRANCH && \\
            git pull --rebase \"https://\${GITHUB_USERNAME}:\${GITHUB_PASSWORD}@$repoName\" $repoBranch && \\
            git checkout $repoBranch && \\
            git rebase $CANDIDATE_BRANCH && \\
            git push --force \"https://\${GITHUB_USERNAME}:\${GITHUB_PASSWORD}@$repoName\" HEAD:$repoBranch && \\
            git push --delete \"https://\${GITHUB_USERNAME}:\${GITHUB_PASSWORD}@$repoName\" $CANDIDATE_BRANCH
        """
         )
    }
}

def defaultPrePrepareSteps() {
    fileUtils.modifyFile("pom.xml") {
        it.replaceFirst('<version>.+</version>\n    </parent>', "<version>$RELEASE_VERSION</version>\n    </parent>")
    }
    sh "git commit -a -m '[release] Stable parent $RELEASE_VERSION for release'"
}

def debeziumPrePrepareSteps() {
    fileUtils.modifyFile('debezium-testing/debezium-testing-system/pom.xml') {
        it.replaceFirst('<version.debezium.connector>.+</version.debezium.connector>', "<version.debezium.connector>$RELEASE_VERSION</version.debezium.connector>")
    }
    sh "git commit -a -m '[release] Stable $RELEASE_VERSION for testing module deps'"
}

def serverPrePrepareSteps() {
    fileUtils.modifyFile('debezium-server-bom/pom.xml') {
        it.replaceFirst('<version>.+</version>\n    </parent>', "<version>$RELEASE_VERSION</version>\n    </parent>")
    }
    defaultPrePrepareSteps()
}

def operatorPrePrepareSteps() {
    defaultPrePrepareSteps()

    // Update k8 resources and generate manifests for operator
    def buildArgs = "-Dversion.debezium=$RELEASE_VERSION"
    def profiles = "stable,k8update"
    def releaseProfiles = "stable"
    if (LATEST_SERIES) {
        profiles += ",olmLatest"
        releaseProfiles += ",olmLatest"
    }
    buildArgs +=" -P$releaseProfiles"
    BUILD_ARGS['operator'] = buildArgs
    sh "./mvnw clean install -P$profiles -DskipTests -DskipITs"
    sh "git commit -a -m '[release] Manifests and resources for $RELEASE_VERSION'"
}

@Field final PRE_PREPARE_STEPS = [
    'debezium': this.&debeziumPrePrepareSteps,
    'server': this.&serverPrePrepareSteps,
    'operator': this.&operatorPrePrepareSteps,
]

def defaultPostPerformSteps() {
    fileUtils.modifyFile("pom.xml") {
        it.replaceFirst('<version>.+</version>\n    </parent>', "<version>$DEVELOPMENT_VERSION</version>\n    </parent>")
    }

    sh "git commit -a -m \"${String.format(POST_PERFORM_COMMIT_DEFAULT, DEVELOPMENT_VERSION)}\""
}

def debeziumPostPerformSteps() {
    fileUtils.modifyFile('debezium-testing/debezium-testing-system/pom.xml') {
        it.replaceFirst('<version.debezium.connector>.+</version.debezium.connector>', '<version.debezium.connector>\\${project.version}</version.debezium.connector>')
    }
    sh "git commit -a -m \"${POST_PERFORM_COMMIT_DEBEZIUM}\""
}

def serverPostPerformSteps() {
    fileUtils.modifyFile('debezium-server-bom/pom.xml') {
        it.replaceFirst('<version>.+</version>\n    </parent>', "<version>$DEVELOPMENT_VERSION</version>\n    </parent>")
    }
    defaultPostPerformSteps()
}

def operatorPostPerformSteps() {
    fileUtils.modifyFile("pom.xml") {
        it.replaceFirst('<version>.+</version>\n    </parent>', "<version>$DEVELOPMENT_VERSION</version>\n    </parent>")
    }

    // For operator, we need to build with k8update profile to update manifests back to dev version
    sh "./mvnw clean package -Pk8update -DskipTests -DskipITs"
    sh "git commit -a -m \"${String.format(POST_PERFORM_COMMIT_DEFAULT, DEVELOPMENT_VERSION)}\""
}

@Field final POST_PERFORM_STEPS = [
    'debezium': this.&debeziumPostPerformSteps,
    'server': this.&serverPostPerformSteps,
    'operator': this.&operatorPostPerformSteps,
]

def releasePrepare(repoDir, repoName) {
    echo "Building current development version"
    sh "./mvnw clean install -T 1C -DskipTests -DskipITs -Passembly"

    def ignoreSnaphots = FORCE_IGNORE_SNAPSHOTS.getOrDefault(repoDir, IGNORE_SNAPSHOTS)

    def buildArgs = buildArgsForRepo(repoDir)

    echo 'Executing pre-prepare steps'
    PRE_PREPARE_STEPS.getOrDefault(repoDir, this.&defaultPrePrepareSteps)()

    echo 'Executing release:prepare'
    sh "env MAVEN_OPTS='-Xmx8g -Xms1g' ./mvnw release:clean release:prepare -DreleaseVersion=$RELEASE_VERSION -Dtag=$VERSION_TAG -DdevelopmentVersion=$DEVELOPMENT_VERSION -DpushChanges=${!DRY_RUN} -DignoreSnapshots=$ignoreSnaphots -DpreparationGoals='clean install' -Darguments=\"-DskipTests -DskipITs -Passembly $buildArgs\" $buildArgs"

    gitPushCandidate(repoName)
}

def releasePerformUpload(repoDir, repoName) {
    def buildArgs = buildArgsForRepo(repoDir)

    echo 'Executing release:perform'
    sendZulipNotification("Publishing version $RELEASE_VERSION of $repoDir")

    executeShell('.', "env MAVEN_USERNAME=\${MAVEN_USERNAME} MAVEN_TOKEN=\${MAVEN_TOKEN} MAVEN_OPTS='-Xmx8g -Xms1g' ./mvnw release:perform -DstagingProgressTimeoutMinutes=60 -DlocalCheckout=$DRY_RUN -Dpublish.auto=${!DRY_RUN} -Dpublish.skip=${DRY_RUN} -Dpublish.wait.until=uploaded -DconnectionUrl=\"scm:git:https://\${GITHUB_USERNAME}:\${GITHUB_PASSWORD}@${repoName}\" -Darguments=\"-s \\\$HOME/.m2/settings-snapshots.xml -DstagingProgressTimeoutMinutes=60 -Dpublish.auto=${!DRY_RUN} -Dpublish.skip=${DRY_RUN} -Dpublish.wait.until=uploaded -Dgpg.homedir=\\\$WORKSPACE/$GPG_DIR -Dgpg.passphrase=\${GPG_PASSPHRASE} -DskipTests -DskipITs -Dschema.generator.output.dir=${DESCRIPTORS_OUTPUT_DIR} $buildArgs\" $buildArgs")
}

def releasePerformPostSteps(repoDir, repoName) {
    def buildArgs = buildArgsForRepo(repoDir)

    echo "Building new development version"
    sh "env MAVEN_OPTS='-Xmx8g -Xms1g' ./mvnw clean install -T 1C -DskipTests -DskipITs -Passembly $buildArgs"

    echo 'Executing post-perform steps'
    POST_PERFORM_STEPS.getOrDefault(repoDir, this.&defaultPostPerformSteps)()

    gitPushCandidate(repoName)
}

def smokeTestContainerImages() {
    // Start HTTP server to simulate Maven staging repository
    sh """
        docker rm -f maven || true
        docker run -d --rm -v $LOCAL_MAVEN_REPO:/usr/share/nginx/html:Z -p 18080:80 --name maven mirror.gcr.io/nginx
    """
    sleep 5
    def mavenContainerIp = sh(
        script: "docker inspect maven | jq -r '.[0].NetworkSettings.IPAddress'",
        returnStdout: true
    ).trim()
    STAGING_REPO = "http://$mavenContainerIp"

    def sums = [:]
    for (def i = 0; i < CONNECTORS.size(); i++) {
        def connector = CONNECTORS[i]
        dir("$LOCAL_MAVEN_REPO/io/debezium/debezium-connector-$connector/$RELEASE_VERSION") {
            def md5sum = sh(script: "md5sum -b debezium-connector-${connector}-${RELEASE_VERSION}-plugin.tar.gz | awk '{print \$1}'", returnStdout: true).trim()
            sums["${connector.toUpperCase()}"] = md5sum
        }
    }
    echo "MD5 sums calculated: ${sums}"
    def serverSum = sh(script: "md5sum -b $LOCAL_MAVEN_REPO/io/debezium/debezium-server-dist/$RELEASE_VERSION/debezium-server-dist-${RELEASE_VERSION}.tar.gz | awk '{print \$1}'", returnStdout: true).trim()
    def operatorSum = sh(script: "md5sum -b $LOCAL_MAVEN_REPO/io/debezium/debezium-operator-dist/$RELEASE_VERSION/debezium-operator-dist-${RELEASE_VERSION}.tar.gz | awk '{print \$1}'", returnStdout: true).trim()
    def platformSum = sh(script: "md5sum -b $LOCAL_MAVEN_REPO/io/debezium/debezium-platform-conductor/$RELEASE_VERSION/debezium-platform-conductor-${RELEASE_VERSION}.tar.gz | awk '{print \$1}'", returnStdout: true).trim()
    sums['SCRIPTING'] = sh(script: "md5sum -b $LOCAL_MAVEN_REPO/io/debezium/debezium-scripting/$RELEASE_VERSION/debezium-scripting-${RELEASE_VERSION}.tar.gz | awk '{print \$1}'", returnStdout: true).trim()
    sums['CHRONICLE_QUEUE'] = sh(script: "md5sum -b $LOCAL_MAVEN_REPO/io/debezium/debezium-storage-chronicle-queue/$RELEASE_VERSION/debezium-storage-chronicle-queue-${RELEASE_VERSION}-store.tar.gz | awk '{print \$1}'", returnStdout: true).trim()
    dir("$IMAGES_DIR/connect/$IMAGE_TAG") {
        echo 'Modifying main Dockerfile'
        def additionalRepoList = MAVEN_REPOSITORIES.collect({ id, repo -> "${id.toUpperCase()}=$STAGING_REPO" }).join(' ')
        fileUtils.modifyFile('Dockerfile') {
            def ret = it
                    .replaceFirst('DEBEZIUM_VERSION="\\S+"', "DEBEZIUM_VERSION=\"$RELEASE_VERSION\"")
                    .replaceFirst('MAVEN_REPO_CENTRAL="[^"]*"', "MAVEN_REPO_CENTRAL=\"$STAGING_REPO\"")
                    .replaceFirst('MAVEN_REPOS_ADDITIONAL="[^"]*"', "MAVEN_REPOS_ADDITIONAL=\"$additionalRepoList\"")
            for (entry in sums) {
                ret = ret.replaceFirst("${entry.key}_MD5=\\S+", "${entry.key}_MD5=${entry.value}")
            }
            return ret
        }
        fileUtils.modifyFile('Dockerfile.local') {
            it
                    .replaceFirst('DEBEZIUM_VERSION=\\S+', "DEBEZIUM_VERSION=$RELEASE_VERSION")
        }
    }
    echo 'Modifying snapshot Dockerfile'
    dir("$IMAGES_DIR/connect/snapshot") {
        fileUtils.modifyFile('Dockerfile') {
            it.replaceFirst('DEBEZIUM_VERSION=\\S+', "DEBEZIUM_VERSION=$DEVELOPMENT_VERSION")
        }
    }
    echo 'Modifying Server Dockerfile'
    dir("$IMAGES_DIR/server/$IMAGE_TAG") {
        fileUtils.modifyFile('Dockerfile') {
            it
                    .replaceFirst('MAVEN_REPO_CENTRAL="[^"]*"', "MAVEN_REPO_CENTRAL=\"$STAGING_REPO\"")
                    .replaceAll('DEBEZIUM_VERSION=\\S+', "DEBEZIUM_VERSION=$RELEASE_VERSION")
                    .replaceFirst('SERVER_MD5=\\S+', "SERVER_MD5=$serverSum")
        }
    }
    echo 'Modifying Server snapshot Dockerfile'
    dir("$IMAGES_DIR/server/snapshot") {
        fileUtils.modifyFile('Dockerfile') {
            it.replaceFirst('DEBEZIUM_VERSION=\\S+', "DEBEZIUM_VERSION=$DEVELOPMENT_VERSION")
        }
    }
    echo 'Modifying Operator Dockerfile'
    dir("$IMAGES_DIR/operator/$IMAGE_TAG") {
        fileUtils.modifyFile('Dockerfile') {
            it
                    .replaceFirst('MAVEN_REPO_CENTRAL="[^"]*"', "MAVEN_REPO_CENTRAL=\"$STAGING_REPO\"")
                    .replaceFirst('DEBEZIUM_VERSION=\\S+', "DEBEZIUM_VERSION=$RELEASE_VERSION")
                    .replaceFirst('OPERATOR_MD5=\\S+', "OPERATOR_MD5=$operatorSum")
        }
    }
    echo 'Modifying Operator snapshot Dockerfile'
    dir("$IMAGES_DIR/operator/snapshot") {
        fileUtils.modifyFile('Dockerfile') {
            it.replaceFirst('DEBEZIUM_VERSION=\\S+', "DEBEZIUM_VERSION=$DEVELOPMENT_VERSION")
        }
    }

    echo 'Modifying Platform Conductor Dockerfile'
    dir("$IMAGES_DIR/platform-conductor/$IMAGE_TAG") {
        fileUtils.modifyFile('Dockerfile') {
            it
                    .replaceFirst('MAVEN_REPO_CENTRAL="[^"]*"', "MAVEN_REPO_CENTRAL=\"$STAGING_REPO\"")
                    .replaceFirst('DEBEZIUM_VERSION=\\S+', "DEBEZIUM_VERSION=$RELEASE_VERSION")
                    .replaceFirst('PLATFORM_CONDUCTOR_MD5=\\S+', "PLATFORM_CONDUCTOR_MD5=$platformSum")
        }
    }
    echo 'Modifying Platform Conductor snapshot Dockerfile'
    dir("$IMAGES_DIR/platform-conductor/snapshot") {
        fileUtils.modifyFile('Dockerfile') {
            it.replaceFirst('DEBEZIUM_VERSION=\\S+', "DEBEZIUM_VERSION=$DEVELOPMENT_VERSION")
        }
    }

    echo 'Modifying Platform stage Dockerfile'
    dir("$IMAGES_DIR/platform-stage/$IMAGE_TAG") {
        fileUtils.modifyFile('Dockerfile') {
            it
                    .replaceFirst('BRANCH=\\S+', "BRANCH=$VERSION_TAG")
                    .replaceFirst(/RUN git clone -b \$\{BRANCH\} https:\/\/github.com\/debezium\/debezium-platform.git/, "COPY $PLATFORM_STAGE_DIR ./debezium-platform")
        }
    }

    echo 'Modifying container images build scripts'
    dir(IMAGES_DIR) {
        fileUtils.modifyFile('build-all-multiplatform.sh') {
            it.replaceFirst('DEBEZIUM_VERSION=\"\\S+\"', "DEBEZIUM_VERSION=\"$IMAGE_TAG\"")
        }
        fileUtils.modifyFile('build-all.sh') {
            it.replaceFirst('DEBEZIUM_VERSION=\"\\S+\"', "DEBEZIUM_VERSION=\"$IMAGE_TAG\"")
        }
    }

    dir(PLATFORM_STAGE_DIR) {
        sh "git tag -f $VERSION_TAG"
        sh "mkdir -p $WORKSPACE/$IMAGES_DIR/platform-stage/$IMAGE_TAG/$PLATFORM_STAGE_DIR && cp -r ./* $WORKSPACE/$IMAGES_DIR/platform-stage/$IMAGE_TAG/$PLATFORM_STAGE_DIR"
    }

/*
    dir(IMAGES_DIR) {
        script {
            env.DEBEZIUM_DOCKER_REGISTRY_PRIMARY_NAME='localhost:5500/debeziumquay'
            env.DEBEZIUM_DOCKER_REGISTRY_SECONDARY_NAME='localhost:5500/debezium'
        }
        sh """
            docker run --privileged --rm mirror.gcr.io/tonistiigi/binfmt --install all
            ./setup-local-builder.sh
            docker compose -f local-registry/docker-compose.yml up -d
            env DRY_RUN=false SKIP_UI=false MULTIPLATFORM_PLATFORMS="linux/amd64" ./build-all-multiplatform.sh
        """
    }
    sh """
        docker rm -f connect kafka mysql || true
        docker run -it -d --name mysql -p 53306:3306 -e MYSQL_ROOT_PASSWORD=debezium -e MYSQL_USER=mysqluser -e MYSQL_PASSWORD=mysqlpw $DEBEZIUM_DOCKER_REGISTRY_PRIMARY_NAME/example-mysql:$IMAGE_TAG
        sleep 10
        docker run -it -d --name kafka -p 9092:9092 $DEBEZIUM_DOCKER_REGISTRY_PRIMARY_NAME/kafka:$IMAGE_TAG
        sleep 10
        docker run -it -d --name connect -p 8083:8083 -e GROUP_ID=1 -e CONFIG_STORAGE_TOPIC=my_connect_configs -e OFFSET_STORAGE_TOPIC=my_connect_offsets --link kafka:kafka --link mysql:mysql $DEBEZIUM_DOCKER_REGISTRY_PRIMARY_NAME/connect:$IMAGE_TAG
        sleep 60

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
        def watcherlog = sh(script: "docker run --name watcher --rm --link kafka:kafka $DEBEZIUM_DOCKER_REGISTRY_PRIMARY_NAME/kafka:$IMAGE_TAG watch-topic -a -k dbserver1.inventory.customers --max-messages 2 2>&1", returnStdout: true).trim()
        echo watcherlog
        sh 'docker rm -f connect kafka mysql maven'
        if (!watcherlog.contains('Processed a total of 2 messages')) {
            error 'Tutorial watcher did not reported messages'
        }
    }
*/
}


node {
    if (
        !params.RELEASE_VERSION ||
        !params.DEVELOPMENT_VERSION ||
        !params.SOURCE_BRANCH ||
        !params.SOURCE_REPOSITORIES ||
        !params.DESCRIPTOR_REPOSITORY ||
        !params.DESCRIPTOR_BRANCH ||
        !params.IMAGES_REPOSITORY ||
        !params.IMAGES_BRANCH ||
        !params.POSTGRES_DECODER_REPOSITORY ||
        !params.POSTGRES_DECODER_BRANCH ||
        !params.DBZ_PR_URL ||
        !params.WEBSITE_PR_URL
    ) {
        error 'Input parameters not provided'
    }

    DRY_RUN = common.getBooleanParameter(params.DRY_RUN)
    FROM_SCRATCH = common.getBooleanParameter(params.FROM_SCRATCH)
    IGNORE_SNAPSHOTS = common.getBooleanParameter(params.IGNORE_SNAPSHOTS)
    CHECK_BACKPORTS = common.getBooleanParameter(params.CHECK_BACKPORTS)
    LATEST_SERIES = common.getBooleanParameter(params.LATEST_SERIES)
    IGNORE_RELEASE_PLAN_INCONSISTENCIES = common.getBooleanParameter(params.IGNORE_RELEASE_PLAN_INCONSISTENCIES)

    echo "Ignore snapshots: ${IGNORE_SNAPSHOTS}"
    echo "Check backports: ${CHECK_BACKPORTS}"
    echo "From scratch: ${FROM_SCRATCH}"
    echo "Latest series: ${LATEST_SERIES}"
    echo "Ignore release plan inconsistencies: ${IGNORE_RELEASE_PLAN_INCONSISTENCIES}"

    RELEASE_VERSION = params.RELEASE_VERSION
    DEVELOPMENT_VERSION = params.DEVELOPMENT_VERSION
    SOURCE_BRANCH = params.SOURCE_BRANCH
    SOURCE_REPOSITORIES = params.SOURCE_REPOSITORIES
    DESCRIPTOR_REPOSITORY = params.DESCRIPTOR_REPOSITORY
    DESCRIPTOR_BRANCH = params.DESCRIPTOR_BRANCH
    IMAGES_BRANCH = params.IMAGES_BRANCH
    IMAGES_REPOSITORY = params.IMAGES_REPOSITORY
    POSTGRES_DECODER_BRANCH = params.POSTGRES_DECODER_BRANCH
    POSTGRES_DECODER_REPOSITORY = params.POSTGRES_DECODER_REPOSITORY
    ZULIP_TO = params.ZULIP_TO
    DBZ_PR_URL = params.DBZ_PR_URL
    WEBSITE_PR_URL = params.WEBSITE_PR_URL

    VERSION_TAG = "v$RELEASE_VERSION"
    VERSION_PARTS = RELEASE_VERSION.split('\\.')
    VERSION_MAJOR_MINOR = "${VERSION_PARTS[0]}.${VERSION_PARTS[1]}"
    CANDIDATE_BRANCH = "candidate-$RELEASE_VERSION"

    IMAGE_TAG = VERSION_MAJOR_MINOR
    echo "Images tagged with $IMAGE_TAG will be used"

    CONNECTORS = CONNECTORS_PER_VERSION[VERSION_MAJOR_MINOR]
    if (CONNECTORS == null) {
        error "List of connectors not available"
    }
    echo "Connectors to be released: $CONNECTORS"

    // Repositories are parsed from SOURCE_REPOSITORIES in the order they are listed.
    // Ordering matters for 'Prepare release' which iterates MAVEN_REPOSITORIES directly.
    SOURCE_REPOSITORIES.split().each { item ->
        item.tokenize('#').with { parts ->
            switch(parts.size()) {
                case 2:
                    MAVEN_REPOSITORIES[parts[0]] = ['git': parts[1], subDir: ".", 'branch': SOURCE_BRANCH]
                    break
                case 3:
                    MAVEN_REPOSITORIES[parts[0]] = ['git': parts[1], subDir: ".", 'branch': parts[2]]
                    break
                case 4:
                    MAVEN_REPOSITORIES[parts[0]] = ['git': parts[1], subDir: parts[2], 'branch': parts[3]]
                    break
            }
            echo "Repository ${parts[1]} will be used, branch ${MAVEN_REPOSITORIES[parts[0]].branch}"
        }
    }

    catchError {
        sendZulipNotification("Starting build of Debezium $RELEASE_VERSION ($BUILD_URL)")

        stage('Validate parameters') {
            common.validateVersionFormat(RELEASE_VERSION)

            if (!(DEVELOPMENT_VERSION ==~ /\d+\.\d+.\d+\-SNAPSHOT/)) {
                error "Development version '$DEVELOPMENT_VERSION' is not of the required format x.y.z-SNAPSHOT"
            }

            if (FROM_SCRATCH && fileExists(DEBEZIUM_DIR)) {
                input message: 'Really start from scratch?', ok: 'yes', cancel: 'no'
            }

            def planIds = RELEASE_PLAN.flatten().toSet()
            def repoIds = MAVEN_REPOSITORIES.keySet()
            def missing = repoIds - planIds
            def extra = planIds - repoIds
            if (!IGNORE_RELEASE_PLAN_INCONSISTENCIES) {
                if (missing) { error "RELEASE_PLAN is missing repositories: ${missing.sort()}" }
                if (extra) { error "RELEASE_PLAN contains unknown repositories: ${extra.sort()}" }
            } else if (missing || extra) {
                echo "WARNING: Ignoring release plan inconsistencies — missing: ${missing.sort()}, extra: ${extra.sort()}"
            }
        }

        stage('Initialize') {
            DESCRIPTORS_OUTPUT_DIR = "${WORKSPACE}/descriptors-output"

            if (!FROM_SCRATCH) {
                return
            }
            deleteDir()
            sh "rm -rf $LOCAL_MAVEN_REPO/io/debezium"

            echo 'Configuring git'
            sh "git config user.email || git config --global user.email \"debezium@gmail.com\" && git config --global user.name \"Debezium Builder\""
            sh "ssh-keyscan github.com >> $HOME_DIR/.ssh/known_hosts"

            echo "Configuring GPG in '${GPG_DIR}'"
            executeShell(GPG_DIR, '''gpg --import --batch --passphrase ${GPG_PASSPHRASE} --homedir . <<-EOF
${GPG_PRIVATE_KEY}
EOF''')

            MAVEN_REPOSITORIES.each { id, repo ->
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
                      branches                         : [[name: "*/$POSTGRES_DECODER_BRANCH"]],
                      doGenerateSubmoduleConfigurations: false,
                      extensions                       : [[$class: 'RelativeTargetDirectory', relativeTargetDir: POSTGRES_DECODER_DIR]],
                      submoduleCfg                     : [],
                      userRemoteConfigs                : [[url: "https://$POSTGRES_DECODER_REPOSITORY", credentialsId: GIT_CREDENTIALS_ID]]
            ]
            )
            checkout([$class                           : 'GitSCM',
                      branches                         : [[name: "*/$DESCRIPTOR_BRANCH"]],
                      doGenerateSubmoduleConfigurations: false,
                      extensions                       : [[$class: 'RelativeTargetDirectory', relativeTargetDir: DESCRIPTORS_REPO_DIR]],
                      submoduleCfg                     : [],
                      userRemoteConfigs                : [[url: "https://$DESCRIPTOR_REPOSITORY", credentialsId: GIT_CREDENTIALS_ID]]
            ]
            )

            echo 'Install Oracle dependencies'
            dir(DEBEZIUM_DIR) {
                def ORACLE_ARTIFACT_VERSION = (readFile('pom.xml') =~ /(?ms)<version.oracle.driver>(.+)<\/version.oracle.driver>/)[0][1]
                def ORACLE_INSTANTCLIENT_ARTIFACT_VERSION = (readFile('pom.xml') =~ /(?ms)<version.oracle.instantclient>(.+)<\/version.oracle.instantclient>/)[0][1]
                def ORACLE_ARTIFACT_DIR = "/tmp/oracle-libs/${ORACLE_ARTIFACT_VERSION}${((ORACLE_ARTIFACT_VERSION =~ /^(\d+)/)[0][1] as int) >= 23 ? '' : '.0'}"

                sh "./mvnw install:install-file -DgroupId=com.oracle.instantclient -DartifactId=xstreams -Dversion=$ORACLE_INSTANTCLIENT_ARTIFACT_VERSION -Dpackaging=jar -Dfile=$ORACLE_ARTIFACT_DIR/xstreams.jar"
            }
        }

        stage('Await pre-release PRs') {
            if (DRY_RUN || !FROM_SCRATCH) {
                return
            }

            def issues = []

            withSecrets(config: ONE_PASSWORD_CONFIG, secrets: SECRETS) {
                // Fetch DBZ PR head ref as owner/repo/branch (pre-merge: content lives on the PR branch)
                def dbzRef = sh(
                    script: """GH_TOKEN=\${GITHUB_PASSWORD} gh pr view $DBZ_PR_URL \
--json headRefName,headRepositoryOwner,headRepository \
-q '[.headRepositoryOwner.login,.headRepository.name,.headRefName] | join("/")'""",
                    returnStdout: true
                ).trim().tokenize('/')
                def (dbzOwner, dbzRepo, dbzBranch) = dbzRef

                // Fetch WEBSITE PR head ref as owner/repo/branch (pre-merge: content lives on the PR branch)
                def webRef = sh(
                    script: """GH_TOKEN=\${GITHUB_PASSWORD} gh pr view $WEBSITE_PR_URL \
--json headRefName,headRepositoryOwner,headRepository \
-q '[.headRepositoryOwner.login,.headRepository.name,.headRefName] | join("/")'""",
                    returnStdout: true
                ).trim().tokenize('/')
                def (webOwner, webRepo, webBranch) = webRef

                def rawUrl = { owner, repo, branch, path ->
                    "https://raw.githubusercontent.com/${owner}/${repo}/${branch}/${path}"
                }

                def changelogUrl    = rawUrl(dbzOwner, dbzRepo, dbzBranch, 'CHANGELOG.md')
                def copyrightUrl    = rawUrl(dbzOwner, dbzRepo, dbzBranch, 'COPYRIGHT.txt')
                def releaseNotesUrl = rawUrl(webOwner, webRepo, webBranch, "releases/$VERSION_MAJOR_MINOR/release-notes.asciidoc")
                def versionYmlUrl   = rawUrl(webOwner, webRepo, webBranch, "_data/releases/$VERSION_MAJOR_MINOR/${RELEASE_VERSION}.yml")

                echo "Checking CHANGELOG.md at:        $changelogUrl"
                echo "Checking COPYRIGHT.txt at:       $copyrightUrl"
                echo "Checking release-notes at:       $releaseNotesUrl"
                echo "Checking version YAML at:        $versionYmlUrl"

                def changelog    = new URL(changelogUrl).text
                def copyright    = new URL(copyrightUrl).text
                def releaseNotes = new URL(releaseNotesUrl).text
                def versionYml   = new URL(versionYmlUrl).text

                checkPreReleaseContent(changelog, copyright, releaseNotes, versionYml, issues, 'WARNING')
            }

            def issuesSummary = issues ? issues.collect { "  - $it" }.join('\n') : '  No issues found — ready to merge'
            sendZulipNotification("""\
Release $RELEASE_VERSION pre-release PRs are pending review.
  debezium PR:            $DBZ_PR_URL
  debezium.github.io PR:  $WEBSITE_PR_URL
Issues found:
${issuesSummary}
  Jenkins input URL:      $BUILD_URL/input""")
            input(
                message: """\
Review and merge both pre-release PRs before continuing.
Issues found:
${issuesSummary}

  $DBZ_PR_URL
  $WEBSITE_PR_URL""",
                ok: 'Both PRs are merged, continue'
            )

            withSecrets(config: ONE_PASSWORD_CONFIG, secrets: SECRETS) {
                // Re-fetch main repo to pick up merged PR changes
                dir(DEBEZIUM_DIR) {
                    sh "git pull https://\${GITHUB_USERNAME}:\${GITHUB_PASSWORD}@github.com/debezium/debezium.git $SOURCE_BRANCH"
                }

                // Re-fetch docs repo to pick up merged PR changes
                if (!fileExists(WEBSITE_DIR)) {
                    sh "git clone --depth=1 -b develop https://\${GITHUB_USERNAME}:\${GITHUB_PASSWORD}@github.com/debezium/debezium.github.io.git $WEBSITE_DIR"
                } else {
                    dir(WEBSITE_DIR) {
                        sh "git pull https://\${GITHUB_USERNAME}:\${GITHUB_PASSWORD}@github.com/debezium/debezium.github.io.git develop"
                    }
                }
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
            }
        }

        stage('Check GitHub Project') {
            if (!DRY_RUN) {
            }
        }

        stage('Check changelog') {
            if (!DRY_RUN) {
                def failures = []

                withSecrets(config: ONE_PASSWORD_CONFIG, secrets: SECRETS) {
                    // Check DBZ PR is merged
                    def dbzState = sh(
                        script: "GH_TOKEN=\${GITHUB_PASSWORD} gh pr view $DBZ_PR_URL --json state -q .state",
                        returnStdout: true
                    ).trim()
                    if (dbzState != 'MERGED') {
                        echo "ERROR: DBZ PR $DBZ_PR_URL is not merged (state: $dbzState)"
                        failures << "DBZ PR is not merged (state: $dbzState)"
                    }

                    // Check WEBSITE PR is merged
                    def webState = sh(
                        script: "GH_TOKEN=\${GITHUB_PASSWORD} gh pr view $WEBSITE_PR_URL --json state -q .state",
                        returnStdout: true
                    ).trim()
                    if (webState != 'MERGED') {
                        echo "ERROR: WEBSITE PR $WEBSITE_PR_URL is not merged (state: $webState)"
                        failures << "WEBSITE PR is not merged (state: $webState)"
                    }

                    // Pull latest debezium repo to surface merged content on SOURCE_BRANCH
                    dir(DEBEZIUM_DIR) {
                        sh "git pull https://\${GITHUB_USERNAME}:\${GITHUB_PASSWORD}@github.com/debezium/debezium.git $SOURCE_BRANCH"
                    }

                    // Clone or pull debezium.github.io into WEBSITE_DIR
                    if (!fileExists(WEBSITE_DIR)) {
                        sh "git clone --depth=1 -b develop https://\${GITHUB_USERNAME}:\${GITHUB_PASSWORD}@github.com/debezium/debezium.github.io.git $WEBSITE_DIR"
                    } else {
                        dir(WEBSITE_DIR) {
                            sh "git pull https://\${GITHUB_USERNAME}:\${GITHUB_PASSWORD}@github.com/debezium/debezium.github.io.git develop"
                        }
                    }

                    // Run shared content checks against locally checked-out files
                    def changelogContent     = readFile("$DEBEZIUM_DIR/CHANGELOG.md")
                    def copyrightContent     = readFile("$DEBEZIUM_DIR/COPYRIGHT.txt")
                    def releaseNotesContent  = readFile("$WEBSITE_DIR/releases/$VERSION_MAJOR_MINOR/release-notes.asciidoc")
                    def versionYmlContent    = readFile("$WEBSITE_DIR/_data/releases/$VERSION_MAJOR_MINOR/${RELEASE_VERSION}.yml")

                    checkPreReleaseContent(changelogContent, copyrightContent, releaseNotesContent, versionYmlContent, failures, 'ERROR')
                }

                if (failures) {
                    sendZulipNotification("""\
Release $RELEASE_VERSION changelog checks FAILED:
${failures.collect { "  - $it" }.join('\n')}""")
                    error("Changelog checks failed — see build log for details.")
                }
            }
        }

        stage('Dockerfiles present') {
            def missingImages = []
            for (def image: IMAGES) {
                if (!fileExists("$IMAGES_DIR/$image/$IMAGE_TAG/Dockerfile")) {
                    missingImages << image
                }
            }
            if (missingImages) {
                error "Dockerfile(s) not present for $missingImages tag $IMAGE_TAG"
            }
        }

        stage('Prepare release') {
            MAVEN_REPOSITORIES.each { id, repo ->
                dir("$id/${repo.subDir}") {
                    if (branchExists(CANDIDATE_BRANCH)) {
                        return
                    }

                    echo "Preparing release for $id"
                    sh "git checkout -b $CANDIDATE_BRANCH"

                    // Obtain dependencies not available in Maven Central (introduced for Cassandra Enterprise)
                    if (fileExists(INSTALL_ARTIFACTS_SCRIPT)) {
                        sh "./$INSTALL_ARTIFACTS_SCRIPT"
                    }

                    releasePrepare(id, repo.git)
                }
            }
        }

        stage('Execute smoke test') {
            def shouldSkip = false

            dir(IMAGES_DIR) {
                if (branchExists(CANDIDATE_BRANCH)) {
                    shouldSkip = true
                    return
                }

                echo 'Executing smoke test'
                sh "git checkout -b $CANDIDATE_BRANCH"
            }
            if (shouldSkip) {
                return
            }

            // Smoke test with prepared artifacts
            smokeTestContainerImages()
        }

        stage('Perform release') {
            RELEASE_PLAN.each { tier ->
                // Upload repos in this tier for which release:perform has not yet been executed.
                // release.properties is present before release:perform and removed by Maven on success.
                def unpublished = []
                tier.each { id ->
                    def repo = MAVEN_REPOSITORIES[id]
                    dir("$id/${repo.subDir}") {
                        if (fileExists('release.properties') && !DRY_RUN) {
                            releasePerformUpload(id, repo.git)
                            unpublished << id
                        }
                    }
                }
                // Wait for all uploaded repos in this tier to appear in Maven Central.
                // Poll all remaining unpublished repos each iteration, notify and remove each as it appears.
                // This also gates the next tier from starting before its dependencies are published.
                timeout(time: PUBLISH_TIMEOUT_MINUTES, unit: 'MINUTES') {
                    waitUntil(initialRecurrencePeriod: PUBLISH_POLL_INTERVAL_MS) {
                        unpublished.removeAll { id ->
                            if (!artifactExists(id)) {
                                return false
                            }
                            sendZulipNotification("Published version $RELEASE_VERSION of $id")
                            return true
                        }
                        unpublished.isEmpty()
                    }
                }
                // Run post-perform steps for every repo in this tier unless a development-version
                // commit already exists in the local history, which means the steps completed in a
                // prior run.  Using the git log as the marker is durable across job restarts,
                // unlike the in-memory unpublished list which is rebuilt from scratch each run.
                tier.each { id ->
                    def repo = MAVEN_REPOSITORIES[id]
                    dir("$id/${repo.subDir}") {
                        if (!postPerformCommitExists()) {
                            releasePerformPostSteps(id, repo.git)
                        } else {
                            echo "Post-perform commit already exists for $id, skipping"
                        }
                    }
                }
            }
        }

        stage('Modify Dockerfiles') {
            // Smoke test with published artifacts
            smokeTestContainerImages()

            // Return Dockerfiles to production state
            dir("$IMAGES_DIR/connect/$IMAGE_TAG") {
                fileUtils.modifyFile('Dockerfile') {
                    it
                            .replaceFirst('MAVEN_REPO_CENTRAL="[^"]+"', "MAVEN_REPO_CENTRAL=\"\"")
                            .replaceFirst('MAVEN_REPOS_ADDITIONAL="[^"]+"', "MAVEN_REPOS_ADDITIONAL=\"\"")
                }
                fileUtils.modifyFile('Dockerfile.local') {
                    it
                            .replaceFirst('DEBEZIUM_VERSION=\"\\S+\"', "DEBEZIUM_VERSION=\"$RELEASE_VERSION\"")
                }
            }
            dir("$IMAGES_DIR/server/$IMAGE_TAG") {
                fileUtils.modifyFile('Dockerfile') {
                    it
                            .replaceFirst('MAVEN_REPO_CENTRAL="[^"]*"', "MAVEN_REPO_CENTRAL=\"$MAVEN_CENTRAL\"")
                }
            }
            dir("$IMAGES_DIR/operator/$IMAGE_TAG") {
                fileUtils.modifyFile('Dockerfile') {
                    it
                            .replaceFirst('MAVEN_REPO_CENTRAL="[^"]*"', "MAVEN_REPO_CENTRAL=\"$MAVEN_CENTRAL\"")
                }
            }

            dir("$IMAGES_DIR/platform-conductor/$IMAGE_TAG") {
                fileUtils.modifyFile('Dockerfile') {
                    it
                            .replaceFirst('MAVEN_REPO_CENTRAL="[^"]*"', "MAVEN_REPO_CENTRAL=\"$MAVEN_CENTRAL\"")
                }
            }

            dir("$IMAGES_DIR/platform-stage/$IMAGE_TAG/") {
                fileUtils.modifyFile("Dockerfile") {
                    it.replaceFirst(/COPY $PLATFORM_STAGE_DIR .\/debezium-platform/, "RUN git clone -b \\\$\\{BRANCH\\} https://github.com/debezium/debezium-platform.git")
                }
                sh "rm -rf $PLATFORM_STAGE_DIR"
            }

            echo 'Updating image version'
            dir("$IMAGES_DIR") {
                // Change of major/minor version - need to provide a new image tag for next releases
                if (!DEVELOPMENT_VERSION.startsWith(IMAGE_TAG)) {
                    def version = DEVELOPMENT_VERSION.split('\\.')
                    def nextTag = "${version[0]}.${version[1]}"
                    for (i = 0; i < IMAGES.size(); i++) {
                        def image = IMAGES[i]
                        if (fileExists("$image/$nextTag")) {
                            continue
                        }
                        sh "cp -r $image/$IMAGE_TAG $image/$nextTag && git add $image/$nextTag"
                    }
                    fileUtils.modifyFile('connect/snapshot/Dockerfile') {
                        it.replaceFirst('FROM \\S+', "FROM quay.io/debezium/connect-base:$nextTag")
                    }
                    fileUtils.modifyFile("connect/$nextTag/Dockerfile") {
                        it.replaceFirst('FROM \\S+', "FROM \\\$DEBEZIUM_DOCKER_REGISTRY_PRIMARY_NAME/connect-base:$nextTag")
                    }
                    fileUtils.modifyFile("connect/$nextTag/Dockerfile.local") {
                        it
                                .replaceFirst('FROM \\S+', "FROM quay.io/debezium/connect-base:$nextTag")
                                .replaceFirst('DEBEZIUM_VERSION=\\S+', "DEBEZIUM_VERSION=${DEVELOPMENT_VERSION - '-SNAPSHOT'}")
                    }
                    fileUtils.modifyFile("connect-base/$nextTag/Dockerfile") {
                        it.replaceFirst('FROM \\S+', "FROM \\\$DEBEZIUM_DOCKER_REGISTRY_PRIMARY_NAME/kafka:$nextTag")
                    }

                }
                sh """
                        git commit -a -m "Updated container images for release $RELEASE_VERSION"
                    """
                gitPushCandidate(IMAGES_REPOSITORY)
            }
        }

        stage('Commit changes to repositories') {
            def mergeFailures = []

            dir(IMAGES_DIR) {
                gitPushTag(IMAGES_REPOSITORY)
                try {
                    gitMergeAndDeleteCandidate(IMAGES_REPOSITORY, IMAGES_BRANCH)
                } catch (e) {
                    echo "Failed to merge candidate branch for $IMAGES_REPOSITORY: ${e.message}"
                    mergeFailures << IMAGES_REPOSITORY
                }
            }
            dir(POSTGRES_DECODER_DIR) {
                gitPushTag(POSTGRES_DECODER_REPOSITORY)
            }
            MAVEN_REPOSITORIES.each { id, repo ->
                dir (id) {
                    try {
                        gitMergeAndDeleteCandidate(repo.git, repo.branch)
                    } catch (e) {
                        echo "Failed to merge candidate branch for ${repo.git}: ${e.message}"
                        mergeFailures << repo.git
                    }
                }
            }

            if (mergeFailures) {
                error "Candidate branch merge failed for the following repositories:\n  - ${mergeFailures.join('\n  - ')}"
            }
        }

        stage('Generate descriptors manifest') {
            def debeziumCommit = sh(
                script: "cd ${WORKSPACE}/${DEBEZIUM_DIR} && git rev-parse --short HEAD",
                returnStdout: true
            ).trim()

            def buildTimestamp = new Date().format("yyyy-MM-dd'T'HH:mm:ss'Z'", TimeZone.getTimeZone('UTC'))

            fileUtils.generateDescriptorManifest(DESCRIPTORS_OUTPUT_DIR, debeziumCommit, SOURCE_BRANCH, buildTimestamp, RELEASE_VERSION)
        }

        stage("Publishing descriptors to ${DESCRIPTOR_REPOSITORY}") {
            if (!DRY_RUN) {
                dir(DESCRIPTORS_REPO_DIR) {
                    sh """
                        mkdir -p ${RELEASE_VERSION}
                        cp -r ${DESCRIPTORS_OUTPUT_DIR}/* ${RELEASE_VERSION}/

                        DEBEZIUM_COMMIT=\$(cd ${WORKSPACE}/${DEBEZIUM_DIR} && git rev-parse --short HEAD)
                        BUILD_TIMESTAMP=\$(date -u +"%Y-%m-%dT%H:%M:%SZ")

                        git add ${RELEASE_VERSION}
                        git commit -m '[release] ${RELEASE_VERSION} from debezium/debezium@'\$DEBEZIUM_COMMIT' at '\$BUILD_TIMESTAMP || echo 'No changes to commit'
                    """

                    executeShell('.', "git push \"https://\${GITHUB_USERNAME}:\${GITHUB_PASSWORD}@${DESCRIPTOR_REPOSITORY}\" HEAD:${DESCRIPTOR_BRANCH}")
                }
            }
        }
        stage('Cleanup GitHub Project') {
            if (!DRY_RUN) {
            }
        }
    }

    sendZulipNotification("Build of Debezium $RELEASE_VERSION finished with ${currentBuild.currentResult} ($BUILD_URL)")
}
