import groovy.json.*
import java.util.*
import com.cloudbees.groovy.cps.NonCPS

IMAGES_DIR = 'images'
TAG_REST_ENDPOINT = "https://api.github.com/repos/${params.DEBEZIUM_REPOSITORY}/tags"
STREAMS_TO_BUILD_COUNT = params.STREAMS_TO_BUILD_COUNT.toInteger()
TAGS_PER_STREAM_COUNT = params.TAGS_PER_STREAM_COUNT.toInteger()
GIT_CREDENTIALS_ID = 'debezium-github'
DOCKER_CREDENTIALS_ID = 'debezium-dockerhub'
QUAYIO_CREDENTIALS_ID = 'debezium-quay'

class Version implements Comparable {
    int major
    int minor
    int micro
    String classifier

    def Version(major, minor) {
        this.major = major
        this.minor = minor
        this.micro = -1
    }

    Version(name) {
        def parts = name.split('\\.')
        major = Integer.parseInt(parts[0])
        minor = Integer.parseInt(parts[1])
        micro = Integer.parseInt(parts[2])
        classifier = parts[3]
    }

    @NonCPS
    def majorMinor() {
        new Version(major, minor)
    }

    @Override
    @NonCPS
    def String toString() {
        def sb = new StringBuilder()
        if (major != -1) {
            sb.append(major)
        }
        if (minor != -1) {
            sb.append('.')
            sb.append(minor)
        }
        if (micro != -1) {
            sb.append('.')
            sb.append(micro)
        }
        if (classifier != null) {
            sb.append('.')
            sb.append(classifier)
        }
        sb.toString()
    }

    @Override
    @NonCPS
    boolean equals(o) {
        (o == null) ? false : toString() == o.toString()
    }

    @Override
    @NonCPS
    int hashCode() {
        toString().hashCode()
    }

    @Override
    @NonCPS
    int compareTo(o) {
        int d = major - o.major
        if (d != 0) {
            return d
        }
        d = minor - o.minor
        if (d != 0) {
            return d
        }
        d = micro - o.micro
        if (d != 0) {
            return d
        }
        if (classifier != null) {
            return classifier.compareTo(o.classifier)
        }
        return 0
    }
}

// Map keyed by stream - e.g 1.5 and containing a list of tags per-stream
versions = [:] as TreeMap

// The most recent streams to be built
streamsToBuild = null

// The most resent stable stream representing latest tag
stableStream = null

@NonCPS
def initVersions(username, password) {
    TAG_REST_ENDPOINT.toURL().openConnection().with {
        doOutput = true
        setRequestProperty('Content-Type', 'application/json')
        setRequestProperty('Authorization', 'Basic ' + Base64.encoder.encodeToString("$username:$password".getBytes(java.nio.charset.StandardCharsets.UTF_8)))
        def json = new JsonSlurper().parse(new StringReader(content.text))
        json.each {
            def current = new Version(it.name.substring(1))
            def stream = current.majorMinor()
            versionList = versions[stream]
            if (versionList == null) {
                versionList = []
                versions[stream] = versionList
            }
            versionList.add(current)
        }
    }

    echo "Versions: $versions"
    // Order the tags per stream form the most recent one
    versions.each { k, v ->
        Collections.sort(v)
        Collections.reverse(v)
    }

    // Find the most recent streams and the latest stable
    streamsToBuild = [versions.lastKey()]
    for (int i = 1; i < STREAMS_TO_BUILD_COUNT; i++) {
        streamsToBuild.add(versions.lowerKey(streamsToBuild[-1]))
    }
    stableStream = streamsToBuild.find {
        def tags = versions[it]
        tags.size() > 0 && tags[0].classifier == 'Final'
    }
}

node('Slave') {
    catchError {
        stage('Initialize') {
            dir('.') {
                deleteDir()
            }
            checkout([$class                           : 'GitSCM',
                      branches                         : [[name: params.IMAGES_BRANCH]],
                      doGenerateSubmoduleConfigurations: false,
                      extensions                       : [[$class: 'RelativeTargetDirectory', relativeTargetDir: IMAGES_DIR]],
                      submoduleCfg                     : [],
                      userRemoteConfigs                : [[url: "https://$params.IMAGES_REPOSITORY", credentialsId: GIT_CREDENTIALS_ID]]
            ]
            )
            withCredentials([usernamePassword(credentialsId: GIT_CREDENTIALS_ID, passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME')]) {
                initVersions(GIT_USERNAME, GIT_PASSWORD)
            }
            withCredentials([usernamePassword(credentialsId: DOCKER_CREDENTIALS_ID, passwordVariable: 'DOCKER_PASSWORD', usernameVariable: 'DOCKER_USERNAME')]) {
                sh """
                    docker login -u $DOCKER_USERNAME -p $DOCKER_PASSWORD
                """
            }
            withCredentials([string(credentialsId: QUAYIO_CREDENTIALS_ID, variable: 'USERNAME_PASSWORD')]) {
                def credentials = USERNAME_PASSWORD.split(':')
                sh """
                    set +x
                    docker login -u ${credentials[0]} -p ${credentials[1]} quay.io
                """
            }
            dir(IMAGES_DIR) {
                sh """
                ./setup-local-builder.sh
                docker run --privileged --rm tonistiigi/binfmt --install all
                """
            }
            sh ""
        }
        stage('master') {
            echo "Building images for streams $streamsToBuild"
            dir(IMAGES_DIR) {
                sh """
                DEBEZIUM_VERSIONS=\"${streamsToBuild.join(' ')}\" LATEST_STREAM=\"$stableStream\" ./build-all-multiplatform.sh
                """
            }
        }
        for (stream in streamsToBuild) {
            for (tag in versions[stream].take(TAGS_PER_STREAM_COUNT)) {
                echo "Building images for tag $tag"
                stage(tag.toString()) {
                    dir(IMAGES_DIR) {
                        // Disable IPv6 as Node.js has problems downloading dependencies using it
                        sh """
                            sudo sysctl -w net.ipv6.conf.all.disable_ipv6=1
                            sudo sysctl -w net.ipv6.conf.default.disable_ipv6=1
                        """
                        sh """
                            git checkout v$tag
                            git fetch origin $IMAGES_BRANCH:$IMAGES_BRANCH
                            git checkout $IMAGES_BRANCH build-all-multiplatform.sh build-debezium-multiplatform.sh build-mongo-multiplatform.sh build-postgres-multiplatform.sh
                            echo '========== Building UI only for linux/amd64, arm64 not working =========='
                            DEBEZIUM_UI_PLATFORM=linux/amd64 RELEASE_TAG=$tag ./build-debezium-multiplatform.sh $stream $MULTIPLATFORM_PLATFORMS
                            git reset --hard
                        """
                    }
                }
            }
        }
    }

    mail to: params.MAIL_TO, subject: "${env.JOB_NAME} run #${env.BUILD_NUMBER} finished with ${currentBuild.currentResult}", body: "Run ${env.BUILD_URL} finished with result: ${currentBuild.currentResult}"
}
