// Job definition to test PostgreSQL connector against different PostgreSQL versions

matrixJob('connector-debezium-postgresql-matrix-test') {

    displayName('Debezium PostgreSQL Connector Test Matrix')
    description('Executes tests for PostgreSQL Connector with PostgreSQL matrix')
    label('Slave')
    combinationFilter('''
         DECODER_PLUGIN == "decoderbufs" ||
        (DECODER_PLUGIN == "pgoutput" && (POSTGRES_VERSION == "10" || POSTGRES_VERSION == "11")) ||
         POSTGRES_VERSION == "12" ||
         POSTGRES_VERSION == "13" ||
         POSTGRES_VERSION == "14"
         ''')

    axes {
        text('POSTGRES_VERSION', '10', '11', '12', '13', '14')
        text('DECODER_PLUGIN', 'decoderbufs', 'wal2json', 'wal2json_streaming', 'pgoutput')
        label("Node", "Slave")
    }

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    parameters {
        stringParam('REPOSITORY', 'https://github.com/debezium/debezium', 'Repository from which Debezium is built')
        stringParam('BRANCH', 'main', 'A branch/tag from which Debezium is built')
        stringParam('SOURCE_URL', "", "URL to productised sources")
        booleanParam('PRODUCT_BUILD', false, 'Is this a productised build?')
    }

    triggers {
        cron('H 04 * * 1-5')
    }

    wrappers {
        preBuildCleanup()

        timeout {
            noActivity(3600)
        }
    }

    publishers {
        archiveJunit('**/target/surefire-reports/*.xml')
        archiveJunit('**/target/failsafe-reports/*.xml')
        mailer('jpechane@redhat.com', false, true)
    }

    logRotator {
        daysToKeep(7)
    }

    steps {
        shell('''
# Ensure WS cleaup
ls -A1 | xargs rm -rf

# Retrieve sources
if [ "$PRODUCT_BUILD" == true ] ; then
    export MAVEN_OPTS="-Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true -Dmaven.wagon.http.ssl.ignore.validity.dates=true"
    PROFILE_PROD="-Ppnc"
    curl -OJs $SOURCE_URL && unzip debezium-*-src.zip
    pushd debezium-*-src
    pushd $(ls | grep -P 'debezium-[^-]+.Final')
else
    git clone $REPOSITORY . 
    git checkout $BRANCH
fi

# Setup pg config for Alpine distributions
if [[ $POSTGRES_VERSION =~ alpine$ ]] ; then
    MAVEN_ARGS = "-Dpostgres.config.file=/usr/local/share/postgresql/postgresql.conf.sample"
else
    MAVEN_ARGS="-Dnone"
fi
                               
# Run maven build
mvn clean install -U -s $HOME/.m2/settings-snapshots.xml -pl debezium-connector-postgres -am -fae \
    -Dmaven.test.failure.ignore=true \
    -Dpostgres.port=55432 \
    -Dversion.postgres.server=$POSTGRES_VERSION \
    -Ddecoder.plugin.name=$DECODER_PLUGIN \
    -Dtest.argline="-Ddebezium.test.records.waittime=5" \
    -Dinsecure.repositories=WARN \
    $PROFILE_PROD \
    $MAVEN_ARGS
''')
    }
}
