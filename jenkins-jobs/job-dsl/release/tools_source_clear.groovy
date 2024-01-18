// Job definition to test PostgreSQL connector against different PostgreSQL versions
folder("release") {
    description("This folder contains all jobs used by developers for upstream release and all relevant stuff")
    displayName("Release")
}

freeStyleJob('release/tools-debezium-source-clear') {

    displayName('Vulnerability scan')
    description('Executes SourceClear vulnerability scan of Debezium sources and binaries')
    label('Slave')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    logRotator {
        daysToKeep(7)
        numToKeep(10)
    }

    wrappers {
        timeout {
            noActivity(900)
        }

        credentialsBinding {
            string('SRCCLR_TOKEN', 'debezium-srcclr-token')
            string('SOURCE_MAVEN_REPO', 'debezium-prod-repo')
        }
    }

    publishers {
        mailer('jpechane@redhat.com', false, true)
    }

    parameters {
        stringParam('BUILD_VERSION', '', "Maven artifact id of the product binaries")
        stringParam('PRODUCT_VERSION', '', "Product version")
        stringParam('SOURCE_TAG', "", "Tagged version of source code to scan")
        booleanParam('BINARY', false, "Scan binary artifacts")
        stringParam('PLUGINS', "mysql postgres mongodb sqlserver oracle db2 vitess", "The plugins whose binaries should be scanned")
    }

    steps {
        shell('''
if [ "$BINARY" = "false" ]; then
    docker run -e SRCCLR_TOKEN="$SRCCLR_TOKEN" quay.io/debezium/vulnerability-scan scm https://github.com/debezium/debezium.git "$PRODUCT_VERSION" "$SOURCE_TAG"
else
    for CONNECTOR in $PLUGINS; do
        docker run -e SRCCLR_TOKEN="$SRCCLR_TOKEN" quay.io/debezium/vulnerability-scan binary "$SOURCE_MAVEN_REPO/debezium-connector-$CONNECTOR/$BUILD_VERSION/debezium-connector-$CONNECTOR-${BUILD_VERSION}-plugin.zip" "$PRODUCT_VERSION" "$SOURCE_TAG"
    done
fi
''')
    }
}
