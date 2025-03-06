folder("release") {
    description("This folder contains all jobs used by developers for upstream release and all relevant stuff")
    displayName("Release")
}

def releaseChartsPipelineParameters = evaluate(readFileFromWorkspace('jenkins-jobs/job-dsl/release/parameters/release_charts_upstream_parameters.groovy'))

pipelineJob('release/release-debezium-charts-upstream') {
    displayName('Debezium Charts Release')
    description('Packages helm charts push into Quay.io and create Github release')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    logRotator {
        numToKeep(5)
    }

    parameters {

        stringParam('MAIL_TO', 'mvitale@redhat.com')
        booleanParam('DRY_RUN', true, 'When checked the changes and artifacts are not pushed to repositories and registries')
        stringParam('RELEASE_VERSION', 'x.y.z.Final', 'Version of Debezium to be released - e.g. 0.5.2.Final')

        // Pass the parameters context to the function
        releaseChartsPipelineParameters(delegate)
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/release/release-charts-pipeline.groovy'))
        }
    }
}
