folder('release') {
    description('This folder contains all jobs used by developers for upstream release and all relevant stuff')
    displayName('Release')
}

def releaseChartsPipelineParameters = evaluate(readFileFromWorkspace('jenkins-jobs/foundation/job-dsl/release/parameters/release_charts_upstream_parameters.groovy'))

pipelineJob('release/release-debezium-charts-upstream') {
    displayName('Debezium Charts Release')
    description('Packages helm charts push into Quay.io and create Github release')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    logRotator {
        numToKeep(20)
    }

    parameters {
        // Pass the parameters context to the function
        releaseChartsPipelineParameters(delegate)
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/foundation/pipelines/release/release-charts-pipeline.groovy'))
            sandbox()
        }
    }
}
