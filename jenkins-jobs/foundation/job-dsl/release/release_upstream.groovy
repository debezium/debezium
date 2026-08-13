folder('release') {
    description('This folder contains all jobs used by developers for upstream release and all relevant stuff')
    displayName('Release')
}

def releasePipelineParameters = evaluate(readFileFromWorkspace('jenkins-jobs/foundation/job-dsl/release/parameters/release_upstream_parameters.groovy'))
def commonParameters = evaluate(readFileFromWorkspace('jenkins-jobs/foundation/job-dsl/release/parameters/common_parameters.groovy'))

pipelineJob('release/release-debezium-upstream') {
    displayName('Debezium Release')
    description('Builds Debezium and deploys into Maven Central and Docker Hub')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    logRotator {
        numToKeep(20)
    }

    parameters {
        // Pass the parameters context to the function
        commonParameters(delegate)
        releasePipelineParameters(delegate)
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/foundation/pipelines/release/release-pipeline.groovy'))
            sandbox()
        }
    }
}
