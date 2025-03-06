folder("release") {
    description("This folder contains all jobs used by developers for upstream release and all relevant stuff")
    displayName("Release")
}

def releasePipelineParameters = evaluate(readFileFromWorkspace('jenkins-jobs/job-dsl/release/parameters/release_upstream_parameters.groovy'))
def commonParameters = evaluate(readFileFromWorkspace('jenkins-jobs/job-dsl/release/parameters/common_parameters.groovy'))

pipelineJob('release/release-debezium-upstream') {
    displayName('Debezium Release')
    description('Builds Debezium and deploys into Maven Central and Docker Hub')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    logRotator {
        numToKeep(5)
    }

    parameters {
        stringParam('MAIL_TO', 'jpechane@redhat.com')
        booleanParam('DRY_RUN', true, 'When checked the changes and artifacts are not pushed to repositories and registries')
        stringParam('RELEASE_VERSION', 'x.y.z.Final', 'Version of Debezium to be released - e.g. 0.5.2.Final')

        // Pass the parameters context to the function
        commonParameters(delegate)
        releasePipelineParameters(delegate)
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/release/release-pipeline.groovy'))
        }
    }
}
