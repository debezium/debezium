folder("release") {
    description("This folder contains all jobs used by developers for upstream release and all relevant stuff")
    displayName("Release")
}

pipelineJob('release/release-orchestrator') {
    displayName('Debezium Release Orchestrator')
    description('Orchestrator pipeline that executes release pipelines')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    logRotator {
        numToKeep(5)
    }

    // Parameters that can be modified when running the orchestrator
    parameters {

        // Specific parameters for controlling the orchestration
        booleanParam('SKIP_PIPELINE_RELEASE_UPSTREAM', false, 'Skip the execution of Debezium Release pipeline')
        booleanParam('SKIP_PIPELINE_CONTAINER_IMAGES', false, 'Skip the execution of Deploy Container Images pipeline')
        booleanParam('SKIP_PIPELINE_RELEASE_CHARTS', false, 'Skip the execution of Debezium Charts Release pipeline')


        stringParam('MAIL_TO', 'jpechane@redhat.com')
        stringParam('RELEASE_VERSION', 'x.y.z.Final', 'Version of Debezium to be released - e.g. 0.5.2.Final')
        stringParam('DEVELOPMENT_VERSION', 'x.y.z-SNAPSHOT', 'Next development version - e.g. 0.5.3-SNAPSHOT')
        booleanParam('DRY_RUN', true, 'When checked the changes and artifacts are not pushed to repositories and registries')
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/release/release-orchestrator-pipeline.groovy'))
            sandbox()  // Enable script sandbox mode
        }
    }
}