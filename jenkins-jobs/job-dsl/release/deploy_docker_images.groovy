folder("release") {
    description("This folder contains all jobs used by developers for upstream release and all relevant stuff")
    displayName("Release")
}

def containerImagePipelineParameters = evaluate(readFileFromWorkspace('jenkins-jobs/job-dsl/release/parameters/deploy_docker_images_parameters.groovy'))
def commonParameters = evaluate(readFileFromWorkspace('jenkins-jobs/job-dsl/release/parameters/common_parameters.groovy'))

pipelineJob('release/release-deploy-container-images') {
    displayName('Debezium Deploy Container Images')
    description('Build and deploy Container images to the registry')

    properties {
        githubProjectUrl('https://github.com/debezium/container-images')
    }

    logRotator {
        daysToKeep(7)
        numToKeep(10)
    }

    triggers {
        cron('@midnight')
    }

    parameters {
        stringParam('MAIL_TO', 'jpechane@redhat.com')
        booleanParam('DRY_RUN', false, 'When checked the changes and artifacts are not pushed to repositories and registries')

        // Pass the parameters context to the function
        commonParameters(delegate)
        containerImagePipelineParameters(delegate)
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/release/build_debezium_images_pipeline.groovy'))
        }
    }
}
