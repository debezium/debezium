pipelineJob('test-job') {
    displayName('Test Job')
    description('Test Job desc')

    logRotator {
        daysToKeep(7)
        numToKeep(10)
    }

    parameters {
        stringParam('SAMPLE_PARAM', '', 'sample parameter')
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/hello-world.groovy'))
        }
    }
}
