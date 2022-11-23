pipelineJob('ocp-aro-teardown') {
    displayName('OCP ARO teardown')
    description('Tears downs Debezium OCP ARO on Azure cloud')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    logRotator {
        numToKeep 10
    }

    parameters {
        stringParam('CLUSTER_NAME', "cluster", "Name of the ARO cluster")
        stringParam('RESOURCE_GROUP', "debezium-ARO", 'name of ARO resource group')
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/aro_teardown_pipeline.groovy'))
            sandbox()
        }
    }
}
