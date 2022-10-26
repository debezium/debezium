pipelineJob('ocp-aro-deployment') {
    displayName('OCP ARO deployment')
    description('Deploys Debezium OCP ARO on Azure cloud')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    logRotator {
        numToKeep 10
    }

    parameters {
        //cluster config
        stringParam('CLUSTER_NAME', "cluster", "Name of the ARO cluster")
        stringParam('DOMAIN', "dbz", "Domain for ARO cluster installation")
        stringParam('RESOURCE_GROUP', "debezium-ARO", 'name of ARO resource group')
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/aro_deployment_pipeline.groovy'))
            sandbox()
        }
    }
}
