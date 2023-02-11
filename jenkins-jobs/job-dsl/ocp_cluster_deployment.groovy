pipelineJob('ocp-cluster-deployment') {
    displayName('OCP on AWS deployment')
    description('Deploys/Removes Debezium OCP on AWS EC2 cloud using OSIA')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    logRotator {
        numToKeep(10)
    }

    parameters {
        stringParam('CLUSTER_NAME', 'cluster', 'Name of OCP cluster')
        stringParam('INSTALLER_VERSION', 'latest-4.10', 'Version of OCP installer')
        stringParam('CLOUD', 'openstack', 'Cloud')
        booleanParam('REMOVE_CLUSTER', false, 'If true, instead of cluster deployment removes said cluster')
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/cluster_deployment_pipeline.groovy'))
            sandbox()
        }
    }
}
