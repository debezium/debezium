pipelineJob('node-snapshot-history-cleanup') {
    displayName('Jenkins node snapshot history cleanup')
    description('Delete old snapshots from openstack')

    logRotator {
        numToKeep(10)
    }

    parameters {
        stringParam('DBZ_GIT_REPOSITORY', 'https://github.com/debezium/debezium', 'Debezium repository where the ansible script is located')
        stringParam('DBZ_GIT_BRANCH', '*/main', 'A branch/tag where the ansible script is located')
        stringParam('SNAPSHOT_NAME', 'debezium-jenkins-node-centos8', 'Name of the snapshot')
        stringParam('OPENSTACK_AUTH', 'psi-clouds-yaml', 'yaml with openstack authentication')
        stringParam('CLOUD_NAME', 'openstack', 'Name of openstack cloud')
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/node_snapshot_history_cleanup_pipeline.groovy'))
            sandbox()
        }
    }
}