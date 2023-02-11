pipelineJob('node-snapshot-build') {
    displayName('Jenkins node snapshot preparation ')
    description('Updates jenkins node snapshot')

    properties {
        githubProjectUrl('https://github.com/debezium/debezium')
    }

    logRotator {
        numToKeep(10)
    }

    parameters {
//      CREDENTIALS
        stringParam('GITLAB_CREDENTIALS', 'gitlab-debeziumci-ssh', 'QE gitlab credentials id')
        stringParam('ANSIBLE_VAULT_PASSWORD', 'ansible-vault-password', 'Password for ansible vault in used ansible playbook')
//      OPENSTACK INSTANCE AND SNAPSHOT CONFIG
        stringParam('CLOUD_NAME', 'openstack', 'Name of openstack cloud')
        stringParam('SNAPSHOT_NAME', 'debezium-jenkins-node-centos8', 'Name of created snapshot')
        stringParam('BASE_IMAGE', 'CentOS-Stream-8-x86_64-GenericCloud', 'Base image for created snapshot')
        stringParam('INSTANCE_NAME', 'Ansible_temporary_instance', 'Name of created instance in Openstack')
        stringParam('INSTANCE_USER', 'centos', 'User used to connect to the instance')
        stringParam('KEYPAIR', 'jenkins', 'Keypair used to connect to the instance')
//      ANSIBLE REPO
        stringParam('ANS_GIT_SECRET', 'jenkins-node-git-repo', 'ID of secret containing repo from which ansible resources are cloned')
        stringParam('ANS_GIT_REPOSITORY', '', 'Repository from which ansible resources are cloned. Use SSH format')
        stringParam('ANS_GIT_BRANCH', 'master', 'A branch/tag of ansible sources')
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/node_snapshot_build_pipeline.groovy'))
            sandbox()
        }
    }
}
