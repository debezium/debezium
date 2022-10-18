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
//      CREDENTIALS
        stringParam('GITLAB_CREDENTIALS', 'gitlab-debeziumci-ssh', 'QE gitlab credentials id')
        stringParam('ANSIBLE_VAULT_PASSWORD', 'ansible-vault-password', 'Password for ansible vault used for secrets encryption')
//      OSIA CONFIGURATION
        stringParam('CLUSTER_NAME', 'cluster', 'Name of OCP cluster')
        stringParam('INSTALLER_VERSION', 'latest-4.10', 'Version of OCP installer')
        booleanParam('REMOVE_CLUSTER', false, 'If true, instead of cluster deployment removes said cluster')
//      OSIA REPO
        stringParam('OSIA_GIT_SECRET', 'osia-git-repo', 'ID of secret containing OSIA configuration repository')
        stringParam('OSIA_GIT_REPOSITORY', '', 'OSIA configuration repository. If empty, repository from previous secret is used. Use SSH format')
        stringParam('OSIA_GIT_BRANCH', 'persistence', 'A branch/tag of OSIA configuration repository')
    }

    definition {
        cps {
            script(readFileFromWorkspace('jenkins-jobs/pipelines/cluster_deployment_pipeline.groovy'))
            sandbox()
        }
    }
}
