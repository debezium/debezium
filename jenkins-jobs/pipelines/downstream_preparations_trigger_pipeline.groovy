pipeline {
    agent {
        label 'Slave'
    }
    stages {
        stage('Start') {
            parallel {
                stage('Invoke_downstream_as') {
                    when {
                        expression { params.EXECUTE_AS }
                    }
                    steps {
                        build job: 'ocp-downstream-artifact-server-prepare-job', parameters: [
                                string(name: MAIL_TO, value: params.MAIL_TO),
                                string(name: 'QUAY_CREDENTIALS', value: params.QUAY_CREDENTIALS),
                                string(name: 'QUAY_ORGANISATION', value: params.QUAY_ORGANISATION),
                                string(name: 'DBZ_GIT_REPOSITORY', value: params.DBZ_GIT_REPOSITORY),
                                string(name: 'DBZ_GIT_BRANCH', value: params.DBZ_GIT_BRANCH),
                                text(name: 'DBZ_EXTRA_LIBS', value: params.AS_DBZ_EXTRA_LIBS),
                                text(name: 'EXTRA_IMAGE_TAGS', value: params.AS_EXTRA_IMAGE_TAGS),
                                booleanParam(name: 'AUTO_TAG', value: params.AUTO_TAG),
                                text(name: 'DBZ_CONNECTOR_ARCHIVE_URLS', value: params.DBZ_CONNECTOR_ARCHIVE_URLS),
                        ]
                        copyArtifacts(projectName: 'ocp-downstream-artifact-server-prepare-job', selector: lastCompleted())
                    }
                }

                stage('Invoke_downstream_strimzi') {
                    when {
                        expression { params.EXECUTE_STRIMZI }
                    }
                    steps {
                        build job: 'ocp-downstream-strimzi-prepare-job', parameters: [
                                string(name: MAIL_TO, value: params.MAIL_TO),
                                string(name: 'QUAY_CREDENTIALS', value: params.QUAY_CREDENTIALS),
                                string(name: 'QUAY_ORGANISATION', value: params.QUAY_ORGANISATION),
                                string(name: 'STRZ_RESOURCES_ARCHIVE_URL', value: params.STRZ_RESOURCES_ARCHIVE_URL),
                                string(name: 'STRZ_RESOURCES_DEPLOYMENT_DESCRIPTOR', value: params.STRZ_RESOURCES_DEPLOYMENT_DESCRIPTOR),
                                text(name: 'STRZ_IMAGES', value: params.STRZ_IMAGES),
                                string(name: 'DBZ_GIT_REPOSITORY', value: params.DBZ_GIT_REPOSITORY),
                                string(name: 'DBZ_GIT_BRANCH', value: params.DBZ_GIT_BRANCH),
                                booleanParam(name: 'DBZ_CONNECT_BUILD', value: params.STRZ_DBZ_CONNECT_BUILD),
                                text(name: 'DBZ_CONNECTOR_ARCHIVE_URLS', value: params.DBZ_CONNECTOR_ARCHIVE_URLS),
                                text(name: 'DBZ_EXTRA_LIBS', value: params.STRZ_DBZ_EXTRA_LIBS),
                        ]
                        copyArtifacts(projectName: 'ocp-downstream-strimzi-prepare-job', selector: lastCompleted())

                    }
                }

                stage('Invoke_rhel') {
                    when {
                        expression { params.EXECUTE_RHEL }
                    }
                    steps {
                        build job: 'rhel-downstream-prepare-job', parameters: [
                                string(name: MAIL_TO, value: params.MAIL_TO),
                                string(name: 'QUAY_CREDENTIALS', value: params.QUAY_CREDENTIALS),
                                string(name: 'QUAY_ORGANISATION', value: params.QUAY_ORGANISATION),
                                string(name: 'RHEL_IMAGE', value: params.RHEL_IMAGE),
                                string(name: 'KAFKA_URL', value: params.KAFKA_URL),
                                string(name: 'DBZ_GIT_REPOSITORY', value: params.DBZ_GIT_REPOSITORY),
                                string(name: 'DBZ_GIT_BRANCH', value: params.DBZ_GIT_BRANCH),
                                booleanParam(name: 'AUTO_TAG', value: params.AUTO_TAG),
                                text(name: 'EXTRA_IMAGE_TAGS', value: params.EXTRA_IMAGE_TAGS),
                                text(name: 'DBZ_CONNECTOR_ARCHIVE_URLS', value: params.DBZ_CONNECTOR_ARCHIVE_URLS),
                                text(name: 'DBZ_EXTRA_LIBS', value: params.STRZ_DBZ_EXTRA_LIBS),
                        ]
                        copyArtifacts(projectName: 'rhel-downstream-prepare-job', selector: lastCompleted())
                    }
                }
            }
        }
    }

    post {
        always {
            script {
                def jobMap = [:] as TreeMap
                jobMap.put("ocp-downstream-artifact-server-prepare-job", params.EXECUTE_AS)
                jobMap.put("ocp-downstream-strimzi-prepare-job", params.EXECUTE_STRIMZI)
                jobMap.put("rhel-downstream-prepare-job", params.EXECUTE_RHEL)

                jobMap.each { entry ->
                    if (!entry.value) {
                        return
                    }

                    def build = jenkins.model.Jenkins.get().getItem("${entry.key}").getLastCompletedBuild()

                    if (!build) {
                        println "No build of ${entry.key} found!"
                        return
                    }

                    // if no label is set, add branch name
                    def label = "#${build.number} parent: #${currentBuild.number}"
                    if (params.LABEL) {
                        label += " ${params.LABEL}"
                    }

                    // set label
                    build.displayName = "${label}"
                }

                // set parent job label
                if (params.LABEL) {
                    currentBuild.displayName = "#${currentBuild.number} ${params.LABEL}"
                } else {
                    // if no label is set, add branch name
                    currentBuild.displayName = "#${currentBuild.number}"
                }

                archiveArtifacts "**/*.zip"
            }
        }
    }
}
