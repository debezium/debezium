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
                            string(name: 'DBZ_EXTRA_LIBS', value: params.AS_DBZ_EXTRA_LIBS ),
                            string(name: 'EXTRA_IMAGE_TAGS', value: params.AS_EXTRA_IMAGE_TAGS),
                            booleanParam(name: 'AUTO_TAG', value: params.AS_AUTO_TAG),
                            string(name: 'DBZ_CONNECTOR_ARCHIVE_URLS', value: params.AS_DBZ_CONNECTOR_ARCHIVE_URLS),
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
                             string(name: 'STRZ_IMAGES', value: params.STRZ_IMAGES),
                             string(name: 'DBZ_GIT_REPOSITORY', value: params.DBZ_GIT_REPOSITORY),
                             string(name: 'DBZ_GIT_BRANCH', value: params.DBZ_GIT_BRANCH),
                             booleanParam(name: 'DBZ_CONNECT_BUILD', value: params.DBZ_CONNECT_BUILD),
                             string(name: 'DBZ_CONNECTOR_ARCHIVE_URLS', value: params.DBZ_CONNECTOR_ARCHIVE_URLS),
                             string(name: 'DBZ_EXTRA_LIBS', value: params.DBZ_EXTRA_LIBS),
                         ]
                         copyArtifacts(projectName: 'ocp-downstream-strimzi-prepare-job', selector: lastCompleted())

                    }
                }

                    stage('Invoke_apicurio') {
                        when {
                            expression { params.EXECUTE_APICURIO }
                        }
                        steps {
                            build job: 'ocp-downstream-apicurio-prepare-job', parameters: [
                                string(name: MAIL_TO, value: params.MAIL_TO),
                                string(name: 'QUAY_CREDENTIALS', value: params.QUAY_CREDENTIALS),
                                string(name: 'QUAY_ORGANISATION', value: params.QUAY_ORGANISATION),
                                string(name: 'APIC_RESOURCES_ARCHIVE_URL', value: params.APIC_RESOURCES_ARCHIVE_URL),
                                string(name: 'APIC_RESOURCES_DEPLOYMENT_DESCRIPTOR', value: params.APIC_RESOURCES_DEPLOYMENT_DESCRIPTOR),
                                string(name: 'APIC_IMAGES', value: params.APIC_IMAGES),
                                string(name: 'DBZ_GIT_REPOSITORY', value: params.DBZ_GIT_REPOSITORY),
                                string(name: 'DBZ_GIT_BRANCH', value: params.DBZ_GIT_BRANCH),
                                booleanParam(name: 'PUSH_IMAGES', value: params.PUSH_IMAGES),
                            ]
                        copyArtifacts(projectName: 'ocp-downstream-apicurio-prepare-job', selector: lastCompleted())
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
                            string(name: 'EXTRA_IMAGE_TAGS', value: params.EXTRA_IMAGE_TAGS),
                            string(name: 'DBZ_CONNECTOR_ARCHIVE_URLS', value: params.DBZ_CONNECTOR_ARCHIVE_URLS),
                            string(name: 'DBZ_EXTRA_LIBS', value: params.DBZ_EXTRA_LIBS),
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
                jobMap.put("ocp-downstream-apicurio-prepare-job", EXECUTE_APICURIO)

                def label = params.LABEL
                def currentBuildLabel = label
                def jobArtifactOutputs = ''
                jobMap.each { entry ->
                    if (!entry.value) {
                        return
                    }

                    def build = jenkins.model.Jenkins.instance.getItem("${entry.key}").lastBuild

                    if (!build) {
                        println "No build of ${entry.key} found!"
                        return
                    }

                    // if no label is set and not a product build, add branch name
                    if (!label) {
                        println "using branch name and build number for label"
                        label = "#${build.number} ${params.DBZ_GIT_BRANCH}"
                        currentBuildLabel = "#${currentBuild.number} ${params.DBZ_GIT_BRANCH}"
                    }

                    // set label
                    build.displayName = label
                }
                if (label) {
                    currentBuild.displayName = currentBuildLabel
                }
               archiveArtifacts "**/*.zip"
            }
        }
    }
}