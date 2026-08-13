import groovy.transform.Field

@Field def upstreamPipelineBuildResult = null
@Field def imagesPipelineBuildResult = null
@Field def chartsPipelineBuildResult = null
@Field def upstreamPipelineStatus = 'NOT_BUILT'
@Field def imagesPipelineStatus = 'NOT_BUILT'
@Field def chartsPipelineStatus = 'NOT_BUILT'

node('release-node') {
    stage('Pipeline Orchestration') {
        try {
            // Define pipeline job names
            def upstreamPipeline = 'release/release-debezium-upstream'
            def imagesPipeline = 'release/release-deploy-container-images'
            def chartsPipeline = 'release-debezium-charts-upstream'

            // Parameters to pass to the pipelines
            def pipelineParams = convertParamsToList(params)


            stage('Execute Debezium Release Pipeline ') {
                if (params.SKIP_PIPELINE_RELEASE_UPSTREAM) {
                    echo "Skipping Debezium Release pipeline as requested"
                    upstreamPipelineStatus = 'SKIPPED'
                } else {
                    echo "Starting execution of ${upstreamPipeline}"
                    try {
                        upstreamPipelineBuildResult = build job: upstreamPipeline,
                                parameters: pipelineParams,
                                wait: true  // Wait for completion before proceeding

                        upstreamPipelineStatus = upstreamPipelineBuildResult.result
                        if (upstreamPipelineStatus != 'SUCCESS') {
                            error "Pipeline Debezium Release failed with result: ${upstreamPipelineStatus}"
                        }
                    } catch (Exception e) {
                        upstreamPipelineStatus = 'FAILURE'
                        throw e
                    }
                }

            }


            stage('Execute Debezium Deploy Container Images Pipeline') {
                if (params.SKIP_PIPELINE_CONTAINER_IMAGES) {
                    echo "Skipping Debezium Deploy Container Images Pipeline as requested"
                    imagesPipelineStatus = 'SKIPPED'
                } else {
                    echo "Starting execution of ${imagesPipeline}"
                    try {
                        imagesPipelineBuildResult = build job: imagesPipeline,
                                parameters: pipelineParams,
                                wait: true

                        imagesPipelineStatus = imagesPipelineBuildResult.result
                        if (imagesPipelineStatus != 'SUCCESS') {
                            error "Debezium Deploy Container Images Pipeline failed with result: ${imagesPipelineStatus}"
                        }
                    } catch (Exception e) {
                        imagesPipelineStatus = 'FAILURE'
                        throw e
                    }
                }
            }


            stage('Execute Debezium Charts Release Pipeline') {
                if (params.SKIP_PIPELINE_RELEASE_CHARTS) {
                    echo "Skipping Debezium Charts Release Pipeline as requested"
                    chartsPipelineStatus = 'SKIPPED'
                } else {
                    echo "Starting execution of ${chartsPipeline}"
                    try {
                        chartsPipelineBuildResult = build job: chartsPipeline,
                                parameters: pipelineParams,
                                wait: true

                        chartsPipelineStatus = chartsPipelineBuildResult.result
                        if (chartsPipelineStatus != 'SUCCESS') {
                            error "Debezium Charts Release Pipeline failed with result: ${chartsPipelineStatus}"
                        }
                    } catch (Exception e) {
                        chartsPipelineStatus = 'FAILURE'
                        throw e
                    }
                }
            }

        } catch (Exception e) {
            currentBuild.result = 'FAILURE'
            echo "Pipeline orchestration failed: ${e.getMessage()}"
            throw e
        } finally {
            // Create summary of all pipeline executions
            createExecutionSummary()
        }
    }
}

def createExecutionSummary() {
    def summary = """
        Pipeline Orchestration Summary
        =============================
        Build Number: ${env.BUILD_NUMBER}
        Status: ${currentBuild.result ?: 'SUCCESS'}
        
        Pipeline 1: ${upstreamPipelineStatus}
        Pipeline 2: ${imagesPipelineStatus}
        Pipeline 3: ${chartsPipelineStatus}
        
        Total Duration: ${currentBuild.durationString}

        Check it ${BUILD_URL}
    """.stripIndent()

    notifyOrchestrationComplete(currentBuild.result ?: 'SUCCESS', summary)
}

def notifyOrchestrationComplete(String result, String summary) {
    def color = result == 'SUCCESS' ? '#00FF00' : '#FF0000'

    mail to: MAIL_TO, subject: "${JOB_NAME} run #${BUILD_NUMBER} finished with ${result}", body: "${summary}"
}


def convertParamsToList(paramsMap) {
    def parametersList = []


    paramsMap.each { paramName, paramValue ->

        if (paramValue instanceof String) {
            parametersList.add(string(name: paramName, value: paramValue))
        } else if (paramValue instanceof Boolean) {
            parametersList.add(booleanParam(name: paramName, value: paramValue))
        } else {
            parametersList.add(string(name: paramName, value: paramValue.toString()))
        }
    }

    return parametersList
}