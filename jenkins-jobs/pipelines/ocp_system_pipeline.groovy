pipeline {
    agent {
        label 'Slave'
    }

    stages {
        stage('Prepare project') {
            steps {
                withCredentials([
                        usernamePassword(credentialsId: "${OCP_CREDENTIALS}", usernameVariable: 'OCP_USERNAME', passwordVariable: 'OCP_PASSWORD'),
                        file(credentialsId: "${PULL_SECRET}", variable: 'SECRET_PATH'),
                ]) {
                    oc login -u ${OCP_USERNAME} -p ${OCP_PASSWORD}
                    oc new-project debezium-test-parent
                    oc adm policy add-cluster-role-to-user cluster-admin system:serviceaccount:debezium-test-parent:default
                    oc create -f ${SECRET_PATH}
                }
            }
        }

        stage('Run tests') {
            steps {
                withCredentials([
                        file(credentialsId: "${PULL_SECRET}", variable: 'SECRET_PATH'),
                ]) {
                    sh '''
                    debezium/jenkins-jobs/docker/debezium-testing-system/deployment_templater.sh --pull-secret-name '${PULL_SECRET}' \
                    --docker-tag "docker-test" \
                    --project-name debezium-test \
                    --product-build false \
                    --strimzi-kc-build true \
                    --dbz-connect-image "quay.io/rh_integration/test-strimzi-kafka:strz-latest-kafka-3.1.0-apc-2.2.3.Final-dbz-2.0.0-SNAPSHOT" \
                    --artifact-server-image "quay.io/rh_integration/test-artifact-server:2.0.0-SNAPSHOT" \
                    --apicurio-version "2.2.3.Final" \
                    --groups-arg 'postgresql & !docker'
                    '''
                    oc create -f test-job-deployment.yml
                }
            }
        }

    }

}