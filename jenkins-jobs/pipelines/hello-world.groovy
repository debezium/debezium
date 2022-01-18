pipeline {
    agent any

    stages {
        stage('Hello') {
            steps {
                echo 'Hello World'
                sh 'echo Hello World'
            }
        }
    }

    post {
        always {
            mail to: 'mmedek@redhat.com', subject: "Jenkins Hello World #${BUILD_NUMBER} finished", body: 'Hello World'
        }
    }
}
