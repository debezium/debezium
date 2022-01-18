pipeline {
    agent {
        label 'Core'
    }

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
            mail to: 'mmedek@redhat.com', subject: "Jenkins Hello World #${BUILD_NUMBER} finished", body: ''
        }
    }
}
