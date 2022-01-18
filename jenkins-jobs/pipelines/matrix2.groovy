pipeline {
    agent any
    matrix {
        agent any
        axes {
            axis {
                name 'PLATFORM'
                values 'linux', 'windows'
            }
            axis {
                name 'BROWSER'
                values 'firefox', 'chrome'
            }
        }

        stages {
            stage('Build') {
                steps {
                    echo "Do Build M2 for ${PLATFORM} - ${BROWSER}"
                }
            }
        }
    }
}

