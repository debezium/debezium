pipeline {
    // job1
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
                    echo "Do Build M1 for ${PLATFORM} - ${BROWSER}"
                }
            }
        }
    }

    // job2
    matrix {
        agent any
        axes {
            axis {
                name 'PLATFORM'
                values 'alinux', 'bwindows'
            }
            axis {
                name 'VERSION'
                values 'V1', 'V2'
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

