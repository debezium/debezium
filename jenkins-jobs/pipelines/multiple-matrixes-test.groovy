pipeline {
    agent none
    stages {
        stage('Execute') {
            parallel {

                stage('DB type 1') {
                    matrix {
                        agent any
                        axes {
                            axis {
                                name 'VERSION'
                                values '1.0', '2.0'
                            }
                        }
                        stages {
                            stage('Build DB 1') {
                                steps {
                                    echo "Do DB type 1 for ${VERSION}"
                                }
                            }
                        }
                    }
                }

                stage('DB type 2') {
                    matrix {
                        agent any
                        axes {
                            axis {
                                name 'VERSION'
                                values '23.0', '23.1'
                            }
                        }
                        stages {
                            stage('Build DB 2') {
                                steps {
                                    echo "Do DB type 2 for ${VERSION}"
                                }
                            }
                        }
                    }
                }

            }
        }
    }
}