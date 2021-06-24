listView('Debezium-OpenShift-Tests') {
    description('OpenShift certification jobs')
    jobs {
        regex(/^ocp-.*$/)
    }
    columns {
        status()
        weather()
        name()
        lastSuccess()
        lastFailure()
        lastDuration()
        buildButton()
    }
}
