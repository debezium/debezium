listView('Debezium-OpenShift-Tests') {
    description('Upstream connector jobs')
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
