listView('Debezium-System-Tests') {
    description('OpenShift certification jobs')
    jobs {
        regex(/^ocp-.*$|^rhel-.*$|^system-.*$|^prepare-.*$/)
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
