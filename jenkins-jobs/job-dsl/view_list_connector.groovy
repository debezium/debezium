listView('Debezium-Connector-Tests') {
    description('Upstream connector jobs')
    jobs {
        regex(/^connector-.*$/)
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
