listView('Debezium-Release-Automation') {
    description('Debezium release jobs')
    jobs {
        regex(/^release-.*$/)
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
