listView('Debezium-Tools') {
    description('Tooling jobs')
    jobs {
        regex(/^tools-.*$/)
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
