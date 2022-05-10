listView('Jenkins') {
    description('Jenkins tools')
    jobs {
        name('job-configurator')
        name('MonitoringTest')
        name('node-snapshot-build')
        name('node-snapshot-history-cleanup')
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
