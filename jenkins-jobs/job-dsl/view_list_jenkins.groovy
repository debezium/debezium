listView('Jenkins') {
    description('Jenkins tools')
    jobs {
        name('job-configurator')
        name('MonitoringTest')
        name('node-snapshot-build')
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
