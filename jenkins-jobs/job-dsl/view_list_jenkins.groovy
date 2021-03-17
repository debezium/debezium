listView('Jenkins') {
    description('Jenkins tools')
    jobs {
        name('job-configurator')
        name('MonitoringTest')
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
