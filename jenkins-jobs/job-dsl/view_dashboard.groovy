dashboardView('Dashboard') {
    description('Debezium job dashboard')
    jobs {
        regex(/^connector-.*$/)
        name('ocp-debezium-openshift-test')
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

    topPortlets {
        jenkinsJobsList()
    }
    leftPortlets {
        testStatisticsChart()
    }
    rightPortlets {
        testTrendChart()
    }
    bottomPortlets {
        buildStatistics()
    }

}
