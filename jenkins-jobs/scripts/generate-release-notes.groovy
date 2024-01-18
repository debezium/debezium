import groovy.json.*

import java.text.SimpleDateFormat

JIRA_BASE_URL = 'https://issues.redhat.com/rest/api/2'
JIRA_PROJECT = 'DBZ'
RELEASE_NOTES_LABEL = 'add-to-upgrade-guide'
ISSUE_BUG = ['Bug'] as Set
ISSUE_ENHANCEMENT = ['Enhancement', 'Feature Request'] as Set
ISSUE_TASK = ['Task', 'Sub-task']
ISSUE_BASE_URL = 'https://issues.redhat.com/browse/'

if (args.length != 4) {
    println "Usage: generate-release-notes.groovy <release-version> <previous-version> <kafka-version> <jira-token>"
    return -1
}
version = args[0]
previousVersion = args[1]
kafkaVersion = args[2]
jiraPat = args[3]

def today() {
    def currentDate = new Date()
    def format
    switch (currentDate.date % 10) {
    case 1:
    case 21:
    case 31:
        format = new SimpleDateFormat("MMMM d'st' yyyy", Locale.ENGLISH)
        break
    case 2:
    case 22:
        format = new SimpleDateFormat("MMMM d'nd' yyyy", Locale.ENGLISH)
        break
    case 3:
    case 23:
        format = new SimpleDateFormat("MMMM d'rd' yyyy", Locale.ENGLISH)
        break
    default:
        format = new SimpleDateFormat("MMMM d'th' yyyy", Locale.ENGLISH)
    }
    format.format(currentDate)
}

def jiraURL(path, params = [:]) {
    def url = "$JIRA_BASE_URL/$path"
    if (params) {
        url <<= '?' << params.collect {k, v -> "$k=${URLEncoder.encode(v, 'US-ASCII')}"}.join('&')
    }
    return url.toString().toURL()
}

def jiraGET(path, params = [:]) {
    jiraURL(path, params).openConnection().with {
        doOutput = true
        requestMethod = 'GET'
        setRequestProperty('Content-Type', 'application/json')
        setRequestProperty('Authorization', "Bearer $jiraPat")
        new JsonSlurper().parse(new StringReader(content.text))
    }
}

def unresolvedIssuesFromJira() {
    jiraGET('search', [
        'jql': "project=$JIRA_PROJECT AND fixVersion=$version ORDER BY key ASC",
        'maxResults': '500'
    ]).issues.collect { ['key': it.key, 'title': it.fields.summary, 'type': it.fields.issuetype.name, 'labels': it.fields.labels] }
}

def findVersion() {
    jiraGET('project/DBZ/versions').find { it.name == version }
}

def markdownSection(section, issues) {
    println """### $section since $previousVersion
"""

    if (!issues) {
        println 'None'
    }
    else {
        issues.each { issue -> println "* ${issue.title} [$issue.key](${ISSUE_BASE_URL}${issue.key})" }
    }

    println "\n"
}

def asciidocSection(section, issues) {
    println "=== $section\n"

    if (!issues) {
        println "There are no ${section.toLowerCase()} in this release."
    }
    else {
        issues.each { issue -> println "* ${issue.title} ${ISSUE_BASE_URL}${issue.key}[$issue.key]" }
    }

    println '\n'
}

def asciidocPlaceholderSection(section, issues) {
    println "=== $section\n"

    if (!issues) {
        println "There are no ${section.toLowerCase()} in this release."
    }
    else {
        issues.each { issue -> println "[Placeholder for $section text] (${ISSUE_BASE_URL}${issue.key}[$issue.key]).\n" }
    }

    println '\n'
}

def versionId = findVersion().id
def issues = unresolvedIssuesFromJira()

def newFeatures = issues.findAll { it.type in ISSUE_ENHANCEMENT && !(RELEASE_NOTES_LABEL in it.labels) }
def fixes = issues.findAll { it.type in ISSUE_BUG && !(RELEASE_NOTES_LABEL in it.labels) }
def otherChanges = issues.findAll { it.type in ISSUE_TASK && !(RELEASE_NOTES_LABEL in it.labels) }
def breakingChanges = issues.findAll { RELEASE_NOTES_LABEL in it.labels }

println """
================================================================================
                               CHANGELOG.md
================================================================================
## $version
${today()} [Detailed release notes](https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=$versionId)
"""

markdownSection('New features', newFeatures)
markdownSection('Breaking changes', breakingChanges)
markdownSection('Fixes', fixes)
markdownSection('Other changes', otherChanges)
println """
================================================================================
================================================================================
                               release-notes.asciidoc
================================================================================
[[release-${version.toLowerCase().reverse().replaceFirst('\\.', '-').reverse()}]]
== *Release $version* _(${today()})_

See the https://issues.redhat.com/secure/ReleaseNote.jspa?projectId=12317320&version=$versionId[complete list of issues].

=== Kafka compatibility

This release has been built against Kafka Connect $kafkaVersion and has been tested with version $kafkaVersion of the Kafka brokers.
See the https://kafka.apache.org/documentation/#upgrade[Kafka documentation] for compatibility with other versions of Kafka brokers.


=== Upgrading

Before upgrading any connector, be sure to check the backward-incompatible changes that have been made since the release you were using.

When you decide to upgrade one of these connectors to $version from any earlier versions,
first check the migration notes for the version you're using.
Gracefully stop the running connector, remove the old plugin files, install the $version plugin files, and restart the connector using the same configuration.
Upon restart, the $version connectors will continue where the previous connector left off.
As one might expect, all change events previously written to Kafka by the old connector will not be modified.

If you are using our container images, then please do not forget to pull them fresh from https://quay.io/organization/debezium[Quay.io].

"""
asciidocPlaceholderSection('Breaking changes', breakingChanges)
asciidocSection('New features', newFeatures)
asciidocSection('Fixes', fixes)
asciidocSection('Other changes', otherChanges)
println '\n================================================================================'
