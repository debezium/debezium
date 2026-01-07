#!/usr/bin/env groovy
/**
 * Debezium project maintenance tool.
 *
 * This script automates routine management tasks for a single GitHub Projects (v2)
 * board in the Debezium organization. It works against issues assigned to a given
 * project iteration and can:
 *
 *  - check-issues-before-release
 *      Verifies that all issues in the given iteration:
 *        • have at least one `component/*` label
 *        • have Status = `Done` in the project
 *      Exits with a non-zero status if any issue violates these rules.
 *
 *  - generate-release-notes
 *      Generates CHANGELOG.md and release-notes.asciidoc sections on stdout based
 *      on issues in the given iteration. Issues are grouped by:
 *        • type/enhancement  -> “New features”
 *        • type/bug          -> “Fixes”
 *        • type/task         -> “Other changes”
 *      Issues labeled `backward-incompatible` are listed under “Breaking changes”.
 *
 *  - new-iteration
 *      Moves all items currently assigned to the given iteration to a new iteration
 *      (for all iteration-type fields where that iteration is selected). By default
 *      only items with Status = `Done` are moved; use `--all-issues` to move all.
 *
 *  - close-issues
 *      Closes all GitHub issues that belong to the given iteration in the
 *      `${owner}/dbz` repository, assuming none of them has a `resolution/*` label.
 *
 *  - release-issues
 *      Sets Status = `Released` in the project for all issues in the given
 *      iteration whose Status is currently `Done`. Exits with a non-zero status
 *      if any issue in the iteration has a different Status.
 *
 * Usage:
 *   groovy dbz-project-tool.groovy \\
 *     --owner debezium \\
 *     --project 5 \\
 *     --iteration "3.4.0.Final" \\
 *     --token "<github-token>" \\
 *     --action <check-issues-before-release|generate-release-notes|new-iteration|close-issues|release-issues> \\
 *     [--previous-iteration "3.4.0.CR1"] \\
 *     [--kafka-version "4.1.0.Final"] \\
 *     [--new-iteration "3.5.0.Alpha1"] \\
 *     [--all-issues]
 *
 * Required options:
 *   -o, --owner              GitHub org or user login (e.g. `debezium`)
 *   -p, --project            Project number from the URL (e.g. `5`)
 *   -i, --iteration          Iteration title as shown in the project (e.g. `3.4.0.Final`)
 *   -t, --token              GitHub personal access token with project/issue access
 *   -a, --action             One of: check-issues-before-release, generate-release-notes,
 *                            new-iteration, close-issues, release-issues
 *
 * Additional options (per action):
 *   --previous-iteration     (generate-release-notes) Previous Debezium iteration name
 *   --kafka-version          (generate-release-notes) Kafka version used for compatibility text
 *   --new-iteration          (new-iteration) Target iteration title to move items to
 *   --all-issues             (new-iteration) Move all issues in the iteration, not only
 *                            those with Status = `Done`
 *
 * Notes:
 *   - The script uses GitHub’s GraphQL API for project/iteration data and the
 *     GitHub Java API to close issues.
 *   - All output for release notes is written to stdout; redirect it to files
 *     as needed.
 */
@Grab('com.squareup.okhttp3:okhttp:4.12.0')
@Grab('org.kohsuke:github-api:1.330')

import groovy.cli.commons.CliBuilder
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.transform.ToString

import java.text.SimpleDateFormat

import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.MediaType

import org.kohsuke.github.GitHubBuilder

LABEL_RELEASE_NOTES = 'backward-incompatible'

def cli = new CliBuilder(usage: 'groovy dbz-project-tool.groovy [options]')
cli.with {
    h longOpt: 'help',                'Show this help'
    o longOpt: 'owner',               args: 1, required: true,  'Org or user login (e.g. debezium)'
    p longOpt: 'project',             args: 1, required: true,  'Project number from URL (e.g. 1)'
    i longOpt: 'iteration',           args: 1, required: true,  'Iteration title (exact, as shown in project)'
    t longOpt: 'token',               args: 1, required: true,  'GitHub token'
    a longOpt: 'action',              args: 1, required: true,  'Action to perform: generate-release-notes/check-issues-before-release/new-iteration/close-issues/release-issues'
    _ longOpt: 'previous-iteration',  args: 1, required: false, 'The previously released iteration'
    _ longOpt: 'kafka-version',       args: 1, required: false, 'Kafka version for this release'
    _ longOpt: 'new-iteration',       args: 1, required: false, 'The iteration to replace the existing one'
    _ longOpt: 'all-issues',          args: 0, required: false, 'Whether all issues should be affected or only those in defined state'
}

def options = cli.parse(args)

if (!options) {
    System.exit(1)
}
if (options.h) {
    cli.usage()
    System.exit(0)
}

owner = options.o
projectNumber = (options.p as int)
iterationTitle = options.i
previousIterationTitle = options.'previous-iteration'
newIterationTitle = options.'new-iteration'
kafkaVersion = options.'kafka-version'
allIssues = options.'all-issues'
itemPageSize = 50

token = options.t
action = options.a

queryIterations = '''
query($owner: String!, $number: Int!, $itemPageSize: Int!, $after: String) {
  organization(login: $owner) {
    projectV2(number: $number) {
      ...projectFields
    }
  }
}

fragment projectFields on ProjectV2 {
  id
  title
  url
  fields(first: 50) {
    nodes {
      __typename
      ... on ProjectV2IterationField {
        id
        name
        configuration {
          iterations {
            id
            title
            startDate
            duration
          }
        }
      }
      ... on ProjectV2SingleSelectField {
        id
        name
        options {
          id
          name
        }
      }
    }
  }
  items(first: $itemPageSize, after: $after) {
    nodes {
      id
      content {
        __typename
        ... on Issue {
          id
          number
          title
          url
          repository { nameWithOwner }
          labels(first: 50) {
            nodes {
              name
            }
          }
          state
        }
      }
      fieldValues(first: 20) {
        nodes {
          __typename
          ... on ProjectV2ItemFieldIterationValue {
            title
            iterationId
            field {
              ... on ProjectV2IterationField {
                id
                name
              }
            }
          }
          ... on ProjectV2ItemFieldSingleSelectValue {
            name
            optionId
            field {
              ... on ProjectV2SingleSelectField {
                id
                name
              }
            }
          }
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
'''

modifyOperationPre = '''
mutation(
  $projectId: ID!
) {
'''

availableIterations = null
projectId = null

statusField = null
statusOptionsByName = null

enum IssueType {
    bug, enhancement, task
}

@ToString
class GitHubIssue {
    def LABEL_COMPONENT = 'component/'
    def LABEL_ISSUE_TYPE = 'type/'
    def LABEL_RESOLUTION = 'resolution/'

    def itemId
    def issueId
    def number
    def title
    def url
    def labels
    def iterations
    def status

    def getType() {
        IssueType.valueOf(labels.find({ it.startsWith(LABEL_ISSUE_TYPE) })[5..-1])
    }

    def getComponents() {
        labels.findAll({ it.startsWith(LABEL_COMPONENT) }).collect { it[10..-1] }
    }

    def getCurrentIteration() {
        iterations['Iteration']
    }

    def getStableIteration() {
        iterations['Stable Iteration']
    }

    def getProductIteration() {
        iterations['Product Iteration']
    }

    def findIterationField(iteration) {
        iterations.find({k, v -> iteration == v})?.key
    }

    def getResolution() {
        labels.find({ it.startsWith(LABEL_RESOLUTION) })?[11..-1]
    }
        
}

@ToString
class Iteration {
    def id
    def name
    def iterationNames
}

def getProjectIssuesForIteration(iteration) {
    def client = new OkHttpClient()
    def slurper = new JsonSlurper()
    def variables = [
        'owner': owner,
        'number': projectNumber,
        'itemPageSize': itemPageSize,
        'after': null
    ]

    def hasNextPage = true
    def endCursor = null

    def issues = []

    while (hasNextPage) {
        variables.after = endCursor
        def payload = JsonOutput.toJson([query: queryIterations, variables: variables])

        def requestBody = RequestBody.create(payload, MediaType.get("application/json"))
        def request = new Request.Builder()
                .url("https://api.github.com/graphql")
                .addHeader("Authorization", "Bearer ${token}")
                .addHeader("Accept", "application/vnd.github+json")
                .post(requestBody)
                .build()
        def response = client.newCall(request).execute()
        def body = response.body().string()

        if (!response.isSuccessful()) {
            System.err.println("GitHub GraphQL call failed: HTTP ${response.code()}")
            System.err.println(body)
            System.exit(3)
        }

        def json = slurper.parseText(body)
        if (json.errors) {
            System.err.println("Failed to parse GraphQL response with errors:")
            json.errors.each { System.err.println(" - ${it.message}") }
            System.exit(4)
        }

        def data = json.data
        def project = data.organization?.projectV2 ?: data.user?.projectV2
        if (!project) {
            System.err.println("Project not found for owner='${owner}', number=${projectNumber}")
            System.exit(5)
        }
        projectId = project.id
        availableIterations = project.fields.nodes.findAll({ it.__typename == 'ProjectV2IterationField' }).collectEntries { [(it.name):
            new Iteration(id: it.id, name: it.name, iterationNames: it.configuration.iterations.collectEntries { [(it.title): it.id] })] }
        statusField = project.fields.nodes.find({ it.__typename == 'ProjectV2SingleSelectField' && it.name == 'Status'})
        statusOptionsByName = statusField.options.collectEntries { [(it.name): it.id] }

        def items = project.items
        items.nodes.each { item ->
            def content = item.content
            if (!content || content.__typename != 'Issue') {
                return
            }
            def labels = content.labels.nodes.collect { it.name }
            def status = item.fieldValues.nodes.findAll({ it.__typename == 'ProjectV2ItemFieldSingleSelectValue' && it.field.name == 'Status' }).collect({ it.name }).first()
            def iterations = item.fieldValues.nodes.findAll({ it.__typename == 'ProjectV2ItemFieldIterationValue' }).collectEntries { [(it.field.name): it.title] }
            issue = new GitHubIssue(itemId: item.id, issueId: content.id, number: content.number, title: content.title, url: content.url, labels: labels, status: status, iterations: iterations)
            if (issue.findIterationField(iteration)) {
                issues << issue
            }
        }

        hasNextPage = items.pageInfo.hasNextPage
        endCursor   = items.pageInfo.endCursor
    }

    return issues
}


def checkIterationIsReadyForRelease() {
    def checkFailed = false
    def issues = getProjectIssuesForIteration(iterationTitle)

    def issuesWithoutComponentSet = issues.findAll { !it.components }
    def issuesNotDone = issues.findAll { it.status != 'Done' }

    if (issuesWithoutComponentSet) {
        println '======================================================================'
        println 'All issues must have component set, there are issues without component'
        issuesWithoutComponentSet.each { println it.url }
        println '======================================================================'
        checkFailed = true
    }
    if (issuesNotDone) {
        println '=========================================================================='
        println 'All issues must have status Done, there are issues with a different status'
        issuesNotDone.each { println it.url }
        println '=========================================================================='
        checkFailed = true
    }

    if (checkFailed) {
        System.exit(8)
    }
}

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

def markdownSection(section, issues) {
    println """### $section since $previousIterationTitle
"""

    if (!issues) {
        println 'None'
    }
    else {
        issues.each { issue -> println "* ${issue.title} [debezium/dbz#${issue.number}](${issue.url})" }
    }

    println "\n"
}

def asciidocSection(section, issues) {
    println "=== $section\n"

    if (!issues) {
        println "There are no ${section.toLowerCase()} in this release."
    }
    else {
        issues.each { issue -> println "* ${issue.title} ${issue.url}[debezium/dbz#${issue.number}]" }
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


def generateReleaseNotes() {
    if (!kafkaVersion || !previousIterationTitle) {
        System.err.println 'Both Kafka version and previous Debezium relese version/iteration must be provided'
        System.exit(6)
    }

    def issues = getProjectIssuesForIteration(iterationTitle)
    if (issues.findAll { it.status != 'Done' }) {
        System.err.println 'All issues must have Done status'
        System.exit(6)
    }

    def newFeatures = issues.findAll { it.type == IssueType.Enhancement && !(LABEL_RELEASE_NOTES in it.labels) }
    def fixes = issues.findAll { it.type == IssueType.Bug && !(LABEL_RELEASE_NOTES in it.labels) }
    def otherChanges = issues.findAll { it.type == IssueType.Task && !(LABEL_RELEASE_NOTES in it.labels) }
    def breakingChanges = issues.findAll { LABEL_RELEASE_NOTES in it.labels }

    println """
================================================================================
                               CHANGELOG.md
================================================================================
## $iterationTitle
${today()} [Detailed release notes](https://github.com/orgs/debezium/projects/5/views/6?filterQuery=status%3AReleased+iteration%3A${iterationTitle})
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
[[release-${iterationTitle.toLowerCase().reverse().replaceFirst('\\.', '-').reverse()}]]
== *Release $iterationTitle* _(${today()})_

See the https://github.com/orgs/debezium/projects/5/views/6?filterQuery=status%3AReleased+iteration%3A${iterationTitle}[complete list of issues].

=== Kafka compatibility

This release has been built against Kafka Connect $kafkaVersion and has been tested with version $kafkaVersion of the Kafka brokers.
See the https://kafka.apache.org/documentation/#upgrade[Kafka documentation] for compatibility with other versions of Kafka brokers.


=== Upgrading

Before upgrading any connector, be sure to check the backward-incompatible changes that have been made since the release you were using.

When you decide to upgrade one of these connectors to $iterationTitle from any earlier versions,
first check the migration notes for the version you're using.
Gracefully stop the running connector, remove the old plugin files, install the $iterationTitle plugin files, and restart the connector using the same configuration.
Upon restart, the $iterationTitle connectors will continue where the previous connector left off.
As one might expect, all change events previously written to Kafka by the old connector will not be modified.

If you are using our container images, then please do not forget to pull them fresh from https://quay.io/organization/debezium[Quay.io].

"""
    asciidocPlaceholderSection('Breaking changes', breakingChanges)
    asciidocSection('New features', newFeatures)
    asciidocSection('Fixes', fixes)
    asciidocSection('Other changes', otherChanges)
    println '\n================================================================================'
}

def setNewIteration() {
    def issues = getProjectIssuesForIteration(iterationTitle)
    def issuesToUpdate = issues.collectEntries({ [(it): it.findIterationField(iterationTitle)] }).findAll { it.value }
    def updateQuery = new StringBuilder(modifyOperationPre)

    issuesToUpdate.each { issue, iterationFieldName ->
        if (!allIssues && (issue.status == 'Done' || issue.status == 'Released')) {
            return
        }
        def itemNo = issue.number
        def itemId = issue.itemId
        def fieldId = availableIterations[iterationFieldName].id
        def iterationId = availableIterations[iterationFieldName].iterationNames[newIterationTitle]
        if (!iterationId) {
            System.err.println "Iteration '$newIterationTitle' requested for field '$iterationFieldName' but not supported"
            System.exit(9)
        }
        def updateFragment = """
  setItem${itemNo}: updateProjectV2ItemFieldValue(
    input: {
      projectId: \$projectId,
      itemId: \"${itemId}\",
      fieldId: \"${fieldId}\",
      value: { iterationId: \"${iterationId}\" }
    }
  ) {
    projectV2Item { id }
  }
"""
        updateQuery << updateFragment
    }
    updateQuery << '}'

    def client = new OkHttpClient()
    def slurper = new JsonSlurper()
    def variables = [
        'projectId': projectId
    ]

    def payload = JsonOutput.toJson([query: updateQuery, variables: variables])

    def requestBody = RequestBody.create(payload, MediaType.get("application/json"))
    def request = new Request.Builder()
            .url("https://api.github.com/graphql")
            .addHeader("Authorization", "Bearer ${token}")
            .addHeader("Accept", "application/vnd.github+json")
            .post(requestBody)
            .build()
    def response = client.newCall(request).execute()
    def body = response.body().string()

    if (!response.isSuccessful()) {
        System.err.println("GitHub GraphQL call failed: HTTP ${response.code()}")
        System.err.println(body)
        System.exit(3)
    }

    def json = slurper.parseText(body)
    if (json.errors) {
        System.err.println("Failed to parse GraphQL response with errors:")
        json.errors.each { System.err.println(" - ${it.message}") }
        System.exit(4)
    }
}

def closeIssuesInIteration() {
    def issuesToClose = getProjectIssuesForIteration(iterationTitle)

    def github = new GitHubBuilder().withOAuthToken(token).build()
    def repo = github.getRepository(owner + '/dbz')

    if (issuesToClose.findAll { it.resolution }) {
        System.err.println 'There are issues with resolution label set, this is not expected'
        System.exit(6)
    }

    issuesToClose.each { repo.getIssue(it.number).close() }
}

def setStatusToReleasedInIteration() {
    def issuesToMarkAsReleased = getProjectIssuesForIteration(iterationTitle).findAll { it.status != 'Released' }
    def issuesNotDone = issuesToMarkAsReleased.findAll { it.status != 'Done' }

    if (issuesNotDone) {
        println '=========================================================================='
        println 'All issues must have status Done, there are issues with a different status'
        issuesNotDone.each { println it.url }
        println '=========================================================================='
        checkFailed = true
        System.exit(8)
    }

    def updateQuery = new StringBuilder(modifyOperationPre)
    issuesToMarkAsReleased.each { issue ->
        def itemNo = issue.number
        def itemId = issue.itemId
        def releaseOptionId = statusOptionsByName['Released']

        def updateFragment = """
  setStatus${itemNo}: updateProjectV2ItemFieldValue(
    input: {
      projectId: \$projectId,
      itemId: \"${itemId}\",
      fieldId: \"${statusField.id}\",
      value: { singleSelectOptionId: \"${releaseOptionId}\" }
    }
  ) {
    projectV2Item { id }
  }
"""
        updateQuery << updateFragment
    }
    updateQuery << '}'

    def client = new OkHttpClient()
    def slurper = new JsonSlurper()
    def variables = [
        'projectId': projectId
    ]

    def payload = JsonOutput.toJson([query: updateQuery, variables: variables])

    def requestBody = RequestBody.create(payload, MediaType.get("application/json"))
    def request = new Request.Builder()
            .url("https://api.github.com/graphql")
            .addHeader("Authorization", "Bearer ${token}")
            .addHeader("Accept", "application/vnd.github+json")
            .post(requestBody)
            .build()
    def response = client.newCall(request).execute()
    def body = response.body().string()

    if (!response.isSuccessful()) {
        System.err.println("GitHub GraphQL call failed: HTTP ${response.code()}")
        System.err.println(body)
        System.exit(3)
    }

    def json = slurper.parseText(body)
    if (json.errors) {
        System.err.println("Failed to parse GraphQL response with errors:")
        json.errors.each { System.err.println(" - ${it.message}") }
        System.exit(4)
    }
}

switch (action) {
    case 'check-issues-before-release':
        checkIterationIsReadyForRelease()
        break
    case 'generate-release-notes':
        generateReleaseNotes()
        break
    case 'new-iteration':
        setNewIteration()
        break
    case 'close-issues':
        closeIssuesInIteration()
        break
    case 'release-issues':
        setStatusToReleasedInIteration()
        break
}
