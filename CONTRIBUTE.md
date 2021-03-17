# Contributing to Debezium

The Debezium community welcomes anyone that wants to help out in any way, whether that includes reporting problems, helping with documentation, or contributing code changes to fix bugs, add tests, or implement new features. This document outlines the basic steps required to work with and contribute to the Debezium codebase.

## Table of contents

- [Talk to us](#talk-to-us)
  - [User chat](https://gitter.im/debezium/user)
  - [Developers chat](https://gitter.im/debezium/user) - Only for internal development subjects
  - [Google Group](https://groups.google.com/forum/#!forum/debezium)
  - [JIRA](https://issues.jboss.org/projects/DBZ/issues/) - You can [create an account for free](https://developer.jboss.org/register.jspa)
- [Install the tools](#install-the-tools)
- Repository
  - [GitHub account](#github-account)
  - [Fork the Debezium repository](#fork-the-debezium-repository)
  - [Clone your fork](#clone-your-fork)
  - [Get the latest upstream code](#get-the-latest-upstream-code)
- Local development
  - [Building locally](#building-locally)
  - [Running and debugging tests](#running-and-debugging-tests)
  - [Making changes](#making-changes)
  - [Rebasing](#rebasing)
- Proposing the changes
  - [Creating a pull request](#creating-a-pull-request)
  - [Continuous Integration](#continuous-integration)
  - [Summary](#summary)

### Talk to us

You can talk to us in our [chat room for users](https://gitter.im/debezium/user) or on our [Google Group](https://groups.google.com/forum/#!forum/debezium). If you want to contribute, consider joining the [chat room for developers](https://gitter.im/debezium/dev). We also [track our issues using JIRA](https://issues.jboss.org/projects/DBZ/issues/) and you can [create an account for free](https://developer.jboss.org/register.jspa); please don't create GitHub issues.

### Install the tools

The following software is required to work with the Debezium codebase and build it locally:

* [Git 2.2.1](https://git-scm.com) or later
* [JDK 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) or [OpenJDK 8](http://openjdk.java.net/projects/jdk8/)
* [Maven 3.2.1](https://maven.apache.org/index.html) or later
* [Docker Engine 1.9](http://docs.docker.com/engine/installation/) or later

See the links above for installation instructions on your platform. You can verify the versions are installed and running:

    $ git --version
    $ javac -version
    $ mvn -version
    $ docker --version

### GitHub account

Debezium uses [GitHub](GitHub.com) for its primary code repository and for pull-requests, so if you don't already have a GitHub account you'll need to [join](https://github.com/join).

### Fork the Debezium repository

Go to the [Debezium repository](https://github.com/debezium/debezium) and press the "Fork" button near the upper right corner of the page. When finished, you will have your own "fork" at `https://github.com/<your-username>/debezium`, and this is the repository to which you will upload your proposed changes and create pull requests. For details, see the [GitHub documentation](https://help.github.com/articles/fork-a-repo/).

### Clone your fork

At a terminal, go to the directory in which you want to place a local clone of the Debezium repository, and run the following commands to use HTTPS authentication:

    $ git clone https://github.com/<your-username>/debezium.git

If you prefer to use SSH and have [uploaded your public key to your GitHub account](https://help.github.com/articles/adding-a-new-ssh-key-to-your-github-account/), you can instead use SSH:

    $ git clone git@github.com:<your-username>/debezium.git

This will create a `debezium` directory, so change into that directory:

    $ cd debezium

This repository knows about your fork, but it doesn't yet know about the official or ["upstream" Debezium repository](https://github.com/debezium/debezium). Run the following commands:

    $ git remote add upstream https://github.com/debezium/debezium.git
    $ git fetch upstream
    $ git branch --set-upstream-to=upstream/master master

Now, when you check the status using Git, it will compare your local repository to the *upstream* repository.

### Get the latest upstream code

You will frequently need to get all the of the changes that are made to the upstream repository, and you can do this with these commands:

    $ git fetch upstream
    $ git pull upstream master

The first command fetches all changes on all branches, while the second actually updates your local `master` branch with the latest commits from the `upstream` repository.

### Building locally

To build the source code locally, checkout and update the `master` branch:

    $ git checkout master
    $ git pull upstream master

Then use Maven to compile everything, run all unit and integration tests, build all artifacts, and install all JAR, ZIP, and TAR files into your local Maven repository:

    $ mvn clean install -Passembly

If you want to skip the integration tests (e.g., if you don't have Docker installed) or the unit tests, you can add `-DskipITs` and/or `-DskipTests` to that command:

    $ mvn clean install -Passembly -DskipITs -DskipTests

### Running and debugging tests

A number of the modules use Docker during their integration tests to run a database. During development it's often desirable to start the Docker container and leave it running so that you can compile/run/debug tests repeatedly from your IDE. To do this, simply go into one of the modules (e.g., `cd debezium-connector-mysql`) and run the following command:

    $ mvn docker:build docker:start

This will first force the build to create a new Docker image for the database container, and then will start a container named "database". You can then run any integration tests from your IDE, though all of our integration tests expect the database connection information to be passed in as system variables (like our Maven build does). For example, the MySQL connector integration tests expect something like `-Ddatabase.hostname=localhost -Ddatabase.port=3306` to be passed as arguments to your test.

When your testing is complete, you can stop the Docker container by running:

    $ mvn docker:stop

or the following Docker commands:

    $ docker stop database; docker rm database

### Making changes

Everything the community does with the codebase -- fixing bugs, adding features, making improvements, adding tests, etc. -- should be described by an issue in our [JIRA](https://issues.jboss.org/projects/DBZ/issues/). If no such issue exists for what you want to do, please create an issue with a meaningful and easy-to-understand description.
If you are going to work on a specific issue and it's your first contribution,
please add a short comment to the issue, so other people know you're working on it.
If you are contributing repeatedly, ask in our [chat room for developers](https://gitter.im/debezium/dev) for the 'developer' JIRA role so you can assign issues to yourself.

Before you make any changes, be sure to switch to the `master` branch and pull the latest commits on the `master` branch from the upstream repository. Also, it's probably good to run a build and verify all tests pass *before* you make any changes.

    $ git checkout master
    $ git pull upstream master
    $ mvn clean install

Once everything builds, create a *topic branch* named appropriately (we recommend using the issue number, such as `DBZ-1234`):

    $ git checkout -b DBZ-1234

This branch exists locally and it is there you should make all of your proposed changes for the issue. As you'll soon see, it will ultimately correspond to a single pull request that the Debezium committers will review and merge (or reject) as a whole. (Some issues are big enough that you may want to make several separate but incremental sets of changes. In that case, you can create subsequent topic branches for the same issue by appending a short suffix to the branch name.)

Your changes should include changes to existing tests or additional unit and/or integration tests that verify your changes work. We recommend frequently running related unit tests (in your IDE or using Maven) to make sure your changes didn't break anything else, and that you also periodically run a complete build using Maven to make sure that everything still works:

    $ mvn clean install

Feel free to commit your changes locally as often as you'd like, though we generally prefer that each commit represent a complete and atomic change to the code. Often, this means that most issues will be addressed with a single commit in a single pull-request, but other more complex issues might be better served with a few commits that each make separate but atomic changes. (Some developers prefer to commit frequently and to ammend their first commit with additional changes. Other developers like to make multiple commits and to then squash them. How you do this is up to you. However, *never* change, squash, or ammend a commit that appears in the history of the upstream repository.) When in doubt, use a few separate atomic commits; if the Debezium reviewers think they should be squashed, they'll let you know when they review your pull request.

Committing is as simple as:

    $ git commit .

which should then pop up an editor of your choice in which you should place a good commit message. _*We do expect that all commit messages begin with a line starting with the JIRA issue and ending with a short phrase that summarizes what changed in the commit.*_ For example:

    DBZ-1234 Expanded the MySQL integration test and correct a unit test.

If that phrase is not sufficient to explain your changes, then the first line should be followed by a blank line and one or more paragraphs with additional details. For example:

```
DBZ-1235 Added support for ingesting data from PostgreSQL.

The new ingest library supports PostgreSQL 9.4 or later. It requires a small plugin to be installed
on the database server, and the database to be configured so that the ingest component can connect
to the database and use logical decoding to read the transaction log. Several new unit tests and one
integration test were added.
```

### Code Formatting

This project utilizes a set of code style rules that are automatically applied by the build process.  There are two ways in which you can apply these rules while contributing:

1. If your IDE supports importing an Eclipse-based formatter file, navigate to your IDE's code format section and import the file `support/ide-configs/src/main/resources/eclipse/debezium-formatter.xml`.  It's recommended that when you import these styles, you may want to make sure these are only applicable only to the current project.

2. If your IDE does not support importing the Eclipse-based formatter file or you'd rather tidy up the formatting after making your changes locally, you can run a project build to make sure that all code changes adhere to the project's desired style.  Instructions on how to run a build locally are provided below.

3. With the command `mvn process-sources` the code style rules can be applied automatically.

In the event that a pull request is submitted with code style violations, continuous integration will fail the pull request build.  

To fix pull requests with code style violations, simply run the project's build locally and allow the automatic formatting happen.  Once the build completes, you will have some local repository files modified to fix the coding style which can be amended on your pull request.  Once the pull request is synchronized with the formatting changes, CI will rerun the build.

To run the build, navigate to the project's root directory and run:

    $ mvn clean install

It might be useful to simply run a _validate_ check against the code instead of automatically applying code style changes.  If you want to simply run validation, navigate to the project's root directory and run:

    $ mvn clean install -Dformat.formatter.goal=validate -Dformat.imports.goal=check     

Please note that when running _validate_ checks, the build will stop as soon as it encounters its first violation.  This means it is necessary to run the build multiple times until no violations are detected.

### Rebasing

If its been more than a day or so since you created your topic branch, we recommend *rebasing* your topic branch on the latest `master` branch. This requires switching to the `master` branch, pulling the latest changes, switching back to your topic branch, and rebasing:

    $ git checkout master
    $ git pull upstream master
    $ git checkout DBZ-1234
    $ git rebase master

If your changes are compatible with the latest changes on `master`, this will complete and there's nothing else to do. However, if your changes affect the same files/lines as other changes have since been merged into the `master` branch, then your changes conflict with the other recent changes on `master`, and you will have to resolve them. The git output will actually tell you you need to do (e.g., fix a particular file, stage the file, and then run `git rebase --continue`), but if you have questions consult Git or GitHub documentation or spend some time reading about Git rebase conflicts on the Internet.

### Documentation

When adding new features such as e.g. a connector or configuration options, they must be documented accordingly in the Debezium [reference documentation](https://debezium.io/documentation/).
The same applies when changing existing behaviors, e.g. type mappings, removing options etc.

The documentation is written using AsciiDoc/Antora and can be found in the Debezium [source code repository](https://github.com/debezium/debezium/tree/master/documentation).
Any documentation update should be part of the pull request you submit for the code change.

Generally, two versions of documentation are published on the website at a given time:

* The documentation for the current _stable_ release (e.g. 0.9.5.Final at the time of writing)
* The documentation for the current _development_ release (e.g. 0.10.0.Beta4)

Documentation changes applying to the development release will be published on the website when the release containing the documented features is done.
In order to apply immediate changes to the documentation without awaiting the next release,
submit a pull request targeting the [development_docs](https://github.com/debezium/debezium/tree/development_docs/documentation) branch.
This should only be done for critical fixes such as correcting wrong documentation; minor changes such as typo fixes should simply be done on the `master` branch.
Upon each release, the `development_docs` branch is rebased to `master`, resulting in the publication of all doc changes done on the `master` branch since the last release.

### Creating a pull request

Once you're finished making your changes, your topic branch should have your commit(s) and you should have verified that your branch builds successfully. At this point, you can shared your proposed changes and create a pull request. To do this, first push your topic branch (and its commits) to your fork repository (called `origin`) on GitHub:

    $ git push origin DBZ-1234

Then, in a browser go to https://github.com/debezium/debezium, and you should see a small section near the top of the page with a button labeled "Create pull request". GitHub recognized that you pushed a new topic branch to your fork of the upstream repository, and it knows you probably want to create a pull request with those changes. Click on the button, and GitHub will present you with a short form that you should fill out with information about your pull request. The title should start with the JIRA issue and ending with a short phrase that summarizes the changes included in the pull request. (If the pull request contains a single commit, GitHub will automatically prepopulate the title and description fields from the commit message.)

When completed, press the "Create" button and copy the URL to the new pull request. Go to the corresponding JIRA issue and record the pull request by pasting the URL into the "Pull request" field. (Be sure to not overwrite any URLs that were already in this field; this is how a single issue is bound to multiple pull requests.) Also, please add a JIRA comment with a clear description of what you changed. You might even use the commit message (except for the first line).

At this point, you can switch to another issue and another topic branch. The Debezium committers will be notified of your new pull request, and will review it in short order. They may ask questions or make remarks using line notes or comments on the pull request. (By default, GitHub will send you an email notification of such changes, although you can control this via your GitHub preferences.)

If the reviewers ask you to make additional changes, simply switch to your topic branch for that pull request:

    $ git checkout DBZ-1234

and then make the changes on that branch and either add a new commit or ammend your previous commits. When you've addressed the reviewers' concerns, push your changes to your `origin` repository:

    $ git push origin DBZ-1234

GitHub will automatically update the pull request with your latest changes, but we ask that you go to the pull request and add a comment summarizing what you did. This process may continue until the reviewers are satisfied.

By the way, please don't take offense if the reviewers ask you to make additional changes, even if you think those changes are minor. The reviewers have a broach understanding of the codebase, and their job is to ensure the code remains as uniform as possible, is of sufficient quality, and is thoroughly tested. When they believe your pull request has those attributes, they will merge your pull request into the official upstream repository.

Once your pull request has been merged, feel free to delete your topic branch both in your local repository:

    $ git branch -d DBZ-1234

and in your fork:

    $ git push origin :DBZ-1234

(This last command is a bit strange, but it basically is pushing an empty branch (the space before the `:` character) to the named branch. Pushing an empty branch is the same thing as removing it.)

### Continuous Integration

The project currently builds its jobs in two environments:

- GitHub Actions for pull requests: https://github.com/debezium/debezium/actions
  - Tests run only against the current version of each supported database
- Jenkins CI for tests matrix, deployment, release, etc - http://ci.hibernate.org/view/Debezium/
  - Test run against all database versions supported by the individual connectors
  - Test Kafka versions
  - Deploy and release

### Summary

Here's a quick check list for a good pull request (PR):

* Discussed and approved on IRC or the mailing list
* A JIRA associated with your PR (include the JIRA issue number in commit comment)
* One commit per PR
* One feature/change per PR
* No changes to code not directly related to your change (e.g. no formatting changes or refactoring to existing code, if you want to refactor/improve existing code that's a separate discussion and separate JIRA issue)
* New/changed features have been documented
* A full build completes successfully
* Do a rebase on upstream `master`
