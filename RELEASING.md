# Releasing Debezium

The Debezium project uses Maven for its build system, relying up on the _release_ plugin to most of the work.

The release process is automated by means of a parameterized [Jenkins job](https://github.com/debezium/debezium/blob/main/jenkins-jobs/release.yaml),
that takes the required information as an input (release version etc.)
and performs most of the required tasks.
Refer to [Automated Release](#automated-release) below for the details.

The following describes the individual steps of the release process,
which may be useful for updating the automated pipeline or in case a manual release is necessary.

## Configure Debezium versions in Jira

Start off from the [__Releases__](https://issues.redhat.com/projects/DBZ?selectedItem=com.atlassian.jira.jira-projects-plugin%3Arelease-page&status=released-unreleased)
page of the Debezium Jira project.

First, rename the current version (e.g. `1.7-next`) to the new version that
will be released (e.g. `1.7.0.Beta1`). All issues previously assigned to the old
_next_ version are now set to the version of the release.  
Then create a new `next` version, e.g. `1.7-next`.

## Verify Jira issues

All issues planned for this release must be resolved.
If not, they have to either be re-planned to another release or rejected.
Use JQL query to find offending issues
```
project=DBZ AND fixVersion=<VERSION> AND status NOT IN ('Resolved', 'Closed')
```

or navigate to the specific release on the [_Releases_](https://issues.redhat.com/projects/DBZ?selectedItem=com.atlassian.jira.jira-projects-plugin%3Arelease-page&status=released-unreleased)
page. 

Also make sure that each issue is assigned to a component ("mysql-connector" etc.).

## Update the changelog and breaking changes

Create two branches for pull requests to add changelogs (and release announcement) for this
main repository and the [`debezium.github.io` repository](https://github.com/debezium/debezium.github.io).
The change log should be updated with all relevant info on new features, bug fixes and breaking changes and
can be generated with the `Release Notes` tool in JIRA on the version's detail page.

* https://github.com/debezium/debezium/blob/main/CHANGELOG.md
* https://github.com/debezium/debezium.github.io/blob/develop/_data/releases/1.7/1.7.0.Beta1.yml
* https://github.com/debezium/debezium.github.io/blob/develop/releases/1.7/release-notes.asciidoc
* https://github.com/debezium/debezium.github.io/blob/develop/_data/releases/1.7/series.yml

JIRA issues that break backwards compatability for existing consumers, should be marked with the "add-to-upgrade-guide" label.
Search for them using [this query](https://issues.jboss.org/issues/?jql=labels%20%3D%20add-to-upgrade-guide) and describe the
implications and required steps for upgrading in the changelog on the website.

## Update antora.yml and series.yml

The `antora.yml` file in the `main` branch always used the version _main_.
During the release process, this file's `version` attribute should be changed to reference the correct major/minor version number.
There are other Asciidoc variables defined here that should be reviewed and modified as needed.

As an example, when releasing version `2.1`, the `antora.yml` file should change from:
```
version: 'main'
```
to
```
version: '2.1'
```

`series.yml` holds metadata for the `Tested Versions` section on the Debezium [Releases](https://debezium.io/releases/)
site and should be updated when dependencies were updated with the new release.


# Manual Release

You can skip this section to [__Automated Release__](#automated-release) when
using the Jenkins CI/CD Release pipeline for the rest of the release process.

## Start with the correct branch

Make sure that you are on the correct branch that is to be released, and that your local Git repository has all of the most recent commits. For example, to release from the `main` branch on the remote repository named `upstream`:

    $ git checkout main
    $ git pull upstream main

Then make sure that you have no local uncommitted changes:

    $ git status

This should report:

    On branch master
    Your branch is up-to-date with 'upstream/master'.
    nothing to commit, working directory clean

Only if this is the case can you proceed with the release.

## Update versions and tag

Once the codebase is in a state that is ready to be released, use the following command to automatically update the POM to use the release number, commit the changes to your local Git repository, tag that commit, and then update the POM to use snapshot versions and commit to your local Git repository:

    $ mvn release:clean release:prepare

This will prompt for several inputs:

* The release version for each of the modules; answer each with the _major.minor.patch_ version number, such as `0.2.0`.
* The SCM release tag or label; answer with `v<releaseVersion>`, or `v0.2.0`.
* The new development version for each of the modules; answer each with the next _major.minor_ snapshot version, such as `0.3-SNAPSHOT`.

The build then proceeds to:

1. automatically update the POMs to use the release number;
2. update the POMs with the release version;
3. perform a build with the updated POMs using the `release`, `assembly`, and `docs` profiles;
4. commit the changes to your local Git repository;
5. tag that commit with the label you supplied;
6. update the POMs to use the new development version you supplied; and
7. commit the changes to your local Git repository.

_Note: our child module POMs do not specify the version, but instead inherit the version from the parent POM. This results in Maven output that shows several of the steps are sometimes skipped. This is normal._

You can look at the Git history to verify the changes:

    $ git log --pretty=oneline --decorate --graph --abbrev-commit

which should show the most recent commits first. The first two lines should look something like:

    * 6749518 (HEAD, upstream/master, master) [maven-release-plugin] prepare for next development iteration
    * d5bbb11 (tag: v0.2.0) [maven-release-plugin] prepare release v0.2.0

followed by commits made prior to the release. The second line shows the commit from step 4 and includes the tag, while the first line shows the subsequent commit from step 7.

Also point the `development_docs` branch to the release tag:

    $ git checkout development_docs
    $ git merge <release tag>

## Push to Git

If the release was successfully prepared, the next step is to push the commits and the tag to the Debezium upstream repository.

    $ git push upstream --follow-tags

Note that if you check for local changes using `git status`, you will see a handful of untracked files, including multiple `pom.xml.releaseBackup` files and a `release.properties` file. These are used by the Maven release plugin, so leave them for the next step in the release process.

At this point, the code on the branch in the [official Debezium repository](https://github.com/debezium/debezium) has been modified and a tag has been created for the release, and we know that the code on the tag successfully builds. Now we're ready to actually perform the release.

## Perform the release

Now that the [official Debezium repository](https://github.com/debezium/debezium) has a tag with the code that we want to release, the next step is to actually build and release what we tagged:

    $ mvn release:perform

This performs the following steps:

1. check out the tagged code from the official Debezium repository;
2. run a full build of that code to create all of the release artifacts;
3. push those artifacts into a staging repository at Maven Central; and
4. close the staging repository

At this point, the staging repository contains all the artifacts for the release, and we need to check that they are valid. The staging repository has already been _closed_ (meaning nothing more can be pushed to the staging repository), but it hasn't yet been _released_ (when its contents get uploaded into Maven Central).

Before we _release_ the staging repository, we first need to review and verify the artifacts.

### Reviewing the staged artifacts

Go to Sonatype's Nexus Repository Manager at https://oss.sonatype.org/ and log in with an account that has privilege to release Debezium artifacts. In the left hand panel, click on the "Staging Repositories". Then type "debezium" in the search box to locate the staging repository, which should be _closed_ but not _released_.

Select the staging repository to see the details, including the staging repository's URL, in the lower portion of the window. Briefly navigate the staged repository contents to verify all modules exist and have the proper versions. You can even look at the POM files or view the details of a file (including its SHA1 and MD5 hash values) using this tool.

Before continuing, using this lower frame of the window to collect the following pieces of information:

* In the "Summary" tab, locate the URL of the staging repository, which might look something like this: `https://oss.sonatype.org/content/repositories/iodebezium-1002`
* In the "Content" tab, navigate to, select, and obtain the MD5 hash of the `...-plugin.tar.gz` file for each connector. (Do _not_ select the `...-plugin.tar.gz.md5` file, since the Repository Manager does not show the contents of the file.)

### Validating the staged artifacts

At this time, the best way to verify the staged artifacts are valid is to locally update the [Debezium Docker images](https://github.com/debezium/docker-images) used in the [Debezium tutorial](https://debezium.io/docs/tutorial) to use the latest connector plugins, and then to run through the tutorial using those locally-built Docker images.

This [GitHub repository](https://github.com/debezium/docker-images) containing the Docker images contains separate Dockerfiles for each major and minor release of Debezium. Start by checking out and getting the latest commits from the [Debezium Docker images](https://github.com/debezium/docker-images) repository and creating a topic branch (using an appropriate branch name):

    $ git checkout master
    $ git pull upstream master
    $ git checkout -b <branch-name>

For Debezium patch releases, we simply update the Dockerfiles that correspond to the minor release's parent. For example, when 0.2 was released, we created new Docker images by copying and updating the images from 0.1. However, when 0.2.1 was released, we simply updated the 0.2 Docker images.

Currently, the only Docker image that contains Debezium code is the [Connect service image](https://github.com/debezium/docker-images/connect), which means this is the only Dockerfile that you will need to change to point to the Maven staging repository. To do this, edit the Dockerfile for the Connect service, and:

* change the `DEBEZIUM_VERSION` environment variable value to match the _major.minor.patch_ version number for the release (e.g., `0.2.1`)
* temporarily replace the Maven Central repository URL with that URL of the staging repository created in [Perform the Release](#perform-the-release), which again looks something like `https://oss.sonatype.org/content/repositories/iodebezium-1002`.
* update the MD5 literal string used to check the `...-plugin.tar.gz` file

After all of the Docker files have been created or updated, go to the top of your local Git repository and run the following command to build the Docker images:

    $ ./build-debezium.sh 0.2

or using the correct Debezium version. After this successfully builds the images, run through the tutorial, being sure to use the same Docker image label (e.g., `0.2`) that you just built.

When the tutorial can be successfully run with the new version, edit the Dockerfile for the Connect service to again use the official Maven Central repository URL.

It is also necessary to update the Connect snapshot(nightly) image with a new development version. Edit `snapshot/Dockerfile` and set `DEBEZIUM_VERSION` environment variable to a new development version, e.g. `0.6.0-SNAPSHOT`.

Now you can commit the changes locally

    $ git commit -m "Updated Docker images for release 0.2.1" .
    $ git push origin <branch-name>

and create a new pull request (or if rerunning the release process update your existing pull request), but do not merge the pull request yet.

If you discover any problems, log issues for the problems and fix them in the code base. Then start this process over from the beginning (though you can force-update the changes to your still-unmerged pull request for the Docker images.

#### Updating the Java version

If you need to update the base Docker image for the JDK to use a different JDK version, then:

1. create a new folder under `jdk8` for the new Java versoin by copying the folder for the most recent existing JDK version,
2. edit the Dockerfile to use the specific version of the JDK and save the file,
3. run the `./build-java.sh` script to locally build the new `debezium/jdk:<version>` Docker image,
4. update all of the Docker images for Zookeeper, Kafka, and Debezium services to use the new base image,
5. re-run the `./build-debezium.sh <version>` command to make sure everything builds,
6. commit the changes locally and add to your still-unmerged pull request.

## Releasing the artifacts

Once you have verified that the artifacts in the staging repository are acceptable, the next step is to _release_ the staging repository so that its artfacts are then pushed into the Maven Central repository.

Go back to Sonatype's Nexus Repository Manager at https://oss.sonatype.org/ and log in with an account that has privilege to release Debezium artifacts. In the left hand panel, click on the "Staging Repositories". Then type "debezium" in the search box to locate the closed staging repository that you've verified.

Select the staging repository (by checking the box) and press the "Release" button. Enter a description in the dialog box, and press "OK" to release the artifacts to Maven Central. You may need to press the "Refresh" button a few times until the staging repository disappears.

It may take some time for the artifacts to actually be [visible in Maven Central search](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22io.debezium%22) or [directly in Maven Central](https://repo1.maven.org/maven2/io/debezium/debezium-core/).

## Merge your pull request to update the Docker images

Only after the artifacts are available on Maven Central can you merge the pull request for the Debezium Docker images. As soon as your changes are merged, Docker Hub will automatically build and deploy the Docker images that were previously configured. If you've just released a patch release and only updated existing Docker images, then you can proceed to [updating the documentation and blog](#update-the-documentation-and-blog).

Otherwise, for major and minor releases your pull request should have added new Docker images, and you need to log into [Debezium's Docker Hub organization](https://hub.docker.com/r/debezium/) and add/update the build settings for each of the affected images.

With every release the Docker image for PostgreSQL needs to be updated as well.
First create a tag in the [postgres-decoderbufs](https://github.com/debezium/postgres-decoderbufs) repository:

    $ git tag v<%version%> && git push upstream v<%version%>

Then update the Debezium version referenced in the [Postgres Docker file](https://github.com/debezium/docker-images/blob/main/postgres/9.6/Dockerfile#L22)
and push that commit which will cause the image to be re-published on Docker Hub automatically.

## Reconfigure Docker Hub builds
If a new version of Docker images is going to be added it is necessary in Docker Hub build settings to

* add a new build for each image with new version
* remove builds for obsolete versions
* update `nightly` and `latest` build tags
* update rules the creates micro tags

## Close Jira issues
Close all issues relesed with this version. The affected issues can be found using JQL query
```
project=DBZ AND fixVersion=<VERSION> AND status='Resolved'
```
Then mark release in Jira as *Released* using `Release` action.

## Documentation post-release changes

After the release, the `antora.yml` file should be changed so that the `version` attribute references `master` once more.

## Update the documentation and blog

Update the documentation on the [Debezium website](https://debezium.io) by following the [instructions for changing the website](https://debezium.io/docs/contribute/#website).
This typically involves updating the documentation (look for pending pull requests tagged as "Merge after next release") and writing a blog post to announce the release.
Then, create a pull request with your changes and wait for a committer to approve and merge your changes.

When the blog post is available, use the [Debezium Twitter account](https://twitter.com/debezium) to announce the release by linking to the blog post.


# Automated Release

There are few manual steps to be completed before the execution:

* Verify the Maven Central [status](https://status.maven.org/) is green
* Update [the changelog](#update-the-changelog)
* Update [configuration](#reconfigure-docker-hub-builds) for Docker Hub builds

To perform a release automatically, invoke the Jenkins job on the release infrastructure. Two parameters are requested:

* `RELEASE_VERSION` - a version to be released in format x.y.z (e.g. `1.7.0.Beta1`)
* `DEVELOPMENT_VERSION` - next development version in format x.y.z-SNAPSHOT (e.g. `1.7.0-SNAPSHOT`)
