# Releasing Debezium

The Debezium project uses Maven for its build system, relying up on the _release_ plugin to most of the work. This document describes the steps required to perform a release.

## Start with the correct branch

Make sure that you are on the correct branch that is to be released, and that your local Git repository has all of the most recent commits. For example, to release from the `master` branch on the remote repository named `upstream`:

    $ git checkout master
    $ git pull upstream master

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

At this point, the staging repository contains all the artifacts for the release, and we need to check that they are valid. Only then can we release the artfifacts.

## Releasing the artifacts

Go to Sonotype's Nexus Repository Manager at https://oss.sonatype.org/ and log in with an account that has privilege to release Debezium artifacts. In the left hand panel, click on the "Staging Repositories". Then type "debezium" in the search box to locate the recently-closed staging repository.

Select the staging repository (by checking the box) and press the "Release" button. Enter a description in the dialog box, and press "OK" to release the artifacts to Maven Central. You may need to press the "Refresh" button a few times until the staging repository disappears.

It may take some time for the artifacts to actually be [visible in Maven Central](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22io.debezium%22).

## Update the Docker images

Once the artifacts are available on Maven Central, check out and get the latest commits from the [Debezium Docker images](https://github.com/debezium/docker-images) repository:

    $ git pull upstream

Create a topic branch and add new Docker images that use the latest release. For major and minor releases, be sure to create new versions of the images rather than update old images.

When you're changes are complete, simply create a pull-request to the [Debezium Docker images](https://github.com/debezium/docker-images) repository as usual. When that pull request is merged Docker Hub will automatically build the images and make them available on our [Debezium Docker Hub](https://hub.docker.com/r/debezium/).

## Update the documentation and blog

Update the documentation on the [Debezium website](http://debezium.io) by following the [instructions for changing the website](http://debezium.io/docs/contribute/#website). This typically involes updating the documentation and writing a blog post to announce the release. Then, create a pull request with your changes and wait for a committer to approve and merge your changes.

When the blog post is available, use the [Debezium Twitter account](https://twitter.com/debezium) to announce the release by linking to the blog post.