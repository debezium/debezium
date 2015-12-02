## Get the code

The easiest way to get started with the code is to [create your own fork](http://help.github.com/forking/) of this repository, and then clone your fork:

	$ git clone git@github.com:<you>/debezium.git
	$ cd debezium
	$ git remote add upstream git://github.com/debezium/debezium.git
	
At any time, you can pull changes from the upstream and merge them onto your master:

	$ git checkout master                    # switches to the 'master' branch
	$ git pull upstream master               # fetches all 'upstream' changes and merges 'upstream/master' onto your 'master' branch
    $ git branch -u upstream/master master   # makes your local 'master' branch track the 'upstream/master' branch

The general idea is to keep your local `master` branch in-sync with the `upstream/master`. You don't have to keep your fork's `master` branch updated, since you will use your fork primarily for topic branches and creating pull requests (see below). But if you do want to update your fork's `master` branch, you can always do this with:

	$ git push origin                   # pushes all the updates to your fork, which should be in-sync with 'upstream'


## Building Debezium

The following command compiles all the code, installs the JARs into your local Maven repository, and run all of the unit and integration tests:

	$ mvn clean install

## Contribute fixes and features

Debezium is licensed under the Apache License 2.0 (see the file LICENSE.txt or [http://www.apache.org/licenses/LICENSE-2.0.html]() for details). We welcome any contributions you want to make to Debezium, but they must be completely authored by you, and you must have the right to contribute them. All contributions to Debezium must be licensed under the Apache License 2.0, just like the project itself.

If you want to fix a bug or make any changes, please log an issue in the [Debezium JIRA](https://issues.jboss.org/browse/DBZ) describing the bug or new feature. Then we highly recommend making the changes on a topic branch named with the JIRA issue number. For example, this command creates a branch for the DBZ-1234 issue:

	$ git checkout -b DBZ-1234

Then make your changes on this branch, and ensure that the entire branch builds with `mvn clean install`. Commit your changes to your branch using reall good comments. You might check for recent changes made in the official repository:

	$ git checkout master               # switches to the 'master' branch
	$ git pull upstream master          # fetches all 'upstream' changes and merges 'upstream/master' onto your 'master' branch
	$ git checkout DBZ-1234             # switches to your topic branch
	$ git rebase master                 # reapplies your changes on top of the latest in master
	                                    # (i.e., the latest from master will be the new base for your changes)

If the pull grabbed a lot of changes, you should rerun your build to make sure your changes are still good.
You can then push your topic branch and its changes into your public fork repository:

	$ git push origin DBZ-1234          # pushes your topic branch into your public fork of Debezium

and [generate a pull-request](http://help.github.com/pull-requests/) for your changes. Your pull request will generate a build on our integration server, and the results will be posted on the pull request page. The Debezium committers will also be notified of your pull request and, if your pull request passes all tests, will review your changes. If they have any questions or suggestions, they will add comments and/or line notes. Otherwise, if the changes are acceptable, they'll merge your pull request into the official codebase and close the JIRA issue.
