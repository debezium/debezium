# Purpose

The scripts in this directory allow auto-generation of GitHub Actions workflows in the `.github/workflows` directory, which are used to perform various tasks when code is pushed or pull requests are opened against this repository.
Each script is designed to use an input template found in the same directory and to output the final YAML descriptor in the `.github/workflows` directory.
A description about each script can be found below.

# Requirements

The GitHub Actions workflows are generated using YAML templates and in order to do this [YTT](https://get-ytt.io/) must be installed.
Installation instructions for YTT can be found [here](https://k14s.io/#install-from-github-release).

# Data files

The `jobs.yml` file acts as a data source used by the YTT templates to know what CI jobs are to be executed by an action's workflow.
The layout of this file is as follows:

```
#@data/values
---
jobs:
- key: sqlserver
  name: "SQL Server"
  maven: "mvn ..."
- key: mysql
  name: "MySQL"
  maven: "mvn ..."
```

Each GitHub Action job has a 1:1 mapping to an entry in the `jobs` array.
Each entry in this array maintains a series of key/value pairs which are described below:

|Key|Description|
|---|---|
|key|Unique ID for each job task in a workflow that adheres to YAML key naming conventions.|
|name|The descriptive name for the job task, often used in the job step's name.  For example, `"PostgreSQL Pgoutput"`.|
|maven|The full maven command line that should be invoked to perform the job build task.|

# OpenJDK matrix workflow

This script is designed to generate a test matrix of jobs that build Debezium against a number of OpenJDK versions.
The generated workflow is scheduled to run once a day.

Below is the template and output YAML descriptor for GitHub Actions used by this script:

|---|---|
|Script|`jdk-matrix.sh`|
|Template|`jdk-matrix.yml`|
|Output|`.github/workflows/jdk-outreach-workflow.yml`|

# Changing jobs or the templates

1. Modify `jobs.yml` to add/update/remove the necessary jobs or modify the workflow templates accordingly.
2. Rerun the appropriate shell script(s) to regenerate the workflows in `.github/workflows`.
3. Commit the changes and send a PR.