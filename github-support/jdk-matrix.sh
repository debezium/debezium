# This script generates a JDK matrix workflow.
# This workflow is based on the template jdk-matrix.yml and uses jobs.yml as the data-source for data-driven jobs.
# The output of this script is automatically written to the .github/workflows/jdk-outreach-workflow.yml file.
# When changes are made to the jobs.yml or jdk-matrix.yml template, this script should be used to regenerate the GitHub Actions workflow prior to being committed.
ytt -f ./jdk-matrix.yml -f ./jobs.yml > ../.github/workflows/jdk-outreach-workflow.yml