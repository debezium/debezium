# Debezium Actions

Debezium actions are designed to provide reuse for a series of steps that should be executed based on a series of input arguments.
When defining the build sequence for a Debezium module, an action should be created in the `actions` directory that follows the current convention of `build-debezium-xyz` where `xyz` identifies what is being built. 
In that directory, an `action.yml` should exist which defines a composite action that includes all the steps to take to build that specific module.

Given that actions cannot define matrix strategies, these are  managed at the workflow level.

# Debezium Workflows

Debezium workflows provide an orchestration of jobs to execute based  on a specific series of events. 
All workflows are defined in the `workflows` directory.
Debezium defines two types of workflows:

* Top-level workflows triggered by some GitHub event
* Callable workflows, used as reusable components across top-level workflows

## Top-level workflows

Debezium defines several top-level workflows, but there are two that define the testing workflow used when pull requests are opened or when a commit is pushed to the repository.

### Pull Requests

The workflow `debezium-workflow-pr.yml` defines the behavior to execute when a pull request is opened.
This top-level workflow is designed to use as many runners that are available for the organization. 

### Pushes

The workflow `debezium-workflow-push.yml` defines the behavior to execute when a commit is pushed to the repository.
This top-level workflow is designed to consumer fewer runners, maximizing those available to pull requests while also allowing the push tests to be completed in a reasonable period of time.

## Callable workflows

Callable workflows are typically defined as `connector-*-workflows.yml`, which defines a matrix setup for executing a specific connector's test suite based on a series of matrix parameters.
We use `workflow_call` workflows so that we can reuse the same matrix setup across multiple top-level workflows.

## Workflow structure

Within a top-level workflow, you will find jobs defined with two styles.
The style to use is determined based on whether the job requires the use of a matrix strategy.

If a job requires a matrix strategy, a callable reusable workflow should be created to define the matrix strategy, and then the top-level workflow should call into that callable workflow.
This guarantees that we have a single place to define the test strategy regardless if the workflow is used for pull requests or pushes to the repository.
You can see an example here:
```yaml
  build_mariadb:
    name: MariaDB
    needs: [ check_style, file_changes ]
    if: >
      ${{ needs.file_changes.outputs.common-changed == 'true' || 
      needs.file_changes.outputs.mariadb-changed == 'true' || 
      needs.file_changes.outputs.mariadb-ddl-parser-changed == 'true' || 
      needs.file_changes.outputs.schema-generator-changed == 'true' || 
      needs.file_changes.outputs.debezium-testing-changed == 'true' }}
    uses: ./.github/workflows/connector-mariadb-workflow.yml
    with:
      maven-cache-key: maven-debezium-test-build
```

If the job does not require a matrix strategy, then you would define the job with an inline step that uses a defined action from the `.github/actions` directory.
You can see an example here:

```yml
  build_sqlserver:
    name: SQL Server
    needs: [ check_style, file_changes ]
    runs-on: ubuntu-latest
    if: >
      ${{ needs.file_changes.outputs.common-changed == 'true' || 
      needs.file_changes.outputs.sqlserver-changed == 'true' || 
      needs.file_changes.outputs.schema-generator-changed == 'true' }}
    steps:
      - name: Checkout Action (Core)
        uses: actions/checkout@v4
      - uses: ./.github/actions/build-debezium-sqlserver
        with:
          maven-cache-key: maven-debezium-test-build-${{ hashFiles('**/pom.xml') }}
```

Conceptually, the idea is to drive as much down into a GitHub action in `.github/actions` as possible.
A `workflow_call` callable workflow should only be created when and if a job requires the use of a matrix strategy.

## When is `action/checkout` necessary?

If your workflow is calling another workflow using `workflow_call`, you do not need a checked out clone of the repository before calling the workflow.
GitHub automatically makes all workflows in the `.github/workflows` directory accessible without a check out.

The only time the repository must be checked out is if a step in the workflow calls a local action, `./.github/actions/xyz`.