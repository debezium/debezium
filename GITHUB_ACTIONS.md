# Debezium GitHub Actions CI Architecture

Debezium utilizes GitHub Actions as its primary platform for Continuous Integration (CI) and automated validation.
The infrastructure is designed to handle the complexities of a large monorepo by employing a modular, "fan-out" architecture.
This approach ensures that only the tests relevant to a specific change are executed, optimizing resource usage and providing faster feedback to contributors.

## Workflow Architecture Layers

The CI system is partitioned into three functional layers: **Orchestration**, **Sanity & Compliance**, and **Functional Integration**.

### The Orchestration Layer
The Orchestration layer serves as the control plane for the CI system.
It is responsible for initial change detection and the conditional dispatch of downstream integration jobs to ensure that only affected modules are tested.

| Workflow File | Trigger | Significance |
|---|---|---|
| `debezium-workflow-pr.yml` | `Pull Request` | Acts as the entry point for all external contributions. It performs initial metadata checks and utilizes the `file_changes` job to calculate the testing scope, ensuring immediate feedback on PR validity. |
| `debezium-workflow-push.yml` | `Push` | Validates the state of the repository post-merge. It is responsible for refreshing global Maven dependency caches used by PR workflows, maintaining a consistent build environment across the project. |
| `file-changes-workflow.yml` | `Workflow Call` | Centralizes the "diff" logic for the monorepo. It maps modified file paths to module-specific flags (e.g., `mysql-changed`), preventing the redundant execution of tests for unaffected components. |

### Sanity and Compliance Layer
Before resource-intensive integration tests are provisioned, the pipeline executes lightweight verification jobs.
These checks ensure that the metadata and legal standing of the commits meet project standards.

| Check | Workflow File | Impact of Failure |
|---|---|---|
| Commit Message Format | `sanity-check.yml` | Enforces the `debezium/dbz#xxx` prefix required for automated changelog generation. A failure here prevents the PR from being merged. |
| DCO Sign-off | `debezium-workflow-pr.yml` | Validates the **Developer Certificate of Origin (DCO)**. This check is a prerequisite for the entire pipeline; if it fails, all downstream connector tests are skipped. |
| Contributor Metadata | `contributor-check.yml` | Verifies that the `COPYRIGHT.txt` and `Aliases.txt` files are properly updated to reflect new contributors. |
| Identity Integrity | `octocat-commits-check.yml` | Ensures the author email is linked to a valid GitHub account, maintaining accurate contribution metrics and accountability. |

> **Important**: Correcting Compliance Failures
> Compliance checks validate the commit objects themselves. If a check fails, you cannot resolve it with a follow-up commit. You must rewrite your local branch history (e.g., `git commit --amend` or `git rebase -i`) and perform a `git push --force` to update the Pull Request.

### Functional Integration Layer
Debezium utilizes a **Matrix Strategy** to validate connector stability across multiple environments.
Each connector build spins up real database instances using a combination of **Testcontainers** or the Maven build system to start container images.

* **Matrix Variants:** Tests cover multiple RDBMS versions (e.g., MySQL 8.0, 8.4, 9.1) and execution profiles (e.g., GTID-enabled, SSL-encrypted).
* **Parallelization:** Jobs are executed concurrently to minimize the time-to-feedback for complex connector suites.

#### Connector Matrix

The following matrix illustrates the typical test coverage for core connectors across different environments. 
*(Note: This is a simplified representation of the connector matrix. Actual configurations may vary across workflows.)*

```
+----------------------+--------------------+--------------------+
| Connector            | RDBMS Versions     | Profiles           |
+----------------------+--------------------+--------------------+
| MySQL                | 8.0, 8.4, 9.1      | GTID, SSL          |
+----------------------+--------------------+--------------------+
| PostgreSQL           | 12, 13, 14, 15, 16 | WAL2JSON, PGOUTPUT |
+----------------------+--------------------+--------------------+
| SQL Server           | 2017, 2019, 2022   | Standard, CDC      |
+----------------------+--------------------+--------------------+
| Oracle               | 19c, 21c           | LogMiner           |
+----------------------+--------------------+--------------------+
```

## The "Fan-Out" Execution Logic

To optimize throughput, the pipeline calculates a "Dependency Graph" for every PR to avoid building unaffected modules:

1. Analysis: The `file_changes` job analyzes the diff between the PR branch and the target branch.
2. Flagging:
  * If core modules (`debezium-util`, `debezium-connect-plugins`, or `debezium-connector-common`) are modified, **all** connector workflows are marked for execution as they depend on these base modules.
  * If a connector module is modified (e.g., `debezium-connector-postgres`), only its specific workflow is triggered.
3. Skipping: If only `documentation/` files are modified, the CI bypasses all Maven builds and triggers documentation-specific notifications.

## Specialized Verification Workflows

* **Java Quality Outreach (`jdk-outreach-workflow.yml`):** Runs daily builds against **Early Access (EA)** JDK releases to detect regressions in the JVM or library dependencies before they impact the stable release.
* **Apicurio Registry Compatibility (`apicurio-check-workflow.yml`):** Validates compatibility of change event schemas with the [Apicurio Registry](https://github.com/Apicurio/apicurio-registry) when using schema-based serialization formats (e.g., [Avro](https://debezium.io/documentation/reference/3.5/configuration/avro.html)).
* **Website Synchronization (`website-build-workflow.yml`):** Automates the deployment of updated documentation to the official project site upon merges to the documentation path.

## Pull Request Approval and Merging

The successful completion of the CI pipeline is a prerequisite for merging.
Debezium follows a structured review and voting process to ensure code quality.

For a detailed description of the pull request approval process, voting requirements, and merging criteria, please refer to the [Debezium Governance](https://github.com/debezium/governance/blob/main/GOVERNANCE.md#pull-requests) document.

Additionally, all contributors should refer to the [Contributing Guide](https://github.com/debezium/debezium/blob/main/CONTRIBUTING.md) for information on repository standards and development practices.

## Troubleshooting Failures

### Checkstyle
Violations in `check_style` indicate code formatting errors.
Correct these locally by running:

```bash
./mvnw process-sources
```

### Flaky Tests
Integration tests depend on Docker and network stability. If a failure appears transient, check if the failing test is annotated with `@Flaky` in the source code. These annotations typically reference a known issue that documents the instability.

> **Note**: Debezium has migrated from JIRA to GitHub Issues. Older legacy issues are marked using a JIRA ID (e.g., `DBZ-xxxx`), while newer issues use the GitHub issue format (`dbz#xxxx`).

To identify if a failure is related to a known issue, you can search the codebase for the issue key using the following command:

```bash
grep -rni "@Flaky"
```

### Logs
Detailed logs are available in the Actions console.
Scroll to the `BUILD FAILURE` section in the Maven output to identify the specific unit or integration test failure.

When a build fails, the Maven output provides detailed error messages. Look for the `[ERROR]` prefix to identify the cause of the failure:

```
[ERROR] Failed to execute goal org.apache.maven.plugins:maven-surefire-plugin:3.0.0-M5:test (default-test) on project debezium-connector-mysql: There are test failures.

[ERROR] Please refer to the `target/surefire-reports` directory for the individual test results.
[ERROR] Please refer to dump files (if any exist) [date].dump, [date]-jvmRun[N].dump and [date].dumpstream.
```
