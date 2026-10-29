# Project Guidelines

This rule file contains branching, commit, PR, and task-finding conventions for the project. Commands read this file to determine how to name branches, format commits, and search for tasks.

Debezium does not provide any way to create a quick fix branch and quick-fix commits.
There are exceptions for docs, CI fixes and releases. 

- **Fix-issue branch:** `debezium/dbz#<ISSUE_NUMBER>-<short-slug>`
- **Quick-fix branch:** NOT ALLOWED!
- **SonarCloud branch:** _(not configured)_
- **Commit format (fix-issue):** `debezium/dbz#<ISSUE_NUMBER> <brief description>`
- **Commit format (quick-fix):** NOT ALLOWED!
- **Commit format (docs):** `[docs] <brief description>`
- **Commit format (release):** `[release] <brief description>`
- **CI-fix branch:** `ci/debezium/dbz#<ISSUE_NUMBER>-<short-slug>`
- **Commit format (ci-fix):** `[ci] <brief description>`
- **PR creation:** always
- **Find-task source:** GitHub labels
- **Find-task beginner label:** `good first issue`
- **Find-task experienced label:** _(none)_
- **Find-task intermediate:** _(none)_
- **Find-task repo:** `debezium/dbz` (issues are tracked in a separate repo)
- **Scope-too-large redirect:** `/oss-create-issue`

## Version
1.0.0
