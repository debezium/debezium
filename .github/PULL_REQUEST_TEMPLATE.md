### Notify
cc @stripe-internal/online-db-cdc

<!--
Assign codeowner reviewers by commenting `r?` on a single line. See go/code-review for more guidance on the code review process.

If you'd like to get an extra review from a language expert, add `r? @stripe-internal/go-tutors` to assign one!

More information on extra language reviews at go/go-language-reviews.
-->

### Summary
<!-- What does the code do? What have you changed? If this is a command-line utility change, consider including before / after output. -->

### Motivation
<!-- Why are you making this change? This can be a link to a Jira task. -->

### Test plan
<!-- How did you test this change? What were you unable to test? Please include additional context, e.g. were you able to cover failures and edge cases? Reference automated tests or describe a manual test plan and confirm the outcome. Please keep the frontpage test, our auditors (who review a random sample of our PRs), and your reviewer in mind. In cases where you are unable to test your changes, or it is not appropriate, please leave both boxes unchecked. -->

- [ ] All changes in PR are covered by tests
- [ ] Failures and edge cases tested

### Rollout/revert plan
<!--
  What services must be deployed as part of this change? Tag these services (or comment `s: example-srv` to have CIBot tag them) so that they are autodeployed.

  If there any post-deploy steps (e.g. run migration or enable feature flag), or manual/unusual deploy processes required, list them here.
  Is this change safe to roll back, and are there additional steps that need to be taken during rollback?

  Changes in the critical payments path must be decoupled from the deploy (eg: by a feature flag or gate).
  See go/code-reviewer-guide and go/safe-payflows-rollouts for details.
-->
Safe to revert unless specified otherwise

### Monitoring plan
<!--
  When this PR is deployed, what monitoring (metrics, logs, etc.) or validation is required to evaluate whether it is working as intended? If automated healthchecks exist, are they sufficient?
  Consider adding additional logging or metrics to your PR if it's not easy to answer this question.
-->
