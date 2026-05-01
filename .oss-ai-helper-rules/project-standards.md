# Project Standards

This rule file contains build tools, commands, and code style constraints for the project. Commands read this file to determine how to build, test, and format code.

- **Build tool:** Maven
- **Build command:** `./mvnw clean install`
- **Test command:** `./mvnw clean install`
- **Format command:** `./mvnw process-sources`
- **Module-specific build:** yes (always run `./mvnw` in the module directory where changes occurred)
- **Parallelized Maven:** no (resource intensive, do NOT parallelize Maven jobs)
- **Code style restrictions:** 
  - Do NOT change public API signatures without justification
  - Do NOT add new dependencies without justification
  - Do NOT use Lombok and Guava (unless already present in the file)
  - Ensure proper license header in files are present
  - Maintain backwards compatibility for public APIs
  - Checkstyle formatter rules (in `support/checkstyle/src/main/resources/checkstyle.xml`) must be followed and will be applied automatically during build or manually (see next item) 
  - Use `./mvnw process-sources` to auto-format code
  - Code style violations will fail CI
  - Commits MUST be signed off (`git commit -s`) and all code changes MUST be reviewed by a human user, understood by that human user and MUST explicitly be confirmed by that human user before committing
  - Ensure inclusive language is used everywhere, eg. allowlist/blocklist, primary/replica, placeholder/example, main branch, conflict-free, concurrent/parallel

## Version
1.0.0
