# Debezium CI/CD Configuration

Debezium CI/CD environment consists of a set of Jenkins jobs. The jobs are configured and generated using [Jenkins Job Builder](https://docs.openstack.org/infra/jenkins-job-builder/) tool.

To generate jobs it is necessary to create a [personal access token](https://help.github.com/articles/creating-a-personal-access-token-for-the-command-line/) on GitHub account associated with Jenkins and use it as a password.

```
jenkins-jobs --conf config.ini -u <username> -p <GitHub Token> update .
```
