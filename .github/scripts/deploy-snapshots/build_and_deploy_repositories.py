#!/usr/bin/env python3
"""
Builds and deploys Debezium repositories to Maven Central.
"""
import json
import os
import subprocess


def main():
    repositories = json.loads(os.environ["REPOSITORIES_JSON"])
    workspace = os.environ["GITHUB_WORKSPACE"]
    descriptors_output_dir = os.environ["DESCRIPTORS_OUTPUT_DIR"]
    home = os.environ["HOME"]
    core_id = os.environ["CORE_ID"]

    for repository in repositories:
        repo_dir = os.path.join(workspace, repository["id"], repository["subdir"])
        install_script = os.path.join(repo_dir, "install-artifacts.sh")

        if os.path.isfile(install_script):
            subprocess.run(["bash", install_script], cwd=repo_dir, check=True)

        command = [
            os.path.join(repo_dir, "mvnw"),
            "-B",
            "-ntp",
            "clean",
            "deploy",
            "-U",
            "-s",
            f"{home}/.m2/settings-snapshots.xml",
            "-DdeployAtEnd=true",
            "-Dpublish.skip=false",
            "-DskipITs",
            "-DskipTests",
            "-Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn",
            "-Dmaven.wagon.http.pool=false",
            "-Dmaven.wagon.httpconnectionManager.ttlSeconds=120",
            "-Dmaven.wagon.rto=20000",
            "-Dmaven.wagon.http.retryHandler.count=1",
            "-Dmaven.wagon.http.serviceUnavailableRetryStrategy.retryInterval=5000",
            f"-Dschema.generator.output.dir={descriptors_output_dir}",
        ]

        if repository["id"] == core_id:
            command.append("-Passembly,oracle-all,docs")
        elif repository["id"] != "jbang-catalog":
            command.append("-Passembly,docs")

        subprocess.run(command, cwd=repo_dir, check=True)


if __name__ == "__main__":
    main()
