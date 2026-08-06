#!/usr/bin/env python3
"""
Clones Debezium repositories based on the parsed repositories JSON.
"""
import json
import os
import subprocess


def main():
    repositories = json.loads(os.environ["REPOSITORIES_JSON"])
    workspace = os.environ["GITHUB_WORKSPACE"]
    token = os.environ["GH_TOKEN"]

    for repository in repositories:
        subprocess.run(
            [
                "git",
                "clone",
                "--depth",
                "1",
                "--branch",
                repository["branch"],
                f"https://x-access-token:{token}@github.com/{repository['repository']}.git",
                os.path.join(workspace, repository["id"]),
            ],
            check=True,
        )


if __name__ == "__main__":
    main()
