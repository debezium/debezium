#!/usr/bin/env python3
"""
Validates that the core repository exists in the parsed repositories JSON.
Extracts the core repository information to a separate file.
"""
import json
import os
import sys
from pathlib import Path


def main():
    runner_temp = os.environ["RUNNER_TEMP"]
    repositories_file = Path(runner_temp, "debezium-repositories.json")
    core_output_file = Path(runner_temp, "core-repository.json")

    repositories = json.loads(repositories_file.read_text(encoding="utf-8"))
    core_repository = next(
        (repository for repository in repositories if repository["id"] == "core"),
        None,
    )

    if core_repository is None:
        print("ERROR: The debezium-repositories setting must include a 'core' entry.", file=sys.stderr)
        sys.exit(1)

    core_output_file.write_text(
        json.dumps(core_repository, indent=2) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
