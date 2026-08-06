#!/usr/bin/env python3

import argparse
import json
from pathlib import Path


def parse_repository(entry: str) -> dict[str, str]:
    parts = [part.strip() for part in entry.split("#")]
    if len(parts) == 3:
        repo_id, repository, branch = parts
        subdir = "."
    elif len(parts) == 4:
        repo_id, repository, subdir, branch = parts
    else:
        raise ValueError(
            f"Invalid repository entry '{entry}'. Expected id#repo#branch or id#repo#subdir#branch."
        )

    if not repo_id or not repository or not branch:
        raise ValueError(f"Invalid repository entry '{entry}'. Empty values are not allowed.")

    return {
        "id": repo_id,
        "repository": repository,
        "subdir": subdir,
        "branch": branch,
    }


def parse_repositories(raw_repositories: str) -> list[dict[str, str]]:
    return [parse_repository(entry) for entry in raw_repositories.split() if entry.strip()]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repositories", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    repositories = parse_repositories(args.repositories)
    Path(args.output).write_text(json.dumps(repositories, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
