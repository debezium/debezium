#!/usr/bin/env python3
"""
Extracts and outputs core repository metadata to GitHub Actions output.
"""
import json
import os


def main():
    core_repository = json.loads(os.environ["CORE_REPOSITORY_JSON"])
    github_output = os.environ["GITHUB_OUTPUT"]

    with open(github_output, "a", encoding="utf-8") as output:
        output.write(f"core_id={core_repository['id']}\n")
        output.write(f"core_repository={core_repository['repository']}\n")
        output.write(f"core_branch={core_repository['branch']}\n")
        output.write(f"core_subdir={core_repository['subdir']}\n")


if __name__ == "__main__":
    main()
