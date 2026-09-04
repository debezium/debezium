#!/usr/bin/env python3
"""
Generates a manifest.json file with build metadata for the descriptors.
"""
import json
import os
from pathlib import Path


def main():
    output_dir = Path(os.environ["DESCRIPTORS_OUTPUT_DIR"])
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "debezium_commit": os.environ["DEBEZIUM_COMMIT"],
        "debezium_branch": os.environ["DEBEZIUM_BRANCH"],
        "build_timestamp": os.environ["BUILD_TIMESTAMP"],
        "snapshot_version": os.environ["SNAPSHOT_VERSION"],
    }

    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
