#!/usr/bin/env python3
"""
Generates a manifest.json for the published descriptors.

The manifest contains build metadata plus a catalog of every generated
component descriptor, scanned from the descriptors output directory.
"""
import json
import os
from pathlib import Path


def build_components(output_dir: Path) -> dict:
    """Scan each component-type subdirectory and collect its descriptors."""
    components = {}
    for component_dir in sorted(p for p in output_dir.iterdir() if p.is_dir()):
        component_type = component_dir.name
        entries = []
        for descriptor in sorted(component_dir.glob("*.json")):
            try:
                data = json.loads(descriptor.read_text(encoding="utf-8"))
            except (ValueError, OSError) as e:
                print(f"  - WARNING: failed to parse {descriptor.name}: {e}")
                continue
            entries.append({
                "class": descriptor.stem,
                "name": data.get("name") or "",
                "description": (data.get("metadata") or {}).get("description") or "",
                "descriptor": f"{component_type}/{descriptor.name}",
            })
        if entries:
            components[component_type] = entries
            print(f"Added {len(entries)} items for {component_type}")
    return components


def main():
    output_dir = Path(os.environ["DESCRIPTORS_OUTPUT_DIR"])
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "schemaVersion": "1.0",
        "build": {
            "version": os.environ["SNAPSHOT_VERSION"],
            "timestamp": os.environ["BUILD_TIMESTAMP"],
            "sourceRepository": os.environ.get("CORE_REPOSITORY", "debezium/debezium"),
            "sourceCommit": os.environ["DEBEZIUM_COMMIT"],
            "sourceBranch": os.environ["DEBEZIUM_BRANCH"],
        },
        "components": build_components(output_dir),
    }

    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"Generated manifest with {len(manifest['components'])} component types")


if __name__ == "__main__":
    main()
