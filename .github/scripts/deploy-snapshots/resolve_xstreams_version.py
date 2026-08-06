#!/usr/bin/env python3
"""
Resolves the Oracle xstreams artifact version from the core repository pom.xml.
"""
import os
import re
from pathlib import Path


def main():
    core_dir = os.environ["CORE_DIR"]
    pom = Path(core_dir, "pom.xml").read_text(encoding="utf-8")
    
    match = re.search(
        r"<version.oracle.driver>(.+)</version.oracle.driver>",
        pom,
        re.MULTILINE | re.DOTALL,
    )
    
    if not match:
        raise SystemExit("Unable to resolve version.oracle.driver from pom.xml")
    
    print(match.group(1).strip())


if __name__ == "__main__":
    main()
