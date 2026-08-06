#!/usr/bin/env python3
"""
Process Debezium versions and generate build information for GitHub Actions.

This script reads version tags from stdin, parses them, groups by streams,
and outputs the information needed for building container images.
"""

import json
import sys
import argparse
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from collections import defaultdict


@dataclass
class Version:
    """Represents a semantic version."""
    major: int
    minor: int
    micro: int
    classifier: Optional[str]
    
    @classmethod
    def from_string(cls, version_str: str) -> 'Version':
        """
        Parse version string (e.g., 'v2.7.3.Final' or '2.7.3.Final').
        
        Args:
            version_str: Version string to parse
            
        Returns:
            Version object
            
        Raises:
            ValueError: If version string is invalid
        """
        # Remove 'v' prefix if present
        version_str = version_str.lstrip('v')
        parts = version_str.split('.')
        
        if len(parts) < 3:
            raise ValueError(f"Invalid version format: {version_str}")
        
        major = int(parts[0])
        minor = int(parts[1])
        micro = int(parts[2])
        classifier = parts[3] if len(parts) > 3 else None
        
        return cls(major, minor, micro, classifier)
    
    @property
    def stream(self) -> str:
        """Return major.minor stream."""
        return f"{self.major}.{self.minor}"
    
    def __str__(self) -> str:
        """String representation."""
        result = f"{self.major}.{self.minor}.{self.micro}"
        if self.classifier:
            result += f".{self.classifier}"
        return result
    
    def __lt__(self, other: 'Version') -> bool:
        """Compare versions for sorting."""
        if self.major != other.major:
            return self.major < other.major
        if self.minor != other.minor:
            return self.minor < other.minor
        if self.micro != other.micro:
            return self.micro < other.micro
        # Handle classifier comparison
        if self.classifier is None and other.classifier is None:
            return False
        if self.classifier is None:
            return False  # No classifier is "greater" (more stable)
        if other.classifier is None:
            return True
        return self.classifier < other.classifier
    
    def __eq__(self, other: object) -> bool:
        """Check equality."""
        if not isinstance(other, Version):
            return False
        return (self.major == other.major and 
                self.minor == other.minor and 
                self.micro == other.micro and 
                self.classifier == other.classifier)
    
    def __hash__(self) -> int:
        """Hash for use in sets/dicts."""
        return hash((self.major, self.minor, self.micro, self.classifier))


def process_versions(
    tags: List[str],
    streams_count: int,
    tags_per_stream: int
) -> Tuple[List[str], Optional[str], List[Dict[str, str]]]:
    """
    Process version tags and generate build information.
    
    Args:
        tags: List of version tag strings
        streams_count: Number of most recent streams to build
        tags_per_stream: Number of most recent tags per stream to build
    
    Returns:
        Tuple of (streams_list, stable_stream, build_list)
        - streams_list: List of stream identifiers (e.g., ['2.7', '2.6'])
        - stable_stream: Most recent stream with 'Final' classifier, or None
        - build_list: List of dicts with 'stream' and 'tag' keys
    """
    # Parse all versions
    versions = []
    for tag in tags:
        try:
            version = Version.from_string(tag)
            versions.append(version)
        except (ValueError, IndexError) as e:
            # Skip invalid version tags
            print(f"Warning: Skipping invalid tag '{tag}': {e}", file=sys.stderr)
            continue
    
    if not versions:
        print("Error: No valid versions found", file=sys.stderr)
        return [], None, []
    
    # Group by stream
    streams: Dict[str, List[Version]] = defaultdict(list)
    for version in versions:
        streams[version.stream].append(version)
    
    # Sort versions within each stream (newest first)
    for stream in streams:
        streams[stream].sort(reverse=True)
    
    # Get most recent streams
    sorted_streams = sorted(streams.keys(), reverse=True, 
                          key=lambda s: tuple(map(int, s.split('.'))))
    streams_to_build = sorted_streams[:streams_count]
    
    # Find stable stream (most recent with 'Final' classifier)
    stable_stream = None
    for stream in streams_to_build:
        if streams[stream] and streams[stream][0].classifier == 'Final':
            stable_stream = stream
            break
    
    # Generate build list
    build_list = []
    for stream in streams_to_build:
        for version in streams[stream][:tags_per_stream]:
            build_list.append({
                'stream': stream,
                'tag': str(version)
            })
    
    return streams_to_build, stable_stream, build_list


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Process Debezium versions for GitHub Actions',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
  gh api repos/debezium/debezium/tags --paginate | \\
    jq -r '.[].name' | \\
    grep -E '^v[0-9]+\\.[0-9]+\\.[0-9]+' | \\
    python3 process-versions.py --streams-count 2 --tags-per-stream 1
        """
    )
    parser.add_argument(
        '--streams-count',
        type=int,
        default=2,
        help='Number of recent streams to build (default: 2)'
    )
    parser.add_argument(
        '--tags-per-stream',
        type=int,
        default=1,
        help='Number of tags per stream to build (default: 1)'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.streams_count < 1:
        print("Error: streams-count must be at least 1", file=sys.stderr)
        return 1
    if args.tags_per_stream < 1:
        print("Error: tags-per-stream must be at least 1", file=sys.stderr)
        return 1
    
    # Read tags from stdin
    tags = [line.strip() for line in sys.stdin if line.strip()]
    
    if not tags:
        print("Error: No tags provided on stdin", file=sys.stderr)
        return 1
    
    # Process versions
    streams, stable_stream, build_list = process_versions(
        tags,
        args.streams_count,
        args.tags_per_stream
    )
    
    if not streams:
        print("Error: No valid streams found", file=sys.stderr)
        return 1
    
    # Output for GitHub Actions (using GITHUB_OUTPUT format)
    print(f"streams={' '.join(streams)}")
    print(f"stable_stream={stable_stream or ''}")
    print(f"build_list={json.dumps(build_list)}")
    
    # Also output summary to stderr for debugging
    print(f"\nProcessed {len(tags)} tags", file=sys.stderr)
    print(f"Found {len(streams)} streams to build: {', '.join(streams)}", file=sys.stderr)
    print(f"Stable stream: {stable_stream or 'None'}", file=sys.stderr)
    print(f"Total builds: {len(build_list)}", file=sys.stderr)
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
