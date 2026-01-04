#!/usr/bin/env python3
"""
Concatenate all files in a directory and subdirectories into a single file.
"""

import argparse
import os
import fnmatch
from pathlib import Path


def should_exclude(path: Path, exclude_dirs: list[str], exclude_patterns: list[str], base_dir: Path) -> bool:
    """Check if a path should be excluded based on directories or patterns."""
    rel_path = path.relative_to(base_dir)

    # Check if any parent directory is in exclude list
    for part in rel_path.parts[:-1] if path.is_file() else rel_path.parts:
        if part in exclude_dirs:
            return True

    # Check file patterns
    if path.is_file():
        for pattern in exclude_patterns:
            if fnmatch.fnmatch(path.name, pattern):
                return True

    return False


def is_text_file(file_path: Path) -> bool:
    """Check if a file is a text file by attempting to read it."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            f.read(1024)
        return True
    except (UnicodeDecodeError, PermissionError):
        return False


def collect_files(directory: Path, exclude_dirs: list[str], exclude_patterns: list[str]) -> list[Path]:
    """Collect all text files in directory, respecting exclusions."""
    files = []

    for root, dirs, filenames in os.walk(directory):
        root_path = Path(root)

        # Filter out excluded directories (modifying dirs in-place prevents os.walk from descending)
        dirs[:] = [d for d in dirs if d not in exclude_dirs]

        for filename in sorted(filenames):
            file_path = root_path / filename

            if should_exclude(file_path, exclude_dirs, exclude_patterns, directory):
                continue

            if is_text_file(file_path):
                files.append(file_path)

    return sorted(files)


def combine_files(directory: Path, output_file: Path, exclude_dirs: list[str], exclude_patterns: list[str]) -> int:
    """Combine all files into a single output file."""
    files = collect_files(directory, exclude_dirs, exclude_patterns)

    with open(output_file, 'w', encoding='utf-8') as out:
        for i, file_path in enumerate(files):
            rel_path = file_path.relative_to(directory)

            # Write separator
            out.write(f"===== FILE: {rel_path} =====\n")

            # Write file contents
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    out.write(content)
                    # Ensure there's a newline before the next separator
                    if content and not content.endswith('\n'):
                        out.write('\n')
            except Exception as e:
                print(f"Warning: Could not read {rel_path}: {e}")
                continue

    return len(files)


def main():
    parser = argparse.ArgumentParser(
        description='Concatenate all files in a directory into a single file.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  %(prog)s /path/to/project -o combined.txt
  %(prog)s . --exclude-dirs node_modules .git --exclude-patterns "*.pyc" "*.log"
        '''
    )

    parser.add_argument(
        'directory',
        type=str,
        help='Directory to process'
    )

    parser.add_argument(
        '-o', '--output',
        type=str,
        default='combined_output.txt',
        help='Output file name (default: combined_output.txt)'
    )

    parser.add_argument(
        '--exclude-dirs',
        nargs='*',
        default=[],
        help='Subdirectories to ignore (e.g., node_modules .git)'
    )

    parser.add_argument(
        '--exclude-patterns',
        nargs='*',
        default=[],
        help='File patterns to ignore (e.g., "*.pyc" "*.log")'
    )

    args = parser.parse_args()

    directory = Path(args.directory).resolve()
    output_file = Path(args.output).resolve()

    if not directory.is_dir():
        print(f"Error: '{directory}' is not a valid directory")
        return 1

    # Prevent output file from being inside the source directory (would cause recursion issues)
    try:
        output_file.relative_to(directory)
        # If we get here, output is inside directory - add it to excludes
        args.exclude_patterns.append(output_file.name)
    except ValueError:
        pass  # Output is outside directory, no problem

    print(f"Processing directory: {directory}")
    print(f"Output file: {output_file}")

    if args.exclude_dirs:
        print(f"Excluding directories: {', '.join(args.exclude_dirs)}")
    if args.exclude_patterns:
        print(f"Excluding patterns: {', '.join(args.exclude_patterns)}")

    file_count = combine_files(directory, output_file, args.exclude_dirs, args.exclude_patterns)

    print(f"Combined {file_count} files into {output_file}")
    return 0


if __name__ == '__main__':
    exit(main())
