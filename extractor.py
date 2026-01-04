#!/usr/bin/env python3
"""
Extract files from a concatenated file back into their original directory structure.
"""

import argparse
import re
from pathlib import Path


SEPARATOR_PATTERN = re.compile(r'^===== FILE: (.+) =====$')


def extract_files(input_file: Path, output_dir: Path) -> int:
    """Extract all files from the combined file into the output directory."""
    file_count = 0
    current_file_path = None
    current_content_lines = []

    def write_current_file():
        """Write the accumulated content to the current file."""
        nonlocal file_count
        if current_file_path is None:
            return

        full_path = output_dir / current_file_path

        # Create parent directories if needed
        full_path.parent.mkdir(parents=True, exist_ok=True)

        # Join content and remove trailing newline that was added by combiner
        content = ''.join(current_content_lines)
        if content.endswith('\n') and current_content_lines:
            # Check if original file likely didn't have trailing newline
            # We can't know for sure, so we keep the newline (safer default)
            pass

        with open(full_path, 'w', encoding='utf-8') as f:
            f.write(content)

        file_count += 1
        print(f"Extracted: {current_file_path}")

    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            match = SEPARATOR_PATTERN.match(line.rstrip('\n'))

            if match:
                # Write previous file if exists
                write_current_file()

                # Start new file
                current_file_path = Path(match.group(1))
                current_content_lines = []
            else:
                # Accumulate content for current file
                if current_file_path is not None:
                    current_content_lines.append(line)

    # Write the last file
    write_current_file()

    return file_count


def main():
    parser = argparse.ArgumentParser(
        description='Extract files from a concatenated file back into directory structure.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  %(prog)s combined.txt -o /path/to/output
  %(prog)s combined.txt  # Extracts to current directory
        '''
    )

    parser.add_argument(
        'input_file',
        type=str,
        help='Combined file to extract'
    )

    parser.add_argument(
        '-o', '--output-dir',
        type=str,
        default='.',
        help='Output directory (default: current directory)'
    )

    args = parser.parse_args()

    input_file = Path(args.input_file).resolve()
    output_dir = Path(args.output_dir).resolve()

    if not input_file.is_file():
        print(f"Error: '{input_file}' is not a valid file")
        return 1

    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Extracting from: {input_file}")
    print(f"Output directory: {output_dir}")

    file_count = extract_files(input_file, output_dir)

    print(f"Extracted {file_count} files to {output_dir}")
    return 0


if __name__ == '__main__':
    exit(main())
