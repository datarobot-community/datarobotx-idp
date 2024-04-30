#!/bin/bash
files="$1"
fix_param="$2"
file_format="py"


if [ "$fix_param" = "--fix" ]; then
    echo "🪛 Fixing $file_format files..."
    # Run the linter to fix the files
    ./linters/run_linter.sh  "black --verbose" "$file_format" "$files"
    ./linters/run_linter.sh  "isort" "$file_format" "$files"
else
    # Run the linter
    echo "🔬 Checking $file_format files..."
    ./linters/run_linter.sh  "black --check" "$file_format" "$files"
    ./linters/run_linter.sh  "isort --check" "$file_format" "$files"
fi
