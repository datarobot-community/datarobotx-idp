#!/bin/bash

files="$1"
fix_param="$2"
file_format="yaml,yml"

if [ "$fix_param" = "--fix" ]; then
    echo "🪛 Fixing $file_format files..."
    # Run the linter to fix the files
    ./linters/run_linter.sh  "yamlfix" "$file_format"
else
    # Run the linter
    echo "🔬 Checking $file_format files..."
    ./linters/run_linter.sh  "yamlfix --check" "$file_format" "$files"
fi
