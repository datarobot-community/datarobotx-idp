#!/bin/bash
files="$1"
fix_param="$2"
file_format="py"


if [ "$fix_param" = "--fix" ]; then
    echo "🪛 Fixing $file_format files..."
    # Run the linter to fix the files
    ./linters/run_linter.sh  "ruff --fix" "$file_format" "$files"
    ./linters/run_linter.sh  "ruff format" "$file_format" "$files"
    # In case if you need black or isort
    # ./linters/run_linter.sh  "black --verbose" "$file_format" "$files"
    # ./linters/run_linter.sh  "isort" "$file_format" "$files"

else
    # Run the linter
    echo "🔬 Checking $file_format files..."
    ./linters/run_linter.sh  "ruff check" "$file_format" "$files"
    ./linters/run_linter.sh  "ruff format --check" "$file_format" "$files"
    ./linters/run_linter.sh  "MYPYPATH=src mypy --namespace-packages --explicit-package-bases --strict" "" "."
    # In case if you need black or isort
    # ./linters/run_linter.sh  "black --check" "$file_format" "$files"
    # ./linters/run_linter.sh  "isort --check" "$file_format" "$files"
fi
