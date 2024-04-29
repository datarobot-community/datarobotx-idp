#!/bin/bash

RED='\033[0;31m' # red Color
GREEN='\033[0;32m' # Green Color
NC='\033[0m' # No Color

# Define a function to capture and log linter errors
log_error() {
    local linter_name="$1"
    errors+=("${linter_name}")
}

# Extract file list from the first argument
file_list="$1"

# Separate files based on their type
IFS=$'\n' read -d '' -r -a py_files < <(grep '\.py$' "${file_list}" && printf '\0')
IFS=$'\n' read -d '' -r -a yaml_files < <(grep '\.yaml$' "${file_list}" && printf '\0')

# Initialize an array to capture linters that fail
errors=()

# Lint Python files
if [ ${#py_files[@]} -ne 0 ]; then
  ./linters/lint_python.sh "${py_files}" || log_error ":python: Python"
fi

# Lint YAML files
if [ ${#yaml_files[@]} -ne 0 ]; then
  ./linters/lint_yaml.sh "${yaml_files}" || log_error "🔶 yamlfix (YAML)"
fi


# Check if there were any errors
if [ ${#errors[@]} -eq 0 ]; then
    echo -e "${GREEN}🥹  All linters passed successfully. ${NC}\n"
    exit 0
else
    echo -e "${RED} 🙃 Linter(s) that failed: ${NC}"
    for error in "${errors[@]}"; do
       echo -e "  --- ${error}"
    done
    exit 1
fi
