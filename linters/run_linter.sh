#!/bin/bash

# Get the command to run from the first argument
command="$1"

# Get the list of file extensions from the second argument
file_extensions="$2"

# Get the list of files to lint from the third argument (if provided)
files_arg="$3"

# Create an empty list to hold the names of failed files
failed_files=""

RED='\033[0;31m' # red Color
GREEN='\033[0;32m' # Green Color
YELLOW="\033[38;5;226m"
NC='\033[0m' # No Color

# Determine the files to lint
if [[ -n "${files_arg}" ]]; then
  # Use the provided files
  files_to_lint="${files_arg}"
else
  # Get the current directory where the script is running
  current_dir="$(dirname "$(realpath "$0")")"

  # Go to the repository root folder
  cd "$current_dir/.." || exit

  # Search for all files with the specified file extensions
  IFS=$'\n' # Set the internal field separator to newline
  for ext in $(echo "${file_extensions}" | tr "," "\n"); do
    files_to_lint="${files_to_lint}$(find . -type f -name "*.${ext}")"$'\n'
  done
fi
# Loop through the files to lint and run the linter
IFS=$'\n'
echo -e "🎢 Running: ${YELLOW}[${command}] ${NC}"
for file in $(echo -e "${files_to_lint}"); do
  #trim any trailing whitespace
  path="${file%"${file##*[! ]}"}"
  # Capture the output of the linter command
  # Check if the output contains any errors
  if ! eval "${command} \"${path}\"" >/dev/null 2>&1; then
    # If there are errors, append the file name to the list of failed files
    failed_files="${failed_files}${file}\n"
  fi

done

# Print the list of failed files
if [[ -n "${failed_files}" ]]; then
  echo -e "🚨 ${RED} [${command}] ${NC} -  Some files failed linting:\n"
  echo -e "${failed_files}"
  exit 1
else
  echo -e  "✅ ${GREEN} [${command}] ${NC} - All looks good.\n"
fi
