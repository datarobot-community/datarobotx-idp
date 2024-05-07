#!/bin/bash

# Get the list of files to lint from the first argument
files="$1"

# Array to store PIDs
pids=()
# Array to store exit codes
exit_codes=()

# Read the linting scripts into an array
mapfile -t scripts < <(find ./ -name 'lint_*.sh')

# Run each script in parallel
for script in "${scripts[@]}"; do
    # Run all scripts that aren't the python linter in parallel
      "$script" "$files" &
      # Store the PID of the process
      pids+=($!)
done

# Wait for all linting scripts to finish and capture their exit codes
for pid in "${pids[@]}"; do
    wait "$pid"
    exit_codes+=($?)
done

# Check exit codes
failure_detected=0
for exit_code in "${exit_codes[@]}"; do
    if [ "$exit_code" -ne 0 ]; then
        failure_detected=1
        break
    fi
done

if [ "$failure_detected" -eq 1 ]; then
    echo "One or more linting scripts failed! See logs above for details."
    exit 1
else
    echo "All linting scripts passed!"
fi
