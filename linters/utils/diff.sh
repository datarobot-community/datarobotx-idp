#!/bin/sh

compare() {
    # Only show differences if the third argument is provided (i.e., within a submodule)
        git diff --name-only --ignore-submodules=all --diff-filter=ACMR "$1" "$2" | awk -v r="$3" '{ print "" r "./" $0}'

}

# The third argument is empty when called for the root to skip root file differences
compare "$1" "$2" ""
