#!/bin/sh
# covgate.sh <profile> <min-percent> <label>: fail unless the total statement coverage of a Go cover profile
# is at least <min-percent>. Run from the module that wrote the profile.
set -e
total=$(go tool cover -func="$1" | awk '/^total:/ {gsub("%","",$3); print $3}')
echo "$3 coverage: ${total}%"
awk -v t="$total" -v m="$2" 'BEGIN { exit !(t+0 >= m+0) }' || { echo "$3 coverage below $2%"; exit 1; }
