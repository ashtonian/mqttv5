#!/usr/bin/env bash
# Runs a command in the directory of every module go.work uses, with MODULE
# set to the module's path (".", "codec/json", ...). Every module runs even
# after one fails, so a single run reports all failures; the exit status is
# non-zero when any module failed.
#
#   scripts/each-module.sh go vet ./...
#   scripts/each-module.sh "$PWD/scripts/test-module.sh"
set -euo pipefail

if (($# == 0)); then
    echo "usage: scripts/each-module.sh COMMAND [ARG]..." >&2
    exit 2
fi
root=$(cd "$(dirname "$0")/.." && pwd -P)
cd "$root"
# In workspace mode go list -m names exactly the modules go.work uses.
dirs=$(go list -m -f '{{.Dir}}')
failed=()
while IFS= read -r dir; do
    module=${dir#"$root"}
    module=${module#/}
    module=${module:-.}
    [[ -n ${GITHUB_ACTIONS:-} ]] && echo "::group::$module: $*"
    if ! (cd "$dir" && MODULE=$module "$@"); then
        failed+=("$module")
        [[ -n ${GITHUB_ACTIONS:-} ]] && echo "::error::$module: $* failed"
    fi
    [[ -n ${GITHUB_ACTIONS:-} ]] && echo "::endgroup::"
done <<<"$dirs"
if ((${#failed[@]})); then
    echo "failed in: ${failed[*]}" >&2
    exit 1
fi
