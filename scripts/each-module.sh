#!/usr/bin/env bash
# Runs a command in the directory of each module go.work uses, with MODULE
# set to the module's path (".", "codec/json", ...) and the packages to check
# appended as arguments. Every module runs even after one fails, so a single
# run reports all failures; the exit status is non-zero when any module
# failed.
#
# GO_PACKAGES limits the run to the affected modules and packages: the
# go-ci-actions scope output, {"<module>": ["./pkg", "."]}. Unset, empty or
# "all", every module runs with ./... .
#
#   scripts/each-module.sh go vet
#   GO_PACKAGES='{"codec/json": ["."]}' scripts/each-module.sh "$PWD/scripts/test-module.sh"
set -euo pipefail

if (($# == 0)); then
    echo "usage: scripts/each-module.sh COMMAND [ARG]..." >&2
    exit 2
fi
root=$(cd "$(dirname "$0")/.." && pwd -P)
cd "$root"
# In workspace mode go list -m names exactly the modules go.work uses.
modules=$(go list -m -f '{{.Dir}}' | while IFS= read -r dir; do
    module=${dir#"$root"}
    module=${module#/}
    echo "${module:-.}"
done)
plan=$(GO_PACKAGES=${GO_PACKAGES:-all} MODULES=$modules python3 -c '
import json, os, sys
modules = os.environ["MODULES"].split("\n")
raw = os.environ["GO_PACKAGES"]
selected = {module: ["./..."] for module in modules} if raw == "all" else json.loads(raw)
unknown = sorted(set(selected) - set(modules))
if unknown:
    sys.exit("GO_PACKAGES names modules go.work does not use: " + ", ".join(unknown))
for module in modules:
    if selected.get(module):
        print(module + "\t" + " ".join(selected[module]))
')
if [[ -z $plan ]]; then
    echo "no module has an affected package"
    exit 0
fi
failed=()
while IFS=$'\t' read -r module packages; do
    read -r -a args <<<"$packages"
    [[ -n ${GITHUB_ACTIONS:-} ]] && echo "::group::$module: $* ${args[*]}"
    if ! (cd "$root/$module" && MODULE=$module "$@" "${args[@]}"); then
        failed+=("$module")
        [[ -n ${GITHUB_ACTIONS:-} ]] && echo "::error::$module: $* failed"
    fi
    [[ -n ${GITHUB_ACTIONS:-} ]] && echo "::endgroup::"
done <<<"$plan"
if ((${#failed[@]})); then
    echo "failed in: ${failed[*]}" >&2
    exit 1
fi
