#!/usr/bin/env bash
# The per-module checks CI runs, from a module's directory, over the given
# packages (default ./...): go.mod and go.sum are tidy, vet passes with and
# without the test build tags, and the race tests pass. The conformance
# module is all build-tagged broker tests, so it gets only the tagged vet;
# its suite runs against brokers in conformance.yml.
# Run it for every module with scripts/each-module.sh "$PWD/scripts/test-module.sh".
set -euo pipefail

module=${MODULE:-.}
packages=("${@:-./...}")
scripts=$(cd "$(dirname "$0")" && pwd -P)
go mod tidy
git diff --exit-code -- go.mod go.sum
go vet -tags 'conformance e2e smoke' "${packages[@]}"
if [[ $module == conformance ]]; then
    exit 0
fi
untagged=()
while IFS= read -r package; do
    untagged+=("$package")
done < <("$scripts/untagged-packages.sh" "${packages[@]}")
if ((${#untagged[@]} == 0)); then
    exit 0
fi
go vet "${untagged[@]}"
go test -race -v -timeout 5m -count=1 -coverprofile=coverage.out "${untagged[@]}"
# Informational, not a gate.
if [[ -n ${GITHUB_STEP_SUMMARY:-} ]]; then
    echo "### Coverage ($module): $(go tool cover -func=coverage.out | tail -n 1 | awk '{print $NF}')" >>"$GITHUB_STEP_SUMMARY"
fi
