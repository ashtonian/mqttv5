#!/usr/bin/env bash
# The per-module checks CI runs, from a module's directory: go.mod and go.sum
# are tidy, vet passes with and without the test build tags, and the race
# tests pass. The conformance module is all build-tagged broker tests, so it
# gets only the tagged vet; its suite runs against brokers in conformance.yml.
# Run it for every module with scripts/each-module.sh "$PWD/scripts/test-module.sh".
set -euo pipefail

module=${MODULE:-.}
go mod tidy
git diff --exit-code -- go.mod go.sum
if [[ $module != conformance ]]; then
    go vet ./...
fi
go vet -tags 'conformance e2e smoke' ./...
if [[ $module == conformance ]]; then
    exit 0
fi
go test -race -v -timeout 5m -count=1 -coverprofile=coverage.out ./...
# Informational, not a gate.
if [[ -n ${GITHUB_STEP_SUMMARY:-} ]]; then
    echo "### Coverage ($module): $(go tool cover -func=coverage.out | tail -n 1 | awk '{print $NF}')" >>"$GITHUB_STEP_SUMMARY"
fi
