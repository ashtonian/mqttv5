#!/usr/bin/env bash
# Builds and vulnerability-scans the given packages (default ./...) from a
# module's directory, as CI's build job does. govulncheck must be on PATH.
# Run it for every module with scripts/each-module.sh "$PWD/scripts/build-module.sh".
set -euo pipefail

scripts=$(cd "$(dirname "$0")" && pwd -P)
untagged=()
while IFS= read -r package; do
    untagged+=("$package")
done < <("$scripts/untagged-packages.sh" "${@:-./...}")
if ((${#untagged[@]} == 0)); then
    exit 0
fi
go build "${untagged[@]}"
govulncheck "${untagged[@]}"
