#!/usr/bin/env bash
# Prints, one per line, the given packages (default ./...) except those that
# exist only under a test build tag, such as the examples module's smoke
# runner: untagged, they have nothing to vet, test, build or scan, and naming
# one explicitly is an error to go. A package that fails to load for any
# other reason stays in, so the command run over it reports the failure.
set -euo pipefail
go list -e -f '{{.ImportPath}}{{"\t"}}{{if .Error}}{{.Error.Err}}{{end}}' "${@:-./...}" |
    while IFS=$'\t' read -r package error; do
        case $error in
        *"build constraints exclude all Go files"*) ;;
        *) echo "$package" ;;
        esac
    done
