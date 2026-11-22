#!/usr/bin/env bash
# Prints, one per line, the given packages (default ./...) except those that
# exist only under a test build tag, such as the examples module's smoke
# runner: untagged, they have nothing to vet, test, build or scan, and naming
# one explicitly is an error to go. With --buildable it also leaves out
# packages with no non-test Go files (the conformance module's root), which
# go build refuses by name. A package that fails to load for any other
# reason stays in, so the command run over it reports the failure.
#
#   scripts/untagged-packages.sh [--buildable] [PACKAGE]...
set -euo pipefail
buildable=false
if [[ ${1:-} == --buildable ]]; then
    buildable=true
    shift
fi
go list -e -f '{{.ImportPath}}{{"\t"}}{{len .GoFiles}}{{"\t"}}{{len .CgoFiles}}{{"\t"}}{{if .Error}}{{.Error.Err}}{{end}}' "${@:-./...}" |
    while IFS=$'\t' read -r package gofiles cgofiles error; do
        case $error in
        *"build constraints exclude all Go files"*) continue ;;
        esac
        if [[ $buildable == true && -z $error && $gofiles == 0 && $cgofiles == 0 ]]; then
            continue
        fi
        echo "$package"
    done
