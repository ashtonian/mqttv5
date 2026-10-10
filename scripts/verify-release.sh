#!/usr/bin/env bash
# Check that a release installs the way a consumer installs it: in a new
# module outside this repository — no workspace, no replace directives —
# go get every released module at the version from the module proxy,
# build and vet a program that imports each of them, and check that
# every module of this repository resolved to that version.
#
#   scripts/verify-release.sh vX.Y.Z
#
# Run it after the tags are pushed (scripts/release.sh does, and so does
# the release workflow). VERIFY_MODULES, a space-separated list of module
# paths, replaces the default list, e.g. to check an older release that
# had fewer modules.
set -euo pipefail

VERSION=${1:?usage: scripts/verify-release.sh vX.Y.Z}
read -r -a MODULES <<<"${VERIFY_MODULES:-github.com/ashtonian/mqttv5 github.com/ashtonian/mqttv5/codec/json github.com/ashtonian/mqttv5/codec/msgpack github.com/ashtonian/mqttv5/store/file github.com/ashtonian/mqttv5/queue/file github.com/ashtonian/mqttv5/transport/ws}"

dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT
cd "$dir"
export GOWORK=off GOFLAGS=-mod=mod
go mod init verify.invalid/mqttv5-release >/dev/null 2>&1

# The proxy may need a moment to see a tag pushed seconds ago.
for m in "${MODULES[@]}"; do
    for attempt in 1 2 3 4 5 6; do
        if go get "$m@$VERSION"; then
            break
        fi
        if [ "$attempt" -eq 6 ]; then
            echo "verify-release: $m@$VERSION does not resolve from the module proxy" >&2
            exit 1
        fi
        sleep 10
    done
done

{
    echo 'package main'
    echo
    echo 'import ('
    for m in "${MODULES[@]}"; do
        echo "	_ \"$m\""
    done
    echo ')'
    echo
    echo 'func main() {}'
} >main.go
go build ./...
go vet ./...

wrong=$(go list -m all | awk -v v="$VERSION" '$1 ~ /^github\.com\/ashtonian\/mqttv5(\/|$)/ && $2 != v')
if [ -n "$wrong" ]; then
    echo "verify-release: modules of this repository resolved to another version:" >&2
    echo "$wrong" >&2
    exit 1
fi
echo "verify-release: $VERSION installs, builds and vets from the module proxy"
