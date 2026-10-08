#!/usr/bin/env bash
# Tag and push a coordinated release across the core module and every
# opt-in submodule. Run from the repo root.
#
# Usage:
#   scripts/release.sh vX.Y.Z          # show plan, run pre-flight, prompt y/N, push
#   scripts/release.sh vX.Y.Z --dry    # show plan only, touch nothing
#   scripts/release.sh vX.Y.Z --yes    # skip prompt (CI / scripted use)
#
# Safety:
#   - Refuses on a dirty working tree.
#   - Refuses if any target tag already exists.
#   - Warns when not on main / master.
#   - Pre-flight: `go test -race ./...` on core and every opt-in submodule.
#
# The modules of this repository reach each other through replace
# directives, which apply only inside it; a consumer's go command reads
# the require lines instead. Before tagging, every requirement on a module
# of this repository is pinned to the release version in a release commit,
# so the tagged go.mod files resolve from the module proxy. After the
# push, scripts/verify-release.sh installs the release from the proxy in
# a module outside this repository and builds it.

set -euo pipefail

VERSION="${1:-}"
MODE="${2:-}"

if [[ "$VERSION" == "" ]]; then
    cat >&2 <<EOF
Usage: $0 vX.Y.Z [--dry|--yes]

  vX.Y.Z   semver-style tag (e.g. v0.2.0, v1.0.0, v0.3.0-rc1)
  --dry    show plan only, run no tests, create no tags
  --yes    skip interactive confirmation (CI)
EOF
    exit 1
fi

# Validate semver: vX.Y.Z with optional -prerelease suffix.
if [[ ! "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.-]+)?$ ]]; then
    echo "Error: version must look like vX.Y.Z (or vX.Y.Z-prerelease), got: $VERSION" >&2
    exit 1
fi

# Reject v2+ — major-version bumps require a separate module path
# (github.com/ashtonian/mqttv5/v2 etc.) and aren't handled here.
MAJOR="${VERSION#v}"
MAJOR="${MAJOR%%.*}"
if (( MAJOR >= 2 )); then
    cat >&2 <<EOF
Error: v${MAJOR}+ releases require a new module path with a /v${MAJOR} suffix
       (e.g. github.com/ashtonian/mqttv5/v${MAJOR}). This script doesn't handle
       major-version path renames. See https://go.dev/ref/mod#major-version-suffixes
EOF
    exit 1
fi

# Opt-in submodules that ship as importable packages. internal/filedb
# cannot be imported from outside this repository, but store/file and
# queue/file depend on it, so it is tagged with them. The modules that
# only run here (benchmarks, conformance, examples) aren't tagged.
SUBMODULES=(
    internal/filedb
    codec/json
    codec/msgpack
    store/file
    queue/file
    transport/ws
)

# Modules whose requirements on this repository are pinned at release.
PINNED=("${SUBMODULES[@]}" examples conformance benchmarks)

# siblings prints the modules of this repository that the go.mod at $1
# requires.
siblings() {
    awk '/^require \(/ { block = 1; next }
         block && /^\)/ { block = 0 }
         /^require [^(]/ { print $2 }
         block { print $1 }' "$1" | grep '^github.com/ashtonian/mqttv5' | sort -u
}

# Core tag first. The release workflow trigger only matches the
# core-style tag pattern (vX.Y.Z), so pushing it last would still
# work but listing it first keeps the plan readable.
TAGS=("$VERSION")
for m in "${SUBMODULES[@]}"; do
    TAGS+=("$m/$VERSION")
done

# --- safety checks ---

if [[ -n "$(git status --porcelain)" ]]; then
    echo "Error: working tree is dirty. Commit or stash first." >&2
    git status --short >&2
    exit 1
fi

for t in "${TAGS[@]}"; do
    if git rev-parse --verify "refs/tags/$t" >/dev/null 2>&1; then
        echo "Error: tag already exists: $t" >&2
        exit 1
    fi
done

BRANCH="$(git rev-parse --abbrev-ref HEAD)"
HEAD_SHA="$(git rev-parse --short HEAD)"

# --- plan ---

echo "Release plan at $HEAD_SHA (branch: $BRANCH):"
for t in "${TAGS[@]}"; do
    if [[ "$t" == "$VERSION" ]]; then
        printf '  %-32s github.com/ashtonian/mqttv5\n' "$t"
    else
        printf '  %-32s github.com/ashtonian/mqttv5/%s\n' "$t" "${t%/$VERSION}"
    fi
done

if [[ "$BRANCH" != "main" && "$BRANCH" != "master" ]]; then
    echo
    echo "Warning: releasing from '$BRANCH' (not main/master)."
fi

echo
echo "Requirements pinned to $VERSION:"
for m in "${PINNED[@]}"; do
    for dep in $(siblings "$m/go.mod"); do
        printf '  %-20s %s\n' "$m" "$dep"
    done
done

if [[ "$MODE" == "--dry" ]]; then
    echo
    echo "(dry run — no tests run, no tags created)"
    exit 0
fi

# --- pin ---

PINNED_FILES=()
for m in "${PINNED[@]}"; do
    for dep in $(siblings "$m/go.mod"); do
        (cd "$m" && go mod edit -require="$dep@$VERSION")
    done
    PINNED_FILES+=("$m/go.mod")
done
unpin() { git checkout -- "${PINNED_FILES[@]}"; }

# --- pre-flight tests ---

echo
echo "Pre-flight: testing core..."
go test -race -timeout 5m ./... >/dev/null || { unpin; exit 1; }

for m in "${SUBMODULES[@]}"; do
    echo "Pre-flight: testing $m..."
    (cd "$m" && go test -race -timeout 5m ./... >/dev/null) || { unpin; exit 1; }
done

echo "Pre-flight passed."

# --- confirm + execute ---

if [[ "$MODE" != "--yes" ]]; then
    echo
    read -r -p "Commit the pinned requirements, create and push ${#TAGS[@]} tags? [y/N] " REPLY
    if [[ ! "$REPLY" =~ ^[Yy]$ ]]; then
        unpin
        echo "Aborted."
        exit 1
    fi
fi

if [[ -n "$(git status --porcelain -- "${PINNED_FILES[@]}")" ]]; then
    git commit -m "Release $VERSION" -- "${PINNED_FILES[@]}"
fi

echo
for t in "${TAGS[@]}"; do
    git tag "$t" -m "Release $t"
    echo "  tagged $t"
done

echo
echo "Pushing $BRANCH and the tags to origin..."
git push origin "HEAD:$BRANCH" "${TAGS[@]}"

echo
echo "Installing $VERSION from the module proxy, outside this repository..."
"$(dirname "$0")/verify-release.sh" "$VERSION"

echo
echo "Done. The release workflow fires on the $VERSION tag push."
echo "Track it: gh run watch"
