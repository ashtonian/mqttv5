# Convenience wrappers around scripts/release.sh.
#
# Usage:
#   make release VERSION=v0.1.0       # interactive: plan + pre-flight + prompt + push
#   make release-dry VERSION=v0.1.0   # show plan only, no tests, no tags
#   make release-yes VERSION=v0.1.0   # skip prompt (CI / scripted use)
#   make verify-release VERSION=v0.1.0 # install a pushed release from the proxy and build it

.PHONY: release release-dry release-yes verify-release

release:
	@if [ -z "$(VERSION)" ]; then echo "Usage: make release VERSION=vX.Y.Z" >&2; exit 1; fi
	@scripts/release.sh $(VERSION)

release-dry:
	@if [ -z "$(VERSION)" ]; then echo "Usage: make release-dry VERSION=vX.Y.Z" >&2; exit 1; fi
	@scripts/release.sh $(VERSION) --dry

release-yes:
	@if [ -z "$(VERSION)" ]; then echo "Usage: make release-yes VERSION=vX.Y.Z" >&2; exit 1; fi
	@scripts/release.sh $(VERSION) --yes

verify-release:
	@if [ -z "$(VERSION)" ]; then echo "Usage: make verify-release VERSION=vX.Y.Z" >&2; exit 1; fi
	@scripts/verify-release.sh $(VERSION)
