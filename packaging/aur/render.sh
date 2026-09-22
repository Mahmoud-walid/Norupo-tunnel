#!/usr/bin/env bash
# Stamps a release version into a PKGBUILD before it is pushed to the AUR.
#
# Usage: packaging/aur/render.sh <norupo|norupo-bin> <version-without-v>
set -euo pipefail

package="${1:?usage: render.sh <package> <version>}"
version="${2:?usage: render.sh <package> <version>}"
version="${version#v}"

pkgbuild="$(dirname "$0")/$package/PKGBUILD"
[ -f "$pkgbuild" ] || { echo "no PKGBUILD for '$package'" >&2; exit 1; }

# `pkgver=` is the only line that changes per release; `updpkgsums` fills in
# the checksums afterwards, so we must not touch them here.
sed -i -E "s/^pkgver=.*/pkgver=${version}/" "$pkgbuild"
sed -i -E "s/^pkgrel=.*/pkgrel=1/" "$pkgbuild"

echo "rendered $pkgbuild at version ${version}"
