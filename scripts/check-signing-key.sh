#!/usr/bin/env bash
# Asserts that the fingerprint people are told to trust is the fingerprint of
# the key actually shipped in this repository.
#
# The two live in different files and are copied by hand. A mismatch is not a
# cosmetic docs bug: `pacman-key --lsign-key <wrong fingerprint>` fails outright
# if the key is absent, and - far worse - succeeds against some *other* key the
# user happens to hold, silently widening what their system trusts to install
# root-owned files.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
key="$root/packaging/pacman/norupo-signing-key.asc"
readme="$root/README.md"

command -v gpg >/dev/null || { echo "gpg is required" >&2; exit 1; }

# A throwaway keyring, so this never reads or writes the caller's own.
GNUPGHOME="$(mktemp -d)"
export GNUPGHOME
trap 'rm -rf "$GNUPGHOME"' EXIT
chmod 700 "$GNUPGHOME"

gpg --batch --quiet --import "$key"
actual="$(gpg --list-keys --with-colons | awk -F: '/^fpr:/ {print $10; exit}')"

# The README's fingerprint is whatever `pacman-key --lsign-key` is handed.
documented="$(grep -oE 'pacman-key --lsign-key [0-9A-F]{40}' "$readme" | awk '{print $3}' | head -1)"

if [ -z "$documented" ]; then
  echo "no 'pacman-key --lsign-key <fingerprint>' line found in README.md" >&2
  exit 1
fi

if [ "$actual" != "$documented" ]; then
  echo "signing key mismatch:" >&2
  echo "  packaging/pacman/norupo-signing-key.asc: $actual" >&2
  echo "  README.md --lsign-key:                   $documented" >&2
  exit 1
fi

echo "signing key OK: $actual"
