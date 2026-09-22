#!/usr/bin/env sh
# Norupo installer for Linux and macOS.
#
#   curl -fsSL https://raw.githubusercontent.com/Mahmoud-walid/Norupo-tunnel/main/scripts/install.sh | sh
#
# Environment:
#   NORUPO_VERSION   tag to install (default: latest)
#   NORUPO_BIN_DIR   install directory (default: /usr/local/bin, or
#                    ~/.local/bin when that is not writable)
#
# POSIX sh on purpose: this has to run under dash, ash (Alpine) and bash alike.
set -eu

REPO="Mahmoud-walid/Norupo-tunnel"
VERSION="${NORUPO_VERSION:-latest}"

die() { printf 'error: %s\n' "$*" >&2; exit 1; }
info() { printf '%s\n' "$*" >&2; }

need() { command -v "$1" >/dev/null 2>&1 || die "this installer needs '$1'"; }

need uname
need tar
command -v curl >/dev/null 2>&1 || command -v wget >/dev/null 2>&1 \
  || die "this installer needs curl or wget"

fetch() {
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$1" -o "$2"
  else
    wget -qO "$2" "$1"
  fi
}

fetch_stdout() {
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$1"
  else
    wget -qO- "$1"
  fi
}

# --- platform detection ------------------------------------------------------

os="$(uname -s)"
arch="$(uname -m)"

case "$os" in
  Linux)
    # Always take the musl build: statically linked, so it runs on Arch,
    # CachyOS, Debian, Alpine, NixOS and anything else without a glibc check.
    case "$arch" in
      x86_64 | amd64) target="x86_64-unknown-linux-musl" ;;
      aarch64 | arm64) target="aarch64-unknown-linux-musl" ;;
      *) die "unsupported Linux architecture: $arch" ;;
    esac
    ;;
  Darwin)
    case "$arch" in
      arm64) target="aarch64-apple-darwin" ;;
      x86_64) target="x86_64-apple-darwin" ;;
      *) die "unsupported macOS architecture: $arch" ;;
    esac
    ;;
  MINGW* | MSYS* | CYGWIN*)
    die "on Windows, use scripts/install.ps1 instead"
    ;;
  *)
    die "unsupported operating system: $os"
    ;;
esac

# --- resolve the version -----------------------------------------------------

if [ "$VERSION" = "latest" ]; then
  info "resolving the latest release ..."
  VERSION="$(
    fetch_stdout "https://api.github.com/repos/$REPO/releases/latest" \
      | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' \
      | head -n 1
  )"
  [ -n "$VERSION" ] || die "could not determine the latest release; set NORUPO_VERSION"
fi
plain_version="${VERSION#v}"

name="norupo-${plain_version}-${target}"
url="https://github.com/$REPO/releases/download/$VERSION/$name.tar.gz"

# --- download and verify -----------------------------------------------------

tmp="$(mktemp -d)"
# shellcheck disable=SC2064  # expand $tmp now, not at trap time
trap "rm -rf '$tmp'" EXIT INT TERM

info "downloading $name ..."
fetch "$url" "$tmp/$name.tar.gz" || die "download failed: $url"

if fetch "$url.sha256" "$tmp/$name.tar.gz.sha256" 2>/dev/null; then
  expected="$(cut -d' ' -f1 < "$tmp/$name.tar.gz.sha256" | tr -d '\r\n')"
  if command -v sha256sum >/dev/null 2>&1; then
    actual="$(sha256sum "$tmp/$name.tar.gz" | cut -d' ' -f1)"
  elif command -v shasum >/dev/null 2>&1; then
    actual="$(shasum -a 256 "$tmp/$name.tar.gz" | cut -d' ' -f1)"
  else
    actual=""
    info "warning: no sha256 tool found; skipping checksum verification"
  fi
  if [ -n "$actual" ] && [ "$actual" != "$expected" ]; then
    die "checksum mismatch: expected $expected, got $actual"
  fi
  [ -n "$actual" ] && info "checksum ok"
else
  info "warning: no published checksum for this asset; skipping verification"
fi

tar -xzf "$tmp/$name.tar.gz" -C "$tmp"

# --- install -----------------------------------------------------------------

if [ -n "${NORUPO_BIN_DIR:-}" ]; then
  bindir="$NORUPO_BIN_DIR"
elif [ -w /usr/local/bin ] 2>/dev/null; then
  bindir=/usr/local/bin
else
  bindir="$HOME/.local/bin"
fi
mkdir -p "$bindir"

for binary in norupo norupo-server; do
  install -m 0755 "$tmp/$name/$binary" "$bindir/$binary" 2>/dev/null \
    || { cp "$tmp/$name/$binary" "$bindir/$binary" && chmod 0755 "$bindir/$binary"; }
done

info ""
info "installed norupo $plain_version to $bindir"
case ":$PATH:" in
  *":$bindir:"*) ;;
  *) info "note: $bindir is not on your PATH; add it to your shell profile" ;;
esac
info ""
info "  norupo http 3000 --server http://your-edge:7000"
info ""
