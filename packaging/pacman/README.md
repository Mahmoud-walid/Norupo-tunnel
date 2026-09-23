# The Norupo pacman repository

The AUR serves PKGBUILDs, not a package database. That is why `pacman -Ss
norupo` finds nothing however well the AUR packages work — `pacman` searches
only the sync databases named in `/etc/pacman.conf`.

This directory holds the public half of the key that signs a real repository,
published by the `pacman-repo` job in `.github/workflows/release.yml` on every
tagged release.

## Layout

The repository lives in two fixed, non-`latest` GitHub releases:

| tag            | contents                                                         |
| -------------- | ---------------------------------------------------------------- |
| `repo-x86_64`  | `norupo.db`, `norupo.files`, `norupo-bin-*-x86_64.pkg.tar.zst`, `.sig` for each |
| `repo-aarch64` | the same, for `aarch64`                                           |

`pacman` substitutes `$arch` in `Server=`, so both are reached by one line.

`norupo.db` is a copy, not a symlink, because a GitHub release asset cannot be
a symlink and `norupo.db` is the name `pacman` actually requests.

## Trust

Everything is detached-signed, and the documented client configuration is
`SigLevel = Required DatabaseOptional`. A repository offered under
`SigLevel = Optional TrustAll` is a promise that anyone who can serve bytes in
the middle — or who takes over the release assets — can install arbitrary
root-owned files. So the publish job refuses to run without a signing key
rather than falling back to unsigned.

`DatabaseOptional` rather than `DatabaseRequired` only because a database
signature is verified against the same key; it adds nothing once every package
in it is required to be signed, and it makes a key rotation recoverable.

## Rotating the key

1. Generate the new key and add its private half to the repository secret
   `PACKAGE_GPG_PRIVATE_KEY`.
2. Replace `norupo-signing-key.asc` here and the fingerprint in `README.md`.
3. Cut a release. The `repo-$arch` assets are rebuilt and re-signed in full.

Existing users must `pacman-key --lsign-key` the new fingerprint; until they
do, `pacman` will correctly refuse the repository. That is the cost of
`SigLevel = Required`, and it is the right cost.
