# Licenses and distribution notices

The repository's root `LICENSE` grants Apache-2.0 for Embrasure Flow. Vendored
Iceberg code retains its upstream licenses, attribution notices and documented
local changes. Dependency licenses apply to their respective components; this
project does not relicense them.

This directory supplies missing upstream texts and the policy used to generate
binary notices. Most dependency notices come directly from Cargo packages.
`supplemental.json` pins missing texts to exact package versions, upstream sources
and SHA-256 hashes. Identical Apache license texts share one checked-in copy.

The Docker build generates a bundle at `/usr/share/licenses/embrasure-flow`.
Its `index.json` records the target, compiler, Cargo.lock hash, dependency versions,
crate checksums and hashes of bundled notices. It covers the daemon's normal and
build dependencies, including some tools and feature-unified dependencies that
may not be linked. Dependencies reached only through dev dependencies are excluded.

## Generate and check

Run from the repository root; no compilation is needed:

```sh
cargo fetch --locked
python3 scripts/package_licenses.py --output target/distribution-licenses
python3 scripts/package_licenses.py --check-only --target x86_64-unknown-linux-gnu
```

The default target is the compiler's host. Reviewed targets are
`aarch64-apple-darwin`, `aarch64-unknown-linux-gnu` and
`x86_64-unknown-linux-gnu`. Output must be absent or empty. CI generates complete
bundles for both Linux targets; `--check-only` checks the policy but does not
establish that every distributable notice is available.

## Dependency updates

[policy.json](policy.json) records exact reviewed license expressions, selected
license alternatives, native package versions, Cargo linkage declarations and
notice hashes. Unknown expressions, targets or linkage and changed native notices
fail the check. Additional terms joined by `AND` remain cumulative. Review changed
sources and terms before updating this policy or supplemental hashes.

Native notices cover RocksDB, LevelDB, xxHash, compression libraries, ring, AWS-LC
and BLAKE3. RocksDB selects Apache-2.0; LZ4 and Zstandard select their BSD library
licenses, without their separately licensed command-line tools. The bzip2 library
retains its own terms independently of its Rust wrapper. The collector is not a
scanner for undisclosed bundled code, including native builds without `links`.

Rust standard-library notices are included outside Cargo's graph and cover other
targets too: the report's Fortanix SGX terms do not imply that component is linked
on the reviewed targets. Snappy's notice also mentions benchmark data that this
daemon does not package. These upstream texts remain intact. Debian libraries and
programs retain their separate notices under `/usr/share/doc` in the image.

The macOS objc2 supplements preserve the
[upstream qualification about Apple SDK-derived bindings](https://raw.githubusercontent.com/madsmtm/objc2/7b1abfd750a2cacaea71d6a56ecfb83cb7de560b/LICENSE.md).
Passing this gate does not resolve that qualification, approve SDK redistribution
or audit the entire toolchain and OS image. Review toolchain, image and target
changes separately.
