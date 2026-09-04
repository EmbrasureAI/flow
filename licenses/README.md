# Distribution notices

The Docker image includes this bundle at `/usr/share/licenses/embrasure-flow`.
`index.json` records the target, compiler, Cargo.lock hash, dependency versions,
crate checksums, and SHA-256 hashes of every bundled notice. The inventory includes
the daemon's normal and build dependency closure, conservatively including build
tools and feature-unified dependencies that may not be linked into the binary.
It excludes dependencies reached only through development dependencies.

Generate the same bundle after `cargo fetch --locked`, without compiling:

```sh
python3 scripts/package_licenses.py --output target/distribution-licenses
```

Use `--target aarch64-unknown-linux-gnu` or `--target x86_64-unknown-linux-gnu`
for the corresponding container target. The output directory must be new or empty.
Generation uses offline Cargo metadata and fails when a selected dependency lacks
license text. This is a notice collector, not a license compatibility checker.

Most texts come directly from the checksum-verified Cargo package. A small number
of packages omit their license or project NOTICE from the published archive.
`supplemental.json` pins the missing texts by upstream revision and SHA-256; those
files are checked in under `upstream/`. `crc32c` publishes an explicit Apache-2.0
or MIT grant in its README but no license file; its bundle selects Apache-2.0 and
includes the standard license from the ASF. Supplement entries match exact crate
versions and must be reviewed when those dependencies change.

The bundle retains the additional licenses for statically compiled native code:
RocksDB (Apache-2.0 choice), LevelDB, xxHash, compression libraries, ring, and AWS-LC.
RocksDB's alternative GPLv2 license does not override its Apache-2.0 option. LZ4
and Zstandard use their BSD library licenses; their separate GPL command-line
tools are not linked. The Rust toolchain's generated `COPYRIGHT-library.html` and
license texts cover the standard library and its dependencies outside Cargo's
graph. That report conservatively includes other Rust targets as well.

Debian's dynamic libraries and base-image programs retain their package copyright
and license notices under `/usr/share/doc`; they are not Cargo dependencies.
Source and binary packages retain their respective upstream licenses. This bundle
does not relicense them or assert that every included component is linked.
