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
license text. Before packaging, it also checks the reviewed policy below.

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

## Reviewed dependency policy

Run the same compatibility gate without creating a bundle or compiling:

```sh
python3 scripts/package_licenses.py --check-only --target x86_64-unknown-linux-gnu
python3 scripts/package_licenses.py --check-only --target aarch64-unknown-linux-gnu
```

[policy.json](policy.json) covers the collector's normal/build dependency closure
for the listed Linux and macOS targets. It records exact reviewed Cargo license
expressions and the license choice used for this distribution. Unknown or missing
expressions fail; there is no wildcard allowance or inferred SPDX parsing.
`AND` obligations remain cumulative. Legacy slash expressions are reviewed as
written: the Brotli and RocksDB entries retain their additional BSD/MIT terms.
The inventory preserves the original Cargo expression alongside the reviewed
choice; all previously collected license and notice texts remain included.

CI creates complete bundles for both Linux targets, so missing notice text also
fails the release check. `--check-only` reviews metadata and pinned native terms;
it does not establish that every dependency's distributable notice is available.

The review uses the [ASF third-party policy](https://www.apache.org/legal/resolved.html),
the [Unicode license](https://www.unicode.org/license.txt), and the exact packaged
texts. This is this project's distribution policy, not an ASF certification.
The bundled bzip2 library retains its own
[bzip2 license](https://spdx.org/licenses/bzip2-1.0.6.html); its Rust wrapper's
Apache/MIT choice does not replace the native terms.

Native reviews pin package versions, Cargo `links` values and SHA-256 hashes of
the selected notice files or source files containing embedded notices. A changed
native package, missing review, changed text or new `links` declaration fails.
BLAKE3's bundled C/assembly is explicitly covered despite having no `links` field.
This is not a scanner for undisclosed native code: dependency review must identify
additional bundled code, including native builds without `links`, when adding or
upgrading dependencies. Refresh hashes only after reviewing the changed sources
and terms; never change an upstream license declaration to make the gate pass.

The `other_links` entries explain wrapper metadata, build tools and system
linkage. They do not approve arbitrary versions of installed native libraries.
Debian packages in the image, including glibc, libstdc++ and OpenSSL, retain their
own terms and package notices. The gate does not classify the entire OS image
as Apache-compatible or discharge its separate redistribution obligations.

The macOS `objc2-core-foundation` and `objc2-system-configuration` 0.3.2 crates
omit their project license file from the Cargo archive. Their supplemental entry
preserves the complete [upstream license document](upstream/objc2/LICENSE.md)
from revision `7b1abfd750a2cacaea71d6a56ecfb83cb7de560b`, including its discussion
of uncertainty about Apple SDK-derived bindings. Both use the existing Apache
license text for their offered Apache-2.0 choice. Passing the metadata and notice
checks does not resolve that SDK redistribution question or approve shipping
Apple SDKs; the upstream qualification remains intact.

The Rust standard-library copyright report is retained outside this Cargo gate.
It covers additional targets: Rust 1.97.1 lists MPL-2.0 `fortanix-sgx-abi`, while
its [standard-library manifest](https://github.com/rust-lang/rust/blob/1.97.1/library/std/Cargo.toml)
restricts that dependency to Fortanix SGX, outside the targets above. Likewise,
Snappy's notice mentions CC-BY benchmark data that this daemon does not package.
Neither occurrence is removed or relabeled. Toolchain, image or target changes
require a separate review; passing this gate is not a complete binary-link audit.
