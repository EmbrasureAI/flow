#!/usr/bin/env python3
"""Package notices from the locked daemon dependency graph, without compiling."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import tomllib


ROOT = Path(__file__).resolve().parents[1]
LEGAL_NAMES = ("LICENSE", "COPYING", "NOTICE", "COPYRIGHT", "AUTHORS", "UNLICENSE")
# These libraries compile bundled native code whose notices are below crate root.
# RocksDB selects Apache-2.0; LZ4 and Zstandard select their BSD library licenses.
NATIVE_FILES = {
    "librocksdb-sys": [
        "rocksdb/LICENSE.Apache", "rocksdb/LICENSE.leveldb", "rocksdb/AUTHORS",
        "rocksdb/utilities/transactions/lock/range/range_tree/lib/COPYING.APACHEv2",
        "snappy/COPYING", "snappy/AUTHORS",
    ],
    "lz4-sys": ["liblz4/lib/LICENSE"],
    "zstd-sys": ["zstd/LICENSE"],
    "libz-sys": ["src/zlib/LICENSE", "src/zlib-ng/LICENSE.md"],
    "bzip2-sys": ["bzip2-1.0.8/LICENSE"],
    "ring": [
        "third_party/fiat/LICENSE", "src/polyfill/once_cell/LICENSE-APACHE",
        "src/polyfill/once_cell/LICENSE-MIT",
    ],
    "aws-lc-sys": ["aws-lc/LICENSE", "aws-lc/third_party/fiat/LICENSE"],
}
NATIVE_HEADERS = {
    "librocksdb-sys": ["rocksdb/util/xxhash.cc", "rocksdb/util/xxhash.h"],
    "zstd-sys": ["zstd/lib/common/xxhash.h"],
}


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def capture(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def copy_notice(source, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)


def packages_for_daemon(metadata):
    packages = {p["id"]: p for p in metadata["packages"]}
    nodes = {n["id"]: n for n in metadata["resolve"]["nodes"]}
    pending = [p["id"] for p in packages.values() if p["name"] == "flow-daemon"]
    if len(pending) != 1:
        raise ValueError("expected exactly one flow-daemon package")
    selected = set()
    while pending:
        package_id = pending.pop()
        if package_id in selected:
            continue
        selected.add(package_id)
        pending.extend(
            dep["pkg"] for dep in nodes[package_id]["deps"]
            if any(kind["kind"] != "dev" for kind in dep["dep_kinds"])
        )
    return sorted((packages[p] for p in selected), key=lambda p: (p["name"], p["version"]))


def package(args):
    rustc = capture("rustc", "-vV")
    target = args.target or next(line[6:] for line in rustc.splitlines() if line.startswith("host: "))
    metadata = json.loads(capture(
        "cargo", "metadata", "--locked", "--offline", "--format-version", "1",
        "--filter-platform", target,
    ))
    supplements = json.loads((ROOT / "licenses/supplemental.json").read_text())["supplements"]
    for entry in supplements:
        if sha256((ROOT / "licenses" / entry["file"]).read_bytes()) != entry["sha256"]:
            raise ValueError(f"changed supplemental notice: {entry['file']}")
    output = args.output.resolve()
    if output.exists() and any(output.iterdir()):
        raise ValueError(f"output must be absent or empty: {output}")
    output.mkdir(parents=True, exist_ok=True)
    for name in ("LICENSE", "NOTICE"):
        copy_notice(ROOT / name, output / name)
    copy_notice(ROOT / "licenses/README.md", output / "README.md")
    lock_bytes = (ROOT / "Cargo.lock").read_bytes()
    locked = {(p["name"], p["version"]): p for p in tomllib.loads(lock_bytes.decode())["package"]}
    inventory = []
    for p in packages_for_daemon(metadata):
        name, version = p["name"], p["version"]
        source = Path(p["manifest_path"]).parent
        destination = output / "third-party" / f"{name}-{version}"
        files = [f for f in source.iterdir() if f.is_file() and f.name.upper().startswith(LEGAL_NAMES)]
        if p["license_file"]:
            files.append(source / p["license_file"])
        for file in sorted(set(files)):
            copy_notice(file, destination / file.name)
        if source.is_relative_to(ROOT / "crates"):
            copy_notice(ROOT / "LICENSE", destination / "LICENSE")
        for relative in NATIVE_FILES.get(name, []):
            copy_notice(source / relative, destination / "native" / relative)
        for relative in NATIVE_HEADERS.get(name, []):
            text = (source / relative).read_text()
            headers = [s for s in re.findall(r"/\*.*?\*/", text, re.DOTALL)
                       if "copyright" in s.lower() and "license" in s.lower()]
            if not headers:
                raise ValueError(f"native license header changed: {name}/{relative}")
            file = destination / "native" / (relative + ".LICENSE")
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_text("\n\n".join(headers) + "\n")
        additions = [s for s in supplements if (s["name"], s["version"]) == (name, version)]
        for entry in additions:
            copy_notice(ROOT / "licenses" / entry["file"], destination / "supplemental" / Path(entry["file"]).name)
        if not any(file.is_file() and file.name.upper().startswith(("LICENSE", "COPYING", "UNLICENSE"))
                   for file in destination.rglob("*")):
            raise ValueError(f"no license text for {name} {version}; add a pinned supplemental notice")
        if source.is_relative_to(ROOT / "vendor"):
            for relative in (".cargo_vcs_info.json", "LOCAL_CHANGES.md"):
                copy_notice(source / relative, destination / relative)
        inventory.append({
            "name": name, "version": version, "license": p["license"],
            "repository": p["repository"], "source": p["source"] or str(source.relative_to(ROOT)),
            "crate_sha256": locked[(name, version)].get("checksum"),
            "supplemental_sources": additions,
        })
    # The standard library is linked outside Cargo's package graph. The toolchain
    # ships a generated copyright report that includes its third-party licenses.
    rust_docs = Path(capture("rustc", "--print", "sysroot")) / "share/doc/rust"
    copy_notice(rust_docs / "COPYRIGHT-library.html", output / "rust/COPYRIGHT-library.html")
    for file in sorted((rust_docs / "licenses").glob("*.txt")):
        copy_notice(file, output / "rust/licenses" / file.name)
    if not (output / "rust/licenses/Apache-2.0.txt").exists():
        raise ValueError("Rust toolchain license files are missing")
    inventory_document = {
        "format_version": 1, "target": target, "rustc": rustc,
        "cargo_lock_sha256": sha256(lock_bytes), "packages": inventory,
        "files": {
            str(file.relative_to(output)): sha256(file.read_bytes())
            for file in sorted(output.rglob("*")) if file.is_file()
        },
    }
    (output / "index.json").write_text(json.dumps(inventory_document, indent=2, sort_keys=True) + "\n")
    print(f"Packaged {len(inventory)} dependency notices for {target} in {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--target", help="Cargo target triple; defaults to rustc host")
    package(parser.parse_args())
