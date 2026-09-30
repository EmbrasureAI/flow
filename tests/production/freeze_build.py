#!/usr/bin/env python3
"""Build and freeze self-identifying production qualification binaries."""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

from provenance import ROOT, repository_identity, sha256_file


def capture(*command, env=None):
    return subprocess.check_output(command, cwd=ROOT, env=env, text=True).strip()


def ensure_repository_unchanged(source):
    if repository_identity() != source:
        raise RuntimeError(
            "repository changed during the build; discard it and build again "
            "from a stable source tree"
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--profile", choices=("release", "profiling"), default="release")
    parser.add_argument("--target", help="Rust target triple; defaults to the active rustc host")
    parser.add_argument("--features", action="append", default=[])
    parser.add_argument("--no-default-features", action="store_true")
    args = parser.parse_args()

    source = repository_identity()
    output = args.output.resolve()
    if output.exists():
        parser.error(f"output already exists: {output}")
    with tempfile.TemporaryDirectory(prefix="embrasure-flow-qualification-") as staging:
        staging = Path(staging)
        environment = os.environ.copy()
        environment["CARGO_TARGET_DIR"] = str(staging)
        if args.profile == "profiling":
            if environment.get("CARGO_ENCODED_RUSTFLAGS"):
                parser.error("profiling builds require CARGO_ENCODED_RUSTFLAGS to be unset")
            flags = environment.get("RUSTFLAGS", "").strip()
            environment["RUSTFLAGS"] = (flags + " -C force-frame-pointers=yes").strip()

        rustc = capture("rustc", "-Vv", env=environment)
        target = args.target or next(
            line.removeprefix("host: ")
            for line in rustc.splitlines()
            if line.startswith("host: ")
        )
        command = ["cargo", "build", "--locked", "--profile", args.profile,
                   "--target", target]
        if args.no_default_features:
            command.append("--no-default-features")
        for features in args.features:
            command += ["--features", features]
        command += ["-p", "flow-daemon", "-p", "flow-testkit", "--bin", "embrasure-flow",
                    "--example", "local_compactor"]
        subprocess.run(command, cwd=ROOT, env=environment, check=True)
        ensure_repository_unchanged(source)

        build_dir = staging / target / args.profile
        staged_binaries = {
            "daemon": build_dir / "embrasure-flow",
            "external_compactor": build_dir / "examples/local_compactor",
        }
        binaries = {
            role: {"file": source_path.name, "sha256": sha256_file(source_path)}
            for role, source_path in staged_binaries.items()
        }
        relevant_environment = {
            name: environment[name]
            for name in sorted(environment)
            if name in {"RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_TARGET_DIR", "RUSTC_WRAPPER",
                        "RUSTC_WORKSPACE_WRAPPER", "SOURCE_DATE_EPOCH", "MACOSX_DEPLOYMENT_TARGET"}
            or name.startswith("CARGO_PROFILE_")
        }
        manifest = {
            "schema": 1,
            "source": source,
            "build": {
                "cargo_profile": args.profile,
                "target": target,
                "target_explicit": args.target is not None,
                "features": args.features,
                "no_default_features": args.no_default_features,
                "command": command,
                "environment": relevant_environment,
                "rustc": rustc,
                "cargo": capture("cargo", "-V", env=environment),
                "cargo_lock_sha256": sha256_file(ROOT / "Cargo.lock"),
            },
            "binaries": binaries,
        }

        # This is the last source-tree read before the frozen artifact is written.
        ensure_repository_unchanged(source)
        created_output = False
        try:
            output.mkdir(parents=True)
            created_output = True
            for role, source_path in staged_binaries.items():
                destination = output / binaries[role]["file"]
                shutil.copy2(source_path, destination)
                if sha256_file(destination) != binaries[role]["sha256"]:
                    raise RuntimeError(f"copied {role} binary failed content verification")
                destination.chmod(0o555)

            manifest_path = output / "build-manifest.json"
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
            manifest_path.chmod(0o444)
            output.chmod(0o555)
        except BaseException:
            if created_output:
                shutil.rmtree(output)
            raise
        print(manifest_path)


if __name__ == "__main__":
    main()
