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


def capture(*command, env=None, cwd=ROOT):
    # Run from the built tree so its rust-toolchain.toml selects the recorded toolchain.
    return subprocess.check_output(command, cwd=cwd, env=env, text=True).strip()


def ensure_repository_unchanged(source, root=ROOT):
    if repository_identity(root) != source:
        raise RuntimeError(
            "repository changed during the build; discard it and build again "
            "from a stable source tree"
        )


PROFILES = ("release", "profiling", "bench-fast")
# A comparison build: the release profile without LTO, with parallel codegen and
# incremental compilation. Expressed as recorded overrides rather than a Cargo
# profile so that any historical revision can be built the same way.
BENCH_FAST = {
    "CARGO_PROFILE_RELEASE_LTO": "false",
    "CARGO_PROFILE_RELEASE_CODEGEN_UNITS": "16",
    "CARGO_PROFILE_RELEASE_INCREMENTAL": "true",
    "CARGO_PROFILE_RELEASE_DEBUG": "line-tables-only",
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--profile", choices=PROFILES, default="release",
                        help="bench-fast builds quickly for comparisons; qualify results with release")
    parser.add_argument("--target", help="Rust target triple; defaults to the active rustc host")
    parser.add_argument("--features", action="append", default=[])
    parser.add_argument("--no-default-features", action="store_true")
    parser.add_argument("--source", type=Path, default=ROOT,
                        help="source tree to build, such as a git worktree; defaults to this checkout")
    parser.add_argument("--target-dir", type=Path,
                        help="reuse this Cargo target directory instead of a fresh temporary one")
    args = parser.parse_args()
    if args.output.resolve().exists():
        parser.error(f"output already exists: {args.output.resolve()}")
    print(freeze(args.output, profile=args.profile, target=args.target, features=args.features,
                 no_default_features=args.no_default_features, source=args.source,
                 target_dir=args.target_dir))


def freeze(output, *, profile="release", target=None, features=(), no_default_features=False,
           source=ROOT, target_dir=None):
    """Build `source` and write self-identifying binaries plus a manifest to `output`."""
    root = Path(source).resolve()
    features = list(features)
    identity = repository_identity(root)
    output = Path(output).resolve()
    if output.exists():
        raise FileExistsError(f"output already exists: {output}")
    with tempfile.TemporaryDirectory(prefix="embrasure-flow-qualification-") as temporary:
        staging = Path(target_dir).resolve() if target_dir else Path(temporary)
        environment = os.environ.copy()
        environment["CARGO_TARGET_DIR"] = str(staging)
        cargo_profile = profile
        if profile == "bench-fast":
            cargo_profile = "release"
            environment.update(BENCH_FAST)
        if profile in ("profiling", "bench-fast"):
            if environment.get("CARGO_ENCODED_RUSTFLAGS"):
                raise ValueError(f"{profile} builds require CARGO_ENCODED_RUSTFLAGS to be unset")
            flags = environment.get("RUSTFLAGS", "").strip()
            environment["RUSTFLAGS"] = (flags + " -C force-frame-pointers=yes").strip()

        rustc = capture("rustc", "-Vv", env=environment, cwd=root)
        explicit_target = target is not None
        target = target or next(
            line.removeprefix("host: ")
            for line in rustc.splitlines()
            if line.startswith("host: ")
        )
        command = ["cargo", "build", "--locked", "--profile", cargo_profile,
                   "--target", target]
        if no_default_features:
            command.append("--no-default-features")
        for feature in features:
            command += ["--features", feature]
        command += ["-p", "flow-daemon", "-p", "flow-testkit", "--bin", "embrasure-flow",
                    "--example", "local_compactor"]
        subprocess.run(command, cwd=root, env=environment, check=True)
        ensure_repository_unchanged(identity, root)

        build_dir = staging / target / cargo_profile
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
            "source": identity,
            "build": {
                "cargo_profile": cargo_profile,
                "target": target,
                "target_explicit": explicit_target,
                "features": features,
                "no_default_features": no_default_features,
                "command": command,
                "environment": relevant_environment,
                "rustc": rustc,
                "cargo": capture("cargo", "-V", env=environment, cwd=root),
                "cargo_lock_sha256": sha256_file(root / "Cargo.lock"),
            },
            "binaries": binaries,
        }

        # This is the last source-tree read before the frozen artifact is written.
        ensure_repository_unchanged(identity, root)
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
        return manifest_path


if __name__ == "__main__":
    main()
