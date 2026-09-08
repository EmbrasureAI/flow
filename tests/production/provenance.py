"""Build and run provenance for production qualification artifacts."""

import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]


def _is_sha256(value):
    return (isinstance(value, str) and len(value) == 64
            and all(character in "0123456789abcdef" for character in value))


def _is_git_object_id(value):
    return (isinstance(value, str) and len(value) in (40, 64)
            and all(character in "0123456789abcdef" for character in value))


def _is_nonempty_string(value):
    return isinstance(value, str) and bool(value.strip())


def sha256_file(path):
    path = Path(path)
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def repository_identity(root=ROOT):
    root = Path(root).resolve()
    command = lambda *args: subprocess.check_output(["git", *args], cwd=root)
    status = command("status", "--porcelain=v1", "-z", "--untracked-files=all")
    tracked = command("diff", "--binary", "--no-ext-diff", "HEAD", "--")
    untracked = command("ls-files", "--others", "--exclude-standard", "-z").split(b"\0")
    digest = hashlib.sha256()
    digest.update(b"tracked-diff\0")
    digest.update(tracked)
    digest.update(b"untracked-files\0")
    for encoded in sorted(path for path in untracked if path):
        path = root / os.fsdecode(encoded)
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        if path.is_symlink():
            target = os.fsencode(os.readlink(path))
            digest.update(b"symlink\0")
            digest.update(len(target).to_bytes(8, "big"))
            digest.update(target)
            continue
        digest.update(b"file\0")
        size = path.stat().st_size
        digest.update(size.to_bytes(8, "big"))
        consumed = 0
        with path.open("rb") as source:
            while chunk := source.read(1 << 20):
                digest.update(chunk)
                consumed += len(chunk)
        if consumed != size:
            raise RuntimeError(f"untracked file changed while hashing: {path}")
    return {
        "head": command("rev-parse", "HEAD").decode().strip(),
        "tree": command("rev-parse", "HEAD^{tree}").decode().strip(),
        "dirty": bool(status),
        "dirty_diff_sha256": digest.hexdigest(),
        "dirty_diff_method": (
            "SHA-256 of binary HEAD diff plus length-framed paths, file kinds, contents, "
            "and symlink targets for non-ignored untracked files"
        ),
    }


def machine_identity():
    memory = None
    try:
        memory = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (OSError, ValueError):
        if sys.platform == "darwin":
            memory = int(subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True))
    return {
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "logical_cpus": os.cpu_count(),
        "physical_memory_bytes": memory,
    }


def load_build_manifest(path):
    path = Path(path).resolve()
    raw = path.read_bytes()
    manifest = json.loads(raw)
    if (not isinstance(manifest, dict)
            or type(manifest.get("schema")) is not int
            or manifest["schema"] != 1):
        raise ValueError("build manifest schema must be 1")
    source = manifest.get("source")
    build = manifest.get("build")
    if not isinstance(source, dict) or not isinstance(build, dict):
        raise ValueError("build manifest is missing source or build provenance")
    source_fields = {"head", "tree", "dirty", "dirty_diff_sha256", "dirty_diff_method"}
    build_fields = {"cargo_profile", "target", "target_explicit", "features",
                    "no_default_features", "command", "environment", "rustc", "cargo",
                    "cargo_lock_sha256"}
    if not source_fields <= source.keys() or not build_fields <= build.keys():
        raise ValueError("build manifest has incomplete source or toolchain provenance")
    if (not _is_git_object_id(source["head"])
            or not _is_git_object_id(source["tree"])
            or len(source["head"]) != len(source["tree"])):
        raise ValueError("build manifest has invalid Git head or tree object IDs")
    if type(source["dirty"]) is not bool:
        raise ValueError("build manifest source dirty field must be a boolean")
    if not _is_nonempty_string(source["dirty_diff_method"]):
        raise ValueError("build manifest has no dirty diff method")
    if not _is_sha256(source["dirty_diff_sha256"]) or not _is_sha256(build["cargo_lock_sha256"]):
        raise ValueError("build manifest has invalid source or Cargo.lock digests")
    if (not _is_nonempty_string(build["cargo_profile"])
            or not _is_nonempty_string(build["target"])
            or not _is_nonempty_string(build["rustc"])
            or not _is_nonempty_string(build["cargo"])):
        raise ValueError("build manifest has invalid build or toolchain strings")
    if type(build["target_explicit"]) is not bool or type(build["no_default_features"]) is not bool:
        raise ValueError("build manifest feature switches must be booleans")
    if (not isinstance(build["features"], list)
            or not all(_is_nonempty_string(feature) for feature in build["features"])):
        raise ValueError("build manifest features must be strings")
    if (not isinstance(build["command"], list) or not build["command"]
            or not all(isinstance(argument, str) for argument in build["command"])):
        raise ValueError("build manifest command must be a non-empty string list")
    if (not isinstance(build["environment"], dict)
            or not all(isinstance(name, str) and isinstance(value, str)
                       for name, value in build["environment"].items())):
        raise ValueError("build manifest environment must map strings to strings")
    binaries = manifest.get("binaries")
    if not isinstance(binaries, dict) or not {"daemon", "external_compactor"} <= binaries.keys():
        raise ValueError("build manifest must identify daemon and external_compactor binaries")
    for role in ("daemon", "external_compactor"):
        binary = binaries[role]
        if not isinstance(binary, dict) or not _is_nonempty_string(binary.get("file")):
            raise ValueError(f"build manifest has no valid file for {role}")
        if Path(binary["file"]).name != binary["file"]:
            raise ValueError(f"build manifest file for {role} must be a basename")
        if not _is_sha256(binary.get("sha256")):
            raise ValueError(f"build manifest has no valid SHA-256 for {role}")
    return {"path": str(path), "sha256": hashlib.sha256(raw).hexdigest(), "contents": manifest}


def verified_binary(manifest_record, role, path):
    path = Path(path).resolve()
    actual = sha256_file(path)
    expected = manifest_record["contents"]["binaries"][role]["sha256"]
    if actual != expected:
        raise ValueError(f"{role} binary does not match build manifest: expected {expected}, got {actual}")
    return {"path": str(path), "sha256": actual, "build_manifest_role": role}
