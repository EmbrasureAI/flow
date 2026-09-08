import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).parent))
from provenance import load_build_manifest, repository_identity, sha256_file, verified_binary  # noqa: E402


class BuildManifestTest(unittest.TestCase):
    @staticmethod
    def manifest(daemon, compactor):
        return {
            "schema": 1,
            "source": {
                "head": "a" * 40,
                "tree": "b" * 40,
                "dirty": False,
                "dirty_diff_sha256": "c" * 64,
                "dirty_diff_method": "test method",
            },
            "build": {
                "cargo_profile": "release",
                "target": "test-target",
                "target_explicit": False,
                "features": [],
                "no_default_features": False,
                "command": ["cargo", "build"],
                "environment": {},
                "rustc": "rustc test",
                "cargo": "cargo test",
                "cargo_lock_sha256": "d" * 64,
            },
            "binaries": {
                "daemon": {"file": "daemon", "sha256": sha256_file(daemon)},
                "external_compactor": {
                    "file": "compactor",
                    "sha256": sha256_file(compactor),
                },
            },
        }

    def test_repository_identity_covers_tracked_and_untracked_content(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            subprocess.run(["git", "init", "--quiet"], cwd=root, check=True)
            subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=root, check=True)
            subprocess.run(["git", "config", "user.name", "Test"], cwd=root, check=True)
            (root / "tracked").write_text("one")
            subprocess.run(["git", "add", "tracked"], cwd=root, check=True)
            subprocess.run(["git", "commit", "--quiet", "-m", "initial"], cwd=root, check=True)
            clean = repository_identity(root)
            self.assertFalse(clean["dirty"])

            (root / "tracked").write_text("two")
            (root / "untracked").write_text("three")
            dirty = repository_identity(root)
            self.assertTrue(dirty["dirty"])
            self.assertNotEqual(clean["dirty_diff_sha256"], dirty["dirty_diff_sha256"])

    def test_verifies_each_binary_role_by_content(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            daemon = root / "daemon"
            compactor = root / "compactor"
            daemon.write_bytes(b"daemon-v1")
            compactor.write_bytes(b"compactor-v1")
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps(self.manifest(daemon, compactor)))
            loaded = load_build_manifest(manifest)
            self.assertEqual(verified_binary(loaded, "daemon", daemon)["sha256"], sha256_file(daemon))
            self.assertEqual(
                verified_binary(loaded, "external_compactor", compactor)["sha256"],
                sha256_file(compactor),
            )
            daemon.write_bytes(b"changed")
            with self.assertRaisesRegex(ValueError, "does not match build manifest"):
                verified_binary(loaded, "daemon", daemon)

    def test_rejects_invalid_git_identity_and_non_boolean_dirty(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            daemon = root / "daemon"
            compactor = root / "compactor"
            daemon.write_bytes(b"daemon")
            compactor.write_bytes(b"compactor")
            valid = self.manifest(daemon, compactor)
            sha256_repository = copy.deepcopy(valid)
            sha256_repository["source"]["head"] = "e" * 64
            sha256_repository["source"]["tree"] = "f" * 64
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps(sha256_repository))
            load_build_manifest(manifest)

            cases = (
                (("source", "head"), "A" * 40, "Git head or tree"),
                (("source", "tree"), "b" * 39, "Git head or tree"),
                (("source", "dirty"), 0, "must be a boolean"),
            )
            for path, value, message in cases:
                with self.subTest(path=path, value=value):
                    malformed = copy.deepcopy(valid)
                    malformed[path[0]][path[1]] = value
                    manifest = root / "manifest.json"
                    manifest.write_text(json.dumps(malformed))
                    with self.assertRaisesRegex(ValueError, message):
                        load_build_manifest(manifest)

    def test_rejects_invalid_build_field_types(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            daemon = root / "daemon"
            compactor = root / "compactor"
            daemon.write_bytes(b"daemon")
            compactor.write_bytes(b"compactor")
            valid = self.manifest(daemon, compactor)
            cases = (
                (("build", "target_explicit"), 0, "switches must be booleans"),
                (("build", "features"), [1], "features must be strings"),
                (("build", "command"), "cargo build", "command must be"),
                (("build", "environment"), {"RUSTFLAGS": 1}, "environment must map"),
                (("binaries", "daemon", "file"), "bin/daemon", "must be a basename"),
            )
            for path, value, message in cases:
                with self.subTest(path=path, value=value):
                    malformed = copy.deepcopy(valid)
                    target = malformed
                    for component in path[:-1]:
                        target = target[component]
                    target[path[-1]] = value
                    manifest = root / "manifest.json"
                    manifest.write_text(json.dumps(malformed))
                    with self.assertRaisesRegex(ValueError, message):
                        load_build_manifest(manifest)


if __name__ == "__main__":
    unittest.main()
