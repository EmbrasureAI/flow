"""Exercise actual gate rejection boundaries without Cargo or native builds."""

import copy
from pathlib import Path
import tempfile
import unittest

from package_licenses import check_policy, packages_for_daemon, sha256


class LicensePolicyTests(unittest.TestCase):
    def test_exact_expressions_do_not_hide_unknown_or_additional_terms(self):
        policy = {"reviewed_expressions": {"MIT OR Apache-2.0": "Apache-2.0"},
                  "bundled_native": {}, "other_links": {}}
        package = {"name": "sample", "version": "1", "license": "MIT OR Apache-2.0"}
        check_policy([package], policy)
        for expression in (None, "", "MIT OR LicenseRef-Unknown", "MIT OR Apache-2.0 AND GPL-3.0-only"):
            with self.subTest(expression=expression), self.assertRaisesRegex(ValueError, "unreviewed license"):
                check_policy([package | {"license": expression}], policy)

    def test_native_versions_links_and_notices_require_review(self):
        with tempfile.TemporaryDirectory() as directory:
            notice = Path(directory) / "LICENSE"
            notice.write_text("original native terms")
            package = {"name": "sample-sys", "version": "1", "license": "MIT", "links": "sample",
                       "manifest_path": str(Path(directory) / "Cargo.toml")}
            policy = {"reviewed_expressions": {"MIT": "MIT"}, "other_links": {}, "bundled_native": {
                "sample-sys": {"version": "1", "links": "sample", "files": {
                    "LICENSE": sha256(notice.read_bytes())}}}}
            check_policy([package], policy)
            for changes in ({"version": "2"}, {"links": "other"}, {"name": "new-sys"}):
                with self.subTest(changes=changes), self.assertRaisesRegex(ValueError, "unreviewed native linkage"):
                    check_policy([package | changes], policy)
            notice.write_text("changed native terms")
            with self.assertRaisesRegex(ValueError, "changed reviewed native notice"):
                check_policy([package], policy)
            notice.unlink()
            with self.assertRaises(FileNotFoundError):
                check_policy([package], policy)

    def test_native_without_links_is_checked_and_collected_notices_cannot_be_omitted(self):
        policy = {"reviewed_expressions": {"MIT": "MIT"}, "other_links": {}, "bundled_native": {
            "blake3": {"version": "1", "links": None, "files": {}}}}
        package = {"name": "blake3", "version": "2", "license": "MIT", "manifest_path": "Cargo.toml"}
        with self.assertRaisesRegex(ValueError, "unreviewed native linkage"):
            check_policy([package], policy)
        package = package | {"name": "lz4-sys", "version": "1", "links": "lz4"}
        with self.assertRaisesRegex(ValueError, "unreviewed bundled native component"):
            check_policy([package], policy)
        policy["bundled_native"]["lz4-sys"] = {"version": "1", "links": "lz4", "files": {}}
        with self.assertRaisesRegex(ValueError, "native notice policy is incomplete"):
            check_policy([package], policy)

    def test_closure_includes_transitive_build_dependencies_but_not_dev_only(self):
        def dependency(name, kinds):
            return {"pkg": name, "dep_kinds": [{"kind": kind} for kind in kinds]}

        packages = [{"id": name, "name": name, "version": "1"}
                    for name in ("flow-daemon", "normal", "build", "shared", "dev-only")]
        nodes = [{"id": "flow-daemon", "deps": [dependency("normal", [None]),
                  dependency("build", ["build"]), dependency("dev-only", ["dev"])]},
                 {"id": "normal", "deps": [dependency("shared", ["dev", None])]},
                 {"id": "build", "deps": []}, {"id": "shared", "deps": []},
                 {"id": "dev-only", "deps": []}]
        metadata = {"packages": packages, "resolve": {"nodes": nodes}}
        before = copy.deepcopy(metadata)
        self.assertEqual({p["name"] for p in packages_for_daemon(metadata)},
                         {"flow-daemon", "normal", "build", "shared"})
        self.assertEqual(metadata, before)


if __name__ == "__main__":
    unittest.main()
