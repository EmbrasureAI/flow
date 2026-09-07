"""Service-free negative controls for legacy journal format admission."""

from pathlib import Path
import struct
import tempfile
import unittest
import zlib

from legacy_evidence import legacy_journal_evidence


def terminal(kind=4, end=103):
    payload = struct.pack("<Q", 7) + b"fixture" + struct.pack("<IQQQ", 17, 100, 102, end)
    header = struct.pack("<4sHBBIQI", b"FLJ1", 1, kind, 0, len(payload), 1, 17)
    return header + struct.pack("<I", zlib.crc32(payload, zlib.crc32(header))) + payload


class LegacyEvidenceTests(unittest.TestCase):
    def inspect(self, content):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "00000000000000000001.segment").write_bytes(content)
            return legacy_journal_evidence(root, "fixture", {17: 103})

    def test_legacy_backlog_requires_complete_crc_checked_identity(self):
        evidence = self.inspect(terminal())
        self.assertEqual([(item["kind"], item["end_lsn"], item["crc_verified"]) for item in evidence],
                         [(4, 103, True)])
        damaged = bytearray(terminal())
        damaged[-1] ^= 1
        with self.assertRaisesRegex(AssertionError, "CRC mismatch"):
            self.inspect(damaged)

    def test_current_binary_cannot_satisfy_legacy_precondition(self):
        with self.assertRaisesRegex(AssertionError, "expected journal terminal 4, observed \\[5\\]"):
            self.inspect(terminal(kind=5))

    def test_partial_terminal_cannot_prove_retained_backlog(self):
        with self.assertRaisesRegex(AssertionError, "terminals are missing"):
            self.inspect(terminal()[:-1])

    def test_other_incarnation_of_same_xid_cannot_satisfy_backlog(self):
        with self.assertRaisesRegex(AssertionError, "terminals are missing"):
            self.inspect(terminal(end=99))


if __name__ == "__main__":
    unittest.main()
