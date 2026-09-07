"""Offline, checksum-checked journal evidence for the legacy upgrade fixture."""

import struct
import zlib


def legacy_journal_evidence(root, source, expected):
    """Match committed backlog identities, never infer format from binary names.

    A crash may leave an incomplete final frame; it cannot satisfy an expected
    terminal. Complete corrupt frames always fail, including unrelated frames.
    """
    observed = {}
    paths = sorted(root.glob("*.segment"))
    for path in paths:
        with path.open("rb") as stream:
            while header := stream.read(28):
                offset = stream.tell() - len(header)
                if len(header) != 28:
                    assert path == paths[-1], f"incomplete journal header in {path}"
                    break
                magic, version, kind, reserved, length, sequence, xid, checksum = struct.unpack("<4sHBBIQII", header)
                assert magic == b"FLJ1" and version == 1 and reserved == 0, f"invalid journal header at {path}:{offset}"
                assert kind in (1, 2, 3, 4, 5), f"unknown journal kind {kind}"
                assert length <= 64 << 20, "journal frame exceeds inspector bound"
                payload = stream.read(length)
                if len(payload) != length:
                    assert path == paths[-1], f"incomplete journal payload in {path}"
                    break
                assert zlib.crc32(payload, zlib.crc32(header[:24])) == checksum, f"journal CRC mismatch at {path}:{offset}"
                if kind not in (2, 4, 5) or xid not in expected:
                    continue
                source_size = struct.unpack_from("<Q", payload)[0]
                assert source_size <= 4096 and len(payload) >= 8 + source_size + 28, "invalid terminal identity"
                payload_source = payload[8:8 + source_size].decode()
                payload_xid, _, _, end_lsn = struct.unpack_from("<IQQQ", payload, 8 + source_size)
                assert payload_xid == xid and payload_source == source, "journal terminal identity mismatch"
                if end_lsn != expected[xid]:
                    continue
                assert xid not in observed, "duplicate backlog terminal"
                observed[xid] = {"segment": path.name, "offset": offset, "kind": kind,
                                 "xid": xid, "end_lsn": end_lsn, "sequence": sequence, "crc_verified": True}
    assert set(observed) == set(expected), "legacy-format precondition: expected backlog terminals are missing"
    assert all(item["kind"] == 4 for item in observed.values()), \
        f"legacy-format precondition: expected journal terminal 4, observed {sorted({item['kind'] for item in observed.values()})}"
    return list(observed.values())
