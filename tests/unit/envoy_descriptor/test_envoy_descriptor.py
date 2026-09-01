#!/usr/bin/env python3
"""Semantic and generator-scope tests for the Envoy descriptor (SPEC-131)."""

from __future__ import annotations

from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[3]
DESCRIPTOR = ROOT / "deploy" / "envoy" / "retrieval.pb"
TASKFILE = ROOT / "Taskfile.yml"
RETRIEVAL_PROTO = "eci/retrieval/v1/retrieval.proto"
RETRIEVAL_SERVICE = "eci.retrieval.v1.RetrievalEngine"


class EnvoyDescriptorTests(unittest.TestCase):
    def test_descriptor_contains_only_the_public_retrieval_contract(self) -> None:
        files = decode_descriptor_set(DESCRIPTOR.read_bytes())
        self.assertEqual([item[0] for item in files], [RETRIEVAL_PROTO])
        services = {f"{package}.{service}" for _, package, names in files for service in names}
        self.assertEqual(services, {RETRIEVAL_SERVICE})

    def test_generator_is_path_scoped_instead_of_module_wide(self) -> None:
        taskfile = TASKFILE.read_text(encoding="utf-8")
        self.assertIn(
            "--path contracts/proto/eci/retrieval/v1/retrieval.proto",
            taskfile,
        )
        self.assertNotIn(
            "buf build contracts --as-file-descriptor-set -o deploy/envoy/retrieval.pb",
            taskfile,
        )

    def test_structural_decoder_rejects_empty_and_truncated_artifacts(self) -> None:
        for payload in (b"", b"\x0a\x05bad"):
            with self.subTest(payload=payload):
                with self.assertRaises(ValueError):
                    decode_descriptor_set(payload)


def decode_descriptor_set(payload: bytes) -> list[tuple[str, str, list[str]]]:
    files: list[tuple[str, str, list[str]]] = []
    for field, wire, value in fields(payload):
        if field != 1 or wire != 2:
            raise ValueError("unexpected FileDescriptorSet field")
        name = ""
        package = ""
        services: list[str] = []
        for child_field, child_wire, child_value in fields(value):
            if child_field == 1 and child_wire == 2:
                name = child_value.decode("utf-8")
            elif child_field == 2 and child_wire == 2:
                package = child_value.decode("utf-8")
            elif child_field == 6 and child_wire == 2:
                service_name = next(
                    (
                        nested.decode("utf-8")
                        for nested_field, nested_wire, nested in fields(child_value)
                        if nested_field == 1 and nested_wire == 2
                    ),
                    "",
                )
                if not service_name:
                    raise ValueError("service descriptor has no name")
                services.append(service_name)
        if not name or not package:
            raise ValueError("file descriptor is incomplete")
        files.append((name, package, services))
    if not files:
        raise ValueError("descriptor set is empty")
    return files


def fields(payload: bytes):
    position = 0
    while position < len(payload):
        tag, position = varint(payload, position)
        field = tag >> 3
        wire = tag & 7
        if field == 0:
            raise ValueError("invalid protobuf field zero")
        if wire == 0:
            value, position = varint(payload, position)
        elif wire == 1:
            end = position + 8
            value = payload[position:end]
            position = end
        elif wire == 2:
            length, position = varint(payload, position)
            end = position + length
            value = payload[position:end]
            position = end
        elif wire == 5:
            end = position + 4
            value = payload[position:end]
            position = end
        else:
            raise ValueError("unsupported protobuf wire type")
        if position > len(payload):
            raise ValueError("truncated protobuf payload")
        yield field, wire, value


def varint(payload: bytes, position: int) -> tuple[int, int]:
    value = 0
    for shift in range(0, 70, 7):
        if position >= len(payload):
            raise ValueError("truncated protobuf varint")
        byte = payload[position]
        position += 1
        value |= (byte & 0x7F) << shift
        if byte < 0x80:
            return value, position
    raise ValueError("protobuf varint exceeds uint64")


if __name__ == "__main__":
    unittest.main()
