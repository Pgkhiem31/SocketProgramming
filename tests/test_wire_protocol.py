import json
import socket
import struct
import unittest
from datetime import datetime
from unittest.mock import Mock, patch
from uuid import uuid4

from backend import wire_protocol as wire


class WireProtocolTests(unittest.TestCase):
    def test_now_has_millisecond_precision(self):
        timestamp = wire.now()
        self.assertIsNotNone(datetime.fromisoformat(timestamp).tzinfo)
        self.assertEqual(len(timestamp.split(".")[1].split("+")[0]), 3)

    def test_presence_roundtrip_and_invalid_versions(self):
        presence = wire.Presence(
            peer_id=uuid4(), name="Test", tcp_port=1234, status="online"
        )
        self.assertEqual(wire.decode_presence(wire.encode_presence(presence)), presence)
        header = json.loads(wire.encode_presence(presence))
        for version in (None, True, "1", 2):
            with self.subTest(version=version), self.assertRaises(wire.ProtocolError):
                wire.decode_presence(json.dumps({**header, "version": version}).encode())
        del header["version"]
        with self.assertRaises(wire.ProtocolError):
            wire.decode_presence(json.dumps(header).encode())

    def test_message_body_limits_and_unicode(self):
        self.assertEqual(wire.message_body("Xin chào"), "Xin chào".encode())
        self.assertEqual(len(wire.message_body("a" * wire.Max_message)), wire.Max_message)
        for text in ("   ", "a" * (wire.Max_message + 1), "\ud800"):
            with self.subTest(text=text[:10]), self.assertRaises(wire.ProtocolError):
                wire.message_body(text)

    def test_tcp_roundtrip(self):
        body = wire.message_body("Xin chào")
        for frame, payload in (
            (wire.Bye(), b""),
            (wire.ChatMessage(message_id=uuid4(), body_length=len(body)), body),
            (wire.FileChunk(file_id=uuid4(), body_length=1024), b"x" * 1024),
        ):
            sender, receiver = socket.socketpair()
            with self.subTest(frame=frame.type), sender, receiver:
                wire.send_frame(sender, frame, payload)
                self.assertEqual(wire.read_frame(receiver), (frame, payload))

    def read_raw_header(self, raw):
        sender, receiver = socket.socketpair()
        with sender, receiver:
            sender.sendall(struct.pack("!I", len(raw)) + raw)
            return wire.read_frame(receiver)

    def test_invalid_tcp_headers(self):
        for header in (
            [], {},
            {"type": "bye", "version": True, "body_length": 0},
            {"type": "bye", "version": 1, "body_length": False},
            {"type": "bye", "version": 1},
            {"type": "unknown", "version": 1, "body_length": 0},
        ):
            with self.subTest(header=header), self.assertRaises(wire.ProtocolError):
                self.read_raw_header(json.dumps(header).encode())

    def test_maximum_header_size_is_accepted(self):
        raw = wire.Bye().model_dump_json().encode()
        raw += b" " * (wire.Max_header - len(raw))
        self.assertEqual(self.read_raw_header(raw), (wire.Bye(), b""))

    def test_send_rejects_mismatched_payload(self):
        with self.assertRaises(wire.ProtocolError):
            wire.send_frame(None, wire.Bye(), b"unexpected")

    def test_read_timeout_restores_socket_timeout(self):
        sender, receiver = socket.socketpair()
        with sender, receiver, patch.object(wire, "IO_TIMEOUT", 0.03):
            for original_timeout in (None, 0.01):
                receiver.settimeout(original_timeout)
                with self.subTest(timeout=original_timeout), self.assertRaises(TimeoutError):
                    wire.recv_exact(receiver, 1)
                self.assertEqual(receiver.gettimeout(), original_timeout)

    def test_idle_waits_then_reads_fragmented_data(self):
        sock = Mock()
        sock.gettimeout.return_value = 0.01
        sock.recv.side_effect = [socket.timeout(), b"a", b"b"]
        self.assertEqual(wire.recv_exact(sock, 2, idle=True), b"ab")

    def test_partial_read_times_out_and_closed_socket_raises_eof(self):
        sender, receiver = socket.socketpair()
        with sender, receiver, patch.object(wire, "IO_TIMEOUT", 0.03):
            sender.sendall(b"a")
            receiver.settimeout(0.01)
            with self.assertRaises(TimeoutError):
                wire.recv_exact(receiver, 2)
            sender.close()
            with self.assertRaises(EOFError):
                wire.recv_exact(receiver, 1)


if __name__ == "__main__":
    unittest.main()
