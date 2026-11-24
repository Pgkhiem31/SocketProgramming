"""Kiểm thử LAN Service bằng TCP thật và adapter chat/file giả lập."""
import socket
import tempfile
import time
import unittest
from pathlib import Path
from threading import Event
from unittest.mock import patch
from uuid import UUID, uuid4

from backend.app_config import Config
from backend.app_errors import LanError
from backend.lan_service import LanService
from backend.wire_protocol import ChatAck, ChatMessage, Presence, encode_presence, message_body


class FakeDiscovery:
    # Không phát UDP trong test để tránh phụ thuộc firewall hoặc mạng thật.
    def __init__(self, config, ip, broadcast, registry, presence, network_error):
        self.registry = registry
        self.presence = presence
        self.refreshed = False
        self.closed = False

    def start(self): pass
    def refresh(self): self.refreshed = True
    def close(self): self.closed = True


class FakeChat:
    # Adapter test, không phải code chat production của đồng đội.
    def __init__(self, store, emit):
        self.store = store
        self.emit = emit
        self.acked = Event()

    def send(self, session, text):
        mid = uuid4()
        body = message_body(text)
        self.store.save_message(session.peer_id, str(mid), text, "outgoing", "pending")
        session.send(ChatMessage(message_id=mid, body_length=len(body)), body)
        return str(mid)

    def handle(self, session, frame, body):
        if isinstance(frame, ChatMessage):
            self.store.store_received(session.peer_id, str(frame.message_id), body.decode("utf-8"))
            session.send(ChatAck(message_id=frame.message_id))
        elif isinstance(frame, ChatAck):
            self.store.update_message_status(session.peer_id, str(frame.message_id), "delivered")
            self.acked.set()

    def session_closed(self, session): pass


class FakeFile:
    # Chỉ kiểm tra socket file và routing, không kiểm thử truyền tệp thực tế.
    def __init__(self, config, store, emit):
        self.received = Event()

    def send(self, manager, host, port, path, peer_id):
        with manager.open_file(host, port, peer_id) as (sock, _):
            sock.sendall(b"X")
        return "fake-file-id"

    def receive(self, sock, hello):
        if sock.recv(1) == b"X":
            self.received.set()


class LanServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.patch = patch("backend.lan_service.Discovery", FakeDiscovery)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def service(self, name, adapters=True, start=True):
        root = Path(self.tmp.name) / name
        config = Config(data_dir=root / "db", downloads=root / "downloads",
                        interface="127.0.0.1", tcp_port=0, name=name)
        service = LanService(config, chat_factory=FakeChat if adapters else None,
                             file_factory=FakeFile if adapters else None)
        self.addCleanup(service.close)
        if start:
            service.start()
        return service

    @staticmethod
    def discover(a, b):
        # Bơm gói presence đúng giao thức vào Registry, không dùng mạng UDP thật.
        a._registry.ingest(encode_presence(b._presence()), "127.0.0.1")

    def pair(self):
        a, b = self.service("A"), self.service("B")
        self.discover(a, b)
        self.discover(b, a)
        return a, b

    def test_start_and_stop(self):
        a = self.service("A")
        port = a.start()
        self.assertGreater(port, 0)
        self.assertEqual(a._presence().tcp_port, port)
        discovery = a._discovery
        a.close()
        self.assertTrue(discovery.closed)
        with self.assertRaises(LanError) as cm:
            a.get_peers()
        self.assertEqual(cm.exception.code, "network_not_started")

    def test_profile_update_and_persistence(self):
        a = self.service("A")
        pid = a.get_profile()["peer_id"]
        a.update_profile("Tên mới", "blue", "away")
        self.assertEqual(a._presence().name, "Tên mới")
        self.assertTrue(a._discovery.refreshed)
        self.assertEqual(a.store.get_profile()["peer_id"], pid)
        self.assertEqual(a.store.get_profile()["status"], "away")

    def test_discovery_and_offline(self):
        a, b = self.pair()
        pid = b.get_profile()["peer_id"]
        self.assertEqual(a.get_peers()[0]["peer_id"], pid)
        a._registry.expire(time.monotonic() + 100)
        self.assertEqual(a.get_peers(), [])
        with self.assertRaises(LanError) as cm:
            a.connect(pid)
        self.assertEqual(cm.exception.code, "peer_offline")

    def test_self_connection(self):
        a = self.service("A")
        with self.assertRaises(LanError) as cm:
            a.connect(a.get_profile()["peer_id"])
        self.assertEqual(cm.exception.code, "self_connection")

    def test_chat_routing_and_storage(self):
        a, b = self.pair()
        pid = b.get_profile()["peer_id"]
        own = a.get_profile()["peer_id"]
        a.connect(pid)
        mid = a.send_message(pid, "Xin chào")
        self.assertTrue(a._chat.acked.wait(3))
        self.assertEqual(a.get_history(pid)[0]["message_id"], mid)
        self.assertEqual(a.get_history(pid)[0]["status"], "delivered")
        self.assertEqual(b.get_history(own)[0]["text"], "Xin chào")
        self.assertEqual(b.get_peers()[0]["unread_count"], 1)
        b.mark_read(own)
        self.assertEqual(b.get_peers()[0]["unread_count"], 0)

    def test_leave_requires_manual_reconnect(self):
        a, b = self.pair()
        pid = b.get_profile()["peer_id"]
        a.connect(pid)
        a.leave(pid)
        with self.assertRaises(LanError) as cm:
            a.send_message(pid, "test")
        self.assertEqual(cm.exception.code, "not_connected")

    def test_file_routing_without_chat(self):
        a, b = self.pair()
        pid = b.get_profile()["peer_id"]
        self.assertEqual(a.send_file(pid, "test.txt"), "fake-file-id")
        self.assertTrue(b._file.received.wait(3))
        self.assertIsNone(a._tcp.get(pid))

    def test_missing_adapters(self):
        a = self.service("A", adapters=False)
        for call, code in ((lambda: a.send_message("unknown", "hi"), "chat_unavailable"),
                           (lambda: a.send_file("unknown", "test.txt"), "file_unavailable")):
            with self.assertRaises(LanError) as cm:
                call()
            self.assertEqual(cm.exception.code, code)

    def test_event_queue(self):
        a, b = self.pair()
        events = a.drain_events()
        self.assertIn("network_started", [e["event"] for e in events])
        self.assertIn("peer_online", [e["event"] for e in events])
        self.assertEqual(a.drain_events(), [])

    def test_start_failure_cleans_tcp(self):
        a = self.service("A", start=False)
        with patch("backend.lan_service.Discovery", side_effect=OSError("test")):
            with self.assertRaises(LanError) as cm:
                a.start()
        self.assertEqual(cm.exception.code, "network_start_failed")
        self.assertIsNone(a._tcp)
        self.assertGreater(a.start(), 0)


if __name__ == "__main__":
    unittest.main()
