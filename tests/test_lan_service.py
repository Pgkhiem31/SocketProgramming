"""Kiểm thử API và vòng đời LanService."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from backend.app_errors import (
    ConnectionFailedError,
    InvalidNameError,
    InvalidPeerError,
    ListenerStartError,
    SelfConnectionError,
    ServiceNotStartedError,
)
from backend.lan_service import LanService


class LanServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.service = self.make_service("Test")
        self.addCleanup(self.service.close)

    def make_service(self, name, **kwargs):
        database_path = Path(self.temp_dir.name) / f"{name}.sqlite3"
        return LanService(name, host="127.0.0.1", tcp_port=0,
                          database_path=database_path, **kwargs)

    def test_start_returns_dynamic_port_and_is_idempotent(self):
        first_port = self.service.start()
        second_port = self.service.start()

        self.assertGreater(first_port, 0)
        self.assertEqual(first_port, second_port)
        self.assertEqual(self.service.tcp_port, first_port)

    def test_connect_requires_service_to_be_started(self):
        with self.assertRaises(ServiceNotStartedError):
            self.service.connect({
                "peer_id": str(uuid4()), "name": "Peer",
                "ip": "127.0.0.1", "tcp_port": 12345,
            })

    def test_invalid_peer_data_is_rejected(self):
        self.service.start()

        for peer in (
            {},
            {"peer_id": "not-a-uuid", "ip": "127.0.0.1", "tcp_port": 1234},
            {"peer_id": str(uuid4()), "ip": "127.0.0.1", "tcp_port": 70000},
        ):
            with self.subTest(peer=peer), self.assertRaises(InvalidPeerError):
                self.service.connect(peer)

    def test_connect_to_self_is_rejected(self):
        self.service.start()
        with self.assertRaises(SelfConnectionError):
            self.service.connect({
                "peer_id": str(self.service.peer_id), "name": "Test",
                "ip": "127.0.0.1", "tcp_port": self.service.tcp_port,
            })

    def test_connection_failure_is_reported_as_typed_error(self):
        self.service.start()
        # Giữ port tạm thời bị bind nhưng không listen để ép connect() thất bại.
        import socket
        blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        blocker.bind(("127.0.0.1", 0))
        port = blocker.getsockname()[1]
        blocker.close()

        with self.assertRaises(ConnectionFailedError):
            self.service.connect({
                "peer_id": str(uuid4()), "name": "Offline",
                "ip": "127.0.0.1", "tcp_port": port,
            })

    def test_failed_start_does_not_mark_service_as_started(self):
        with patch.object(self.service.sessions, "start", side_effect=OSError("bind failed")):
            with self.assertRaises(ListenerStartError):
                self.service.start()

        with self.assertRaises(ServiceNotStartedError):
            self.service.connect({
                "peer_id": str(uuid4()), "name": "Peer",
                "ip": "127.0.0.1", "tcp_port": 1234,
            })

    def test_empty_user_name_is_rejected(self):
        with self.assertRaises(InvalidNameError):
            self.make_service("   ")


if __name__ == "__main__":
    unittest.main()
