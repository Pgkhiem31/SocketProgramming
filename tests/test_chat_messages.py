"""Kiểm thử tích hợp chat qua TCP và lưu SQLite."""
import queue
import tempfile
import time
import unittest
from pathlib import Path
from uuid import uuid4

from backend.app_errors import EmptyMessageError, MessageTooLongError, PeerOfflineError
from backend.lan_service import LanService


class ChatMessagesTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.events_a = queue.Queue()
        self.events_b = queue.Queue()
        root = Path(self.temp_dir.name)
        self.a = LanService(
            "A", host="127.0.0.1", tcp_port=0,
            database_path=root / "a.sqlite3",
            on_event=lambda event, data: self.events_a.put((event, data)),
        )
        self.b = LanService(
            "B", host="127.0.0.1", tcp_port=0,
            database_path=root / "b.sqlite3",
            on_event=lambda event, data: self.events_b.put((event, data)),
        )
        self.addCleanup(self.a.close)
        self.addCleanup(self.b.close)
        self.a.start()
        self.b.start()
        self.a.connect({
            "peer_id": str(self.b.peer_id), "name": "B",
            "ip": "127.0.0.1", "tcp_port": self.b.tcp_port,
        })

    def wait_until(self, predicate, timeout=3.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = predicate()
            if result:
                return result
            time.sleep(0.01)
        self.fail("Hết thời gian chờ kết quả chat")

    def test_send_receive_and_ack_update_both_histories(self):
        outgoing = self.a.send_message(str(self.b.peer_id), "Xin chào B")

        received = self.wait_until(lambda: self.b.history(str(self.a.peer_id)))
        sent = self.wait_until(
            lambda: next((item for item in self.a.history(str(self.b.peer_id))
                          if item["message_id"] == outgoing["message_id"]
                          and item["state"] == "sent"), None)
        )

        self.assertEqual(received[0]["text"], "Xin chào B")
        self.assertEqual(received[0]["direction"], "in")
        self.assertEqual(sent["direction"], "out")
        self.assertEqual(sent["error"], None)

    def test_invalid_messages_raise_typed_errors(self):
        with self.assertRaises(EmptyMessageError):
            self.a.send_message(str(self.b.peer_id), "   ")
        with self.assertRaises(MessageTooLongError):
            self.a.send_message(str(self.b.peer_id), "x" * (16 * 1024 + 1))

    def test_sending_without_a_session_raises_peer_offline(self):
        with self.assertRaises(PeerOfflineError):
            self.a.send_message(str(uuid4()), "Xin chào")

    def test_receive_duplicate_message_saves_and_emits_once_but_acks_twice(self):
        # Dùng một mock session để gửi lại chính xác cùng frame hai lần.
        from unittest.mock import Mock
        from backend.wire_protocol import ChatMessage

        session = Mock()
        session.peer_id = str(uuid4())
        session.remote.name = "Peer"
        message_id = uuid4()
        text = "Tin chỉ lưu một lần"
        body = text.encode("utf-8")
        frame = ChatMessage(message_id=message_id, body_length=len(body))

        self.b.chat.receive(session, frame, body)
        self.b.chat.receive(session, frame, body)

        self.assertEqual(len(self.b.history(session.peer_id)), 1)
        self.assertEqual(session.send.call_count, 2)
        events = []
        while not self.events_b.empty():
            events.append(self.events_b.get_nowait())
        received_events = [item for item in events if item[0] == "message_received"]
        self.assertEqual(len(received_events), 1)


if __name__ == "__main__":
    unittest.main()
