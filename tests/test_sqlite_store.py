"""Kiểm thử lưu lịch sử chat bằng SQLite."""
import tempfile
import unittest
from pathlib import Path

from backend.sqlite_store import MessageIdConflictError, SQLiteStore


class SQLiteStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.database = Path(self.temp_dir.name) / "chat.sqlite3"
        self.store = SQLiteStore(self.database)
        self.addCleanup(self.store.close)

    def insert(self, *, message_id="msg-1", peer_id="peer-1", text="Xin chào",
               direction="out", state="sending"):
        return self.store.insert_message(
            message_id=message_id,
            peer_id=peer_id,
            peer_name="Minh",
            text=text,
            direction=direction,
            state=state,
            timestamp=100.0,
        )

    def test_insert_and_read_message(self):
        message, inserted = self.insert()

        self.assertTrue(inserted)
        self.assertEqual(message["message_id"], "msg-1")
        self.assertEqual(message["peer_id"], "peer-1")
        self.assertEqual(message["text"], "Xin chào")
        self.assertEqual(self.store.get_message("msg-1"), message)
        self.assertEqual(self.store.history("peer-1"), [message])

    def test_state_update_is_saved(self):
        self.insert()

        updated = self.store.set_message_state("msg-1", "sent")

        self.assertIsNotNone(updated)
        self.assertEqual(updated["state"], "sent")
        self.assertIsNone(updated["error"])

    def test_same_message_id_and_content_is_idempotent(self):
        original, inserted = self.insert()
        duplicate, inserted_again = self.insert()

        self.assertTrue(inserted)
        self.assertFalse(inserted_again)
        self.assertEqual(duplicate, original)
        self.assertEqual(len(self.store.history("peer-1")), 1)

    def test_conflicting_message_id_is_rejected(self):
        self.insert()

        with self.assertRaises(MessageIdConflictError):
            self.insert(peer_id="another-peer", text="Nội dung khác")

    def test_history_only_returns_selected_peer(self):
        self.insert(message_id="a", peer_id="peer-a")
        self.insert(message_id="b", peer_id="peer-b")

        self.assertEqual([m["message_id"] for m in self.store.history("peer-a")], ["a"])

    def test_messages_survive_reopening_database(self):
        self.insert()
        self.store.close()

        reopened = SQLiteStore(self.database)
        self.addCleanup(reopened.close)
        self.assertEqual(reopened.history("peer-1")[0]["text"], "Xin chào")

    def test_memory_database_is_supported(self):
        store = SQLiteStore(":memory:")
        self.addCleanup(store.close)
        store.insert_message(
            message_id="memory", peer_id="peer", peer_name="Lan",
            text="test", direction="in", state="received", timestamp=1.0,
        )
        self.assertEqual(len(store.history("peer")), 1)


if __name__ == "__main__":
    unittest.main()
