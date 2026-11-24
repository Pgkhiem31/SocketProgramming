"""Kiểm thử SQLite độc lập, không cần TCP, UDP hay GUI."""
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from backend.sqlite_store import SQLiteStore


class SQLiteStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = SQLiteStore(Path(self.temp.name))

    def test_profile_persistence(self):
        # Peer ID không đổi khi khởi động lại ứng dụng.
        first = self.store.get_profile("An")
        updated = self.store.update_profile("Bình", "blue", "away")
        self.assertEqual(first["peer_id"], updated["peer_id"])
        self.assertEqual(SQLiteStore(self.temp.name).get_profile(), updated)

    def test_profile_validation(self):
        with self.assertRaises(ValueError):
            self.store.update_profile(" ")

    def test_unicode_history(self):
        self.store.store_received("peer-a", "m1", "Xin chào 😊")
        self.assertEqual(SQLiteStore(self.temp.name).get_history("peer-a")[0]["text"], "Xin chào 😊")
        self.assertEqual(self.store.get_history("peer-b"), [])

    def test_duplicate_message(self):
        self.assertTrue(self.store.store_received("a", "m", "hello"))
        self.assertFalse(self.store.store_received("a", "m", "hello"))
        with self.assertRaises(ValueError):
            self.store.store_received("a", "m", "different")
        self.assertEqual(self.store.unread_count("a"), 1)

    def test_delivery_status(self):
        self.store.save_message("a", "m", "hello", "outgoing", "pending")
        self.assertTrue(self.store.update_message_status("a", "m", "delivered"))
        self.assertFalse(self.store.update_message_status("a", "m", "failed"))
        self.assertEqual(self.store.get_history("a")[0]["status"], "delivered")

    def test_read_cursor(self):
        self.store.store_received("a", "1", "one")
        seq = self.store.get_history("a")[0]["seq"]
        self.store.store_received("a", "2", "two")
        self.store.save_message("a", "3", "out", "outgoing", "pending")
        self.assertEqual(self.store.unread_count("a"), 2)
        self.store.mark_read("a", seq)
        self.assertEqual(self.store.unread_count("a"), 1)
        self.store.mark_read("a")
        self.store.mark_read("a", seq)
        self.assertEqual(self.store.unread_count("a"), 0)
        self.store.store_received("a", "4", "new")
        self.assertEqual(self.store.unread_count("a"), 1)

    def test_history_pagination(self):
        for i in range(5):
            self.store.store_received("a", str(i), str(i))
        page = self.store.get_history("a", 2)
        self.assertEqual([r["text"] for r in page], ["3", "4"])
        older = self.store.get_history("a", 2, before_seq=page[0]["seq"])
        self.assertEqual([r["text"] for r in older], ["1", "2"])

    def test_transfer_progress(self):
        self.assertTrue(self.store.create_transfer("a", "f", "incoming", "demo.txt", 10))
        self.assertTrue(self.store.update_transfer("a", "f", "incoming", 5, "transferring"))
        self.assertFalse(self.store.update_transfer("a", "f", "incoming", 5, "completed"))
        self.assertTrue(self.store.update_transfer("a", "f", "incoming", 10, "completed", path="downloads/demo.txt"))
        self.assertEqual(self.store.get_transfers("a")[0]["status"], "completed")

    def test_concurrent_writes(self):
        # Mô phỏng nhiều TCP worker ghi dữ liệu cùng lúc.
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda i: self.store.store_received("a", str(i), f"Tin {i}"), range(40)))
        self.assertTrue(all(results))
        self.assertEqual(self.store.unread_count("a"), 40)

    def test_parameterized_sql(self):
        text = "'); DROP TABLE messages; --"
        self.store.store_received("a", "m", text)
        self.assertEqual(self.store.get_history("a")[0]["text"], text)


if __name__ == "__main__":
    unittest.main()
