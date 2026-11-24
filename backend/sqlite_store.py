"""Lưu lịch sử chat cục bộ bằng SQLite."""
from __future__ import annotations

import sqlite3
from pathlib import Path
from threading import RLock
from typing import TypedDict


class Record(TypedDict):
    """Cấu trúc một tin nhắn trả về cho service và giao diện."""

    message_id: str
    peer_id: str
    peer_name: str
    text: str
    direction: str
    state: str
    timestamp: float
    error: str | None


class MessageIdConflictError(ValueError):
    """Một message_id đã tồn tại nhưng nội dung/peer không khớp."""


class SQLiteStore:
    """Store nhỏ gọn phục vụ lịch sử và trạng thái tin nhắn chat."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        # Tạo thư mục chứa database nếu chưa tồn tại.
        if str(path) != ":memory:":
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = RLock()
        # Một kết nối dùng chung được bảo vệ bằng lock vì callback chạy đa luồng.
        self._connection = sqlite3.connect(
            str(path), timeout=10.0, check_same_thread=False
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._create_tables()

    def _create_tables(self) -> None:
        """Tạo bảng lịch sử khi chạy lần đầu."""
        with self._lock, self._connection:
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS chat_messages (
                    message_id TEXT PRIMARY KEY,
                    peer_id TEXT NOT NULL,
                    peer_name TEXT NOT NULL,
                    text TEXT NOT NULL,
                    direction TEXT NOT NULL CHECK(direction IN ('in', 'out')),
                    state TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    error TEXT
                )
                """
            )
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS ix_chat_peer_time "
                "ON chat_messages(peer_id, timestamp, message_id)"
            )

    @staticmethod
    def _record(row: sqlite3.Row | None) -> Record | None:
        """Chuyển kết quả SQLite thành dict thông thường."""
        return dict(row) if row is not None else None  # type: ignore[return-value]

    def insert_message(self, *, message_id: str, peer_id: str,
                       peer_name: str, text: str, direction: str,
                       state: str, timestamp: float,
                       error: str | None = None) -> tuple[Record, bool]:
        """Lưu tin mới; trả về (tin nhắn, có_phải_vừa_tạo_mới)."""
        if direction not in ("in", "out"):
            raise ValueError("direction phải là 'in' hoặc 'out'")
        with self._lock, self._connection:
            cursor = self._connection.execute(
                """INSERT OR IGNORE INTO chat_messages
                   (message_id, peer_id, peer_name, text, direction, state,
                    timestamp, error)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (message_id, peer_id, peer_name, text, direction,
                 state, timestamp, error),
            )
            row = self._connection.execute(
                "SELECT * FROM chat_messages WHERE message_id = ?",
                (message_id,),
            ).fetchone()
        record = self._record(row)
        if record is None:
            raise RuntimeError("Không thể đọc lại tin nhắn vừa lưu")
        # Chặn việc dùng lại ID của tin khác để ACK nhầm hoặc ghi đè lịch sử.
        if not cursor.rowcount and (
            record["peer_id"] != peer_id
            or record["direction"] != direction
            or record["text"] != text
        ):
            raise MessageIdConflictError("message_id đã được dùng cho tin khác")
        return record, cursor.rowcount == 1

    def get_message(self, message_id: str) -> Record | None:
        """Tìm một tin theo ID duy nhất."""
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM chat_messages WHERE message_id = ?",
                (message_id,),
            ).fetchone()
        return self._record(row)

    def set_message_state(self, message_id: str, state: str,
                          error: str | None = None) -> Record | None:
        """Cập nhật trạng thái gửi như sending, sent hoặc failed."""
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE chat_messages SET state = ?, error = ? "
                "WHERE message_id = ?",
                (state, error, message_id),
            )
            row = self._connection.execute(
                "SELECT * FROM chat_messages WHERE message_id = ?",
                (message_id,),
            ).fetchone()
        return self._record(row)

    def history(self, peer_id: str) -> list[Record]:
        """Đọc lịch sử của một peer theo thời gian phát sinh."""
        with self._lock:
            rows = self._connection.execute(
                """SELECT * FROM chat_messages WHERE peer_id = ?
                   ORDER BY timestamp, rowid""",
                (str(peer_id),),
            ).fetchall()
        return [self._record(row) for row in rows]  # type: ignore[misc]

    def close(self) -> None:
        """Đóng kết nối database khi ứng dụng kết thúc."""
        with self._lock:
            self._connection.close()

