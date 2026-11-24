"""Lưu dữ liệu cục bộ cho ứng dụng LAN bằng SQLite."""
from __future__ import annotations

import sqlite3
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from uuid import uuid4

# Kiểu dữ liệu dùng chung với UDP Discovery.
Record = dict[str, Any]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class SQLiteStore:
    def __init__(self, data_dir: str | Path):
        # Nhận thư mục Config.data_dir, không phải đường dẫn file DB.
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.path = self.data_dir / "lan.sqlite3"
        self._initialize()

    @contextmanager
    def _db(self) -> Iterator[sqlite3.Connection]:
        # Mỗi thao tác dùng kết nối riêng để các thread không chia sẻ connection.
        with closing(sqlite3.connect(self.path, timeout=5.0)) as conn:
            conn.row_factory = sqlite3.Row
            with conn:
                yield conn  # Tự commit nếu thành công, rollback nếu lỗi.

    def _initialize(self) -> None:
        with self._db() as db:
            # WAL hỗ trợ đọc và ghi đồng thời; SQLite vẫn chỉ có một writer.
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS profile (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    peer_id TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    avatar TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('online','away'))
                );
                CREATE TABLE IF NOT EXISTS messages (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    peer_id TEXT NOT NULL,
                    message_id TEXT NOT NULL,
                    direction TEXT NOT NULL CHECK (direction IN ('incoming','outgoing')),
                    text TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('pending','delivered','failed','received')),
                    UNIQUE (peer_id, message_id, direction)
                );
                CREATE INDEX IF NOT EXISTS idx_messages_peer_seq
                    ON messages(peer_id, seq);
                CREATE TABLE IF NOT EXISTS read_cursors (
                    peer_id TEXT PRIMARY KEY,
                    last_read_seq INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS transfers (
                    peer_id TEXT NOT NULL,
                    file_id TEXT NOT NULL,
                    direction TEXT NOT NULL CHECK (direction IN ('incoming','outgoing')),
                    filename TEXT NOT NULL,
                    size INTEGER NOT NULL CHECK (size >= 0),
                    transferred INTEGER NOT NULL DEFAULT 0 CHECK (transferred >= 0 AND transferred <= size),
                    sha256 TEXT,
                    path TEXT,
                    status TEXT NOT NULL CHECK (status IN ('pending','transferring','completed','failed')),
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (peer_id, file_id, direction)
                );
            """)

    def get_profile(self, name: str = "User") -> Record:
        # Chỉ tạo Peer ID lần đầu; lần mở sau vẫn dùng ID cũ.
        self._validate_profile(name, "teal", "online")
        with self._db() as db:
            db.execute("INSERT OR IGNORE INTO profile VALUES (1, ?, ?, 'teal', 'online')",
                       (str(uuid4()), name.strip()))
            return dict(db.execute("SELECT peer_id,name,avatar,status FROM profile WHERE id=1").fetchone())

    @staticmethod
    def _validate_profile(name: str, avatar: str, status: str) -> None:
        # Đồng nhất giới hạn với Presence trong wire_protocol.py.
        if not 1 <= len(name.strip()) <= 40 or not name.strip().isprintable():
            raise ValueError("invalid_name")
        if avatar not in {"teal", "blue", "amber", "rose"}:
            raise ValueError("invalid_avatar")
        if status not in {"online", "away"}:
            raise ValueError("invalid_status")

    def update_profile(self, name: str, avatar: str = "teal", status: str = "online") -> Record:
        self._validate_profile(name, avatar, status)
        with self._db() as db:
            db.execute("""INSERT INTO profile VALUES (1, ?, ?, ?, ?)
                       ON CONFLICT(id) DO UPDATE SET name=excluded.name,
                       avatar=excluded.avatar,status=excluded.status""",
                       (str(uuid4()), name.strip(), avatar, status))
            return dict(db.execute("SELECT peer_id,name,avatar,status FROM profile WHERE id=1").fetchone())

    def save_message(self, peer_id: str, message_id: str, text: str,
                     direction: str, status: str, *, created_at: str | None = None) -> bool:
        # ID trùng không tạo thêm tin nhắn; nội dung khác với cùng ID là lỗi.
        if direction not in {"incoming", "outgoing"}:
            raise ValueError("invalid_direction")
        if status not in ({"received"} if direction == "incoming" else {"pending", "delivered", "failed"}):
            raise ValueError("invalid_status")
        if not text.strip() or len(text.encode("utf-8")) > 16 * 1024:
            raise ValueError("invalid_message")
        peer_id, message_id = str(peer_id), str(message_id)
        with self._db() as db:
            result = db.execute("""INSERT INTO messages
                (peer_id,message_id,direction,text,created_at,status) VALUES (?,?,?,?,?,?)
                ON CONFLICT(peer_id,message_id,direction) DO NOTHING""",
                (peer_id, message_id, direction, text, created_at or _now(), status))
            if result.rowcount == 1:
                return True
            existing = db.execute("""SELECT text FROM messages
                WHERE peer_id=? AND message_id=? AND direction=?""",
                (peer_id, message_id, direction)).fetchone()
            if existing["text"] != text:
                raise ValueError("message_id_conflict")
            return False

    def store_received(self, peer_id: str, message_id: str, text: str) -> bool:
        # Chat chỉ gửi ACK sau khi hàm này hoàn tất, tức DB đã commit.
        return self.save_message(peer_id, message_id, text, "incoming", "received")

    def update_message_status(self, peer_id: str, message_id: str, status: str) -> bool:
        # Không thay đổi tin nhận hoặc hạ trạng thái delivered về failed.
        if status not in {"pending", "delivered", "failed"}:
            raise ValueError("invalid_status")
        with self._db() as db:
            result = db.execute("""UPDATE messages SET status=?
                WHERE peer_id=? AND message_id=? AND direction='outgoing'
                AND (status!='delivered' OR ?='delivered')""",
                (status, str(peer_id), str(message_id), status))
            return result.rowcount == 1

    def get_history(self, peer_id: str, limit: int = 100, *, before_seq: int | None = None) -> list[Record]:
        # Lấy trang tin mới nhất, trả lại theo thứ tự cũ đến mới.
        if not 1 <= limit <= 1000:
            raise ValueError("invalid_limit")
        with self._db() as db:
            rows = db.execute("""SELECT * FROM messages WHERE peer_id=?
                AND (? IS NULL OR seq < ?) ORDER BY seq DESC LIMIT ?""",
                (str(peer_id), before_seq, before_seq, limit)).fetchall()
            return [dict(row) for row in reversed(rows)]

    def mark_read(self, peer_id: str, through_seq: int | None = None) -> int:
        # Mốc đọc chỉ tiến về trước; không đánh dấu tin tương lai.
        peer_id = str(peer_id)
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            last = db.execute("""SELECT COALESCE(MAX(seq),0) FROM messages
                WHERE peer_id=? AND (? IS NULL OR seq <= ?)""",
                (peer_id, through_seq, through_seq)).fetchone()[0]
            db.execute("""INSERT INTO read_cursors VALUES (?,?) ON CONFLICT(peer_id)
                DO UPDATE SET last_read_seq=MAX(last_read_seq,excluded.last_read_seq)""", (peer_id, last))
            return db.execute("SELECT last_read_seq FROM read_cursors WHERE peer_id=?", (peer_id,)).fetchone()[0]

    def unread_count(self, peer_id: str) -> int:
        # Chỉ đếm tin đến sau mốc đọc, không đếm tin do mình gửi.
        with self._db() as db:
            return db.execute("""SELECT COUNT(*) FROM messages WHERE peer_id=?
                AND direction='incoming' AND seq > COALESCE(
                (SELECT last_read_seq FROM read_cursors WHERE peer_id=?),0)""",
                (str(peer_id), str(peer_id))).fetchone()[0]

    def create_transfer(self, peer_id: str, file_id: str, direction: str,
                        filename: str, size: int, *, sha256: str | None = None) -> bool:
        # Lưu metadata, không lưu nội dung file vào database.
        stamp = _now()
        with self._db() as db:
            result = db.execute("""INSERT INTO transfers
                (peer_id,file_id,direction,filename,size,sha256,status,created_at,updated_at)
                VALUES (?,?,?,?,?,?,'pending',?,?)
                ON CONFLICT(peer_id,file_id,direction) DO NOTHING""",
                (str(peer_id), str(file_id), direction, filename, size, sha256, stamp, stamp))
            return result.rowcount == 1

    def update_transfer(self, peer_id: str, file_id: str, direction: str,
                        transferred: int, status: str, *, path: str | Path | None = None) -> bool:
        # Chỉ đánh dấu completed khi toàn bộ byte đã được truyền.
        with self._db() as db:
            result = db.execute("""UPDATE transfers SET transferred=?,status=?,
                path=COALESCE(?,path),updated_at=? WHERE peer_id=? AND file_id=? AND direction=?
                AND transferred <= ? AND (status!='completed' OR ?='completed')
                AND (?!='completed' OR size=?)""",
                (transferred, status, str(path) if path is not None else None, _now(),
                 str(peer_id), str(file_id), direction, transferred, status, status, transferred))
            return result.rowcount == 1

    def get_transfers(self, peer_id: str, limit: int = 100) -> list[Record]:
        if not 1 <= limit <= 1000:
            raise ValueError("invalid_limit")
        with self._db() as db:
            rows = db.execute("""SELECT * FROM transfers WHERE peer_id=?
                ORDER BY created_at DESC, file_id DESC LIMIT ?""", (str(peer_id), limit)).fetchall()
            return [dict(row) for row in rows]
