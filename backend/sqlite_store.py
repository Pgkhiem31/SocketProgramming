"""One SQLite connection, serialized by the store lock."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from threading import RLock
from typing import Any
from uuid import uuid4

from .app_errors import LanError
from .wire_protocol import now

Record = dict[str, Any]


class Store:
    def __init__(self, directory: Path):
        directory.mkdir(parents=True, exist_ok=True)
        self.lock = RLock()
        # Nhiều luồng dùng chung kết nối; khóa của Store tuần tự hóa các thao tác.
        self.db = sqlite3.connect(directory / "lanlink.sqlite3", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        with self.db:
            self.db.executescript("""
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS conversations (
                    peer_id TEXT PRIMARY KEY, name TEXT NOT NULL, avatar TEXT NOT NULL,
                    ip TEXT NOT NULL, tcp_port INTEGER NOT NULL, read_through INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS messages (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, peer_id TEXT NOT NULL,
                    message_id TEXT NOT NULL, text TEXT NOT NULL, direction TEXT NOT NULL,
                    state TEXT NOT NULL, timestamp TEXT NOT NULL, error TEXT,
                    UNIQUE(peer_id, direction, message_id)
                );
                CREATE INDEX IF NOT EXISTS message_history ON messages(peer_id, seq);
                CREATE TABLE IF NOT EXISTS transfers (
                    transfer_id TEXT PRIMARY KEY, peer_id TEXT NOT NULL, direction TEXT NOT NULL,
                    filename TEXT NOT NULL, size INTEGER NOT NULL, transferred INTEGER NOT NULL,
                    state TEXT NOT NULL, timestamp TEXT NOT NULL, error TEXT, saved_name TEXT
                );
            """)
            self.db.execute("INSERT OR IGNORE INTO settings VALUES ('peer_id', ?)", (str(uuid4()),))
            # Đánh dấu công việc còn dang dở từ phiên trước là bị gián đoạn.
            self.db.execute("UPDATE messages SET state='failed', error='interrupted' WHERE state='sending'")
            self.db.execute(
                "UPDATE transfers SET state='failed', error='interrupted' "
                "WHERE state NOT IN ('completed', 'failed')"
            )

    def settings(self) -> Record:
        with self.lock:
            return {row["key"]: row["value"] for row in self.db.execute("SELECT * FROM settings")}

    def save_settings(self, **values: str) -> None:
        with self.lock, self.db:
            self.db.executemany(
                "INSERT INTO settings VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                values.items(),
            )

    def _remember_peer(self, peer: Record) -> None:
        self.db.execute("""
            INSERT INTO conversations(peer_id, name, avatar, ip, tcp_port)
            VALUES (:peer_id, :name, :avatar, :ip, :tcp_port)
            ON CONFLICT(peer_id) DO UPDATE SET
                name=excluded.name, avatar=excluded.avatar, ip=excluded.ip, tcp_port=excluded.tcp_port
            """, peer)

    def update_known_peer(self, peer: Record) -> None:
        with self.lock, self.db:
            self.db.execute(
                """
                UPDATE conversations SET name=:name, avatar=:avatar, ip=:ip, tcp_port=:tcp_port
                WHERE peer_id=:peer_id
                """,
                peer,
            )

    def insert_message(self, peer: Record, message_id: str, text: str, direction: str) -> Record:
        peer_id = peer["peer_id"]
        # Mã tin nhắn cùng máy và cùng chiều được lưu một lần để xử lý gửi lại.
        state = "sending" if direction == "out" else "received"
        with self.lock, self.db:
            self._remember_peer(peer)
            self.db.execute("""
                INSERT OR IGNORE INTO messages(peer_id,message_id,text,direction,state,timestamp)
                VALUES (?,?,?,?,?,?)
                """, (peer_id, message_id, text, direction, state, now()))
            row = self.db.execute(
                "SELECT * FROM messages WHERE peer_id=? AND direction=? AND message_id=?",
                (peer_id, direction, message_id),
            ).fetchone()
            if row["text"] != text:
                raise LanError("message_conflict")
            return dict(row)

    def set_message_state(self, peer_id: str, message_id: str, state: str, error: str | None = None) -> Record:
        with self.lock, self.db:
            row = self.db.execute("""
                UPDATE messages SET state=?, error=?
                WHERE peer_id=? AND direction='out' AND message_id=?
                RETURNING *
                """, (state, error, peer_id, message_id)).fetchone()
            return dict(row)

    def messages(self, peer_id: str, before: int | None, limit: int) -> list[Record]:
        with self.lock:
            rows = self.db.execute(
                "SELECT * FROM messages WHERE peer_id=? AND seq<? ORDER BY seq DESC LIMIT ?",
                (peer_id, before if before is not None else 2**63 - 1, limit),
            ).fetchall()
            # Truy vấn lấy trang mới nhất trước, rồi đảo lại để giao diện hiển thị từ cũ đến mới.
            return [dict(row) for row in reversed(rows)]

    def mark_read(self, peer_id: str, through: int) -> None:
        # Chỉ đánh dấu đã đọc đến tin mà giao diện đã hiển thị khi tạo yêu cầu.
        with self.lock, self.db:
            self.db.execute("""
                UPDATE conversations SET read_through=MAX(read_through, COALESCE(
                    (SELECT MAX(seq) FROM messages WHERE peer_id=? AND seq<=?), 0
                )) WHERE peer_id=?
                """, (peer_id, through, peer_id))

    def conversations(self) -> list[Record]:
        with self.lock:
            return [dict(row) for row in self.db.execute("""
                SELECT c.peer_id,c.name,c.avatar,c.ip,c.tcp_port,
                    m.text AS last_text,m.timestamp AS last_message_at,
                    (SELECT COUNT(*) FROM messages u WHERE u.peer_id=c.peer_id
                    AND u.direction='in' AND u.seq>c.read_through) AS unread
                FROM conversations c
                LEFT JOIN messages m ON m.seq=(SELECT MAX(seq) FROM messages WHERE peer_id=c.peer_id)
                ORDER BY COALESCE(m.seq,0) DESC, c.name
                """)]

    def add_transfer(self, peer: Record, transfer: Record) -> None:
        with self.lock, self.db:
            self._remember_peer(peer)
            self.db.execute("""
                INSERT INTO transfers(transfer_id,peer_id,direction,filename,size,transferred,state,timestamp,error,saved_name)
                VALUES (:transfer_id,:peer_id,:direction,:filename,:size,:transferred,:state,:timestamp,:error,:saved_name)
                """, transfer)

    def update_transfer(self, transfer: Record) -> None:
        with self.lock, self.db:
            self.db.execute("""
                UPDATE transfers SET state=:state, transferred=:transferred, error=:error, saved_name=:saved_name
                WHERE transfer_id=:transfer_id
                """, transfer)

    def transfer(self, transfer_id: str) -> Record | None:
        with self.lock:
            row = self.db.execute("SELECT * FROM transfers WHERE transfer_id=?", (transfer_id,)).fetchone()
            return dict(row) if row else None

    def transfers(self) -> list[Record]:
        with self.lock:
            return [dict(row) for row in self.db.execute("""
                SELECT * FROM transfers ORDER BY
                    CASE WHEN state IN ('completed','failed') THEN 1 ELSE 0 END,
                    timestamp DESC LIMIT 200
                """)]

    def close(self) -> None:
        with self.lock:
            self.db.close()
