"""Lớp điều phối gọn giữa giao diện và các module mạng."""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from threading import Event, RLock
from typing import Any, Callable
from uuid import UUID, uuid4

from .chat_messages import ChatMessages
from .app_errors import (
    ConnectionFailedError,
    InvalidNameError,
    InvalidPeerError,
    ListenerStartError,
    SelfConnectionError,
    ServiceClosedError,
    ServiceNotStartedError,
    StorageError,
)
from .sqlite_store import SQLiteStore, Record
from .tcp_sessions import Session, TCPManager
from .wire_protocol import Header, Presence, ProtocolError

log = logging.getLogger(__name__)
EventCallback = Callable[[str, dict[str, Any]], None]


class LanService:
    """Quản lý một TCP listener và các phiên chat của ứng dụng."""

    def __init__(self, name: str, *, tcp_port: int = 5000,
                 peer_id: UUID | str | None = None, avatar: str = "teal",
                 status: str = "online", host: str = "0.0.0.0",
                 database_path: str | Path | None = None,
                 on_event: EventCallback | None = None):
        # ID này nên được dùng chung với UDP discovery nếu discovery được ghép sau.
        self.peer_id = UUID(str(peer_id)) if peer_id else uuid4()
        self.name = name.strip()
        if not self.name:
            raise InvalidNameError("Tên người dùng không được để trống")
        self.avatar = avatar
        self.status = status
        self.host = host
        self._on_event = on_event
        self._lock = RLock()
        self._listeners: list[EventCallback] = []
        self._stop = Event()
        self._started = False

        # Mặc định lưu database trong thư mục riêng của người dùng.
        if database_path is None:
            database_path = Path.home() / ".lan_chat" / "chat.sqlite3"
        try:
            self.store = SQLiteStore(database_path)
        except (sqlite3.Error, OSError) as exc:
            raise StorageError("Không mở được cơ sở dữ liệu chat") from exc

        # TCPManager gọi ChatMessages.receive khi nhận tin hoặc ACK.
        self.sessions = TCPManager(
            self._presence,
            host=host,
            port=tcp_port,
            on_chat=self._on_chat,
            on_close=self._on_close,
        )
        self.chat = ChatMessages(self)

    @property
    def tcp_port(self) -> int:
        """Cổng TCP thực tế; có thể khác giá trị yêu cầu nếu dùng port=0."""
        return self.sessions.port

    def _presence(self) -> Presence:
        """Tạo thông tin định danh dùng trong bắt tay TCP."""
        return Presence(peer_id=self.peer_id, name=self.name,
                        tcp_port=self.sessions.port, status=self.status,
                        avatar=self.avatar)

    def start(self) -> int:
        """Mở TCP listener; gọi một lần khi khởi động ứng dụng."""
        if self._stop.is_set():
            raise ServiceClosedError()
        try:
            port = self.sessions.start()
        except OSError as exc:
            raise ListenerStartError() from exc
        self._started = True
        return port

    def add_listener(self, callback: EventCallback) -> None:
        """Đăng ký callback nhận sự kiện từ service."""
        with self._lock:
            self._listeners.append(callback)

    def _emit(self, event: str, **data: Any) -> None:
        """Phát sự kiện; callback có thể chạy trên luồng mạng nền."""
        callbacks = []
        if self._on_event:
            callbacks.append(self._on_event)
        with self._lock:
            callbacks.extend(self._listeners)
        for callback in callbacks:
            try:
                callback(event, data)
            except Exception:
                log.exception("Callback xử lý sự kiện %s gặp lỗi", event)

    def _peer_info(self, peer: Any) -> dict[str, Any]:
        """Đọc thông tin peer từ dict hoặc object discovery."""
        def value(key: str, default: Any = None) -> Any:
            if isinstance(peer, dict):
                return peer.get(key, default)
            return getattr(peer, key, default)

        peer_id = str(value("peer_id", ""))
        host = value("ip", value("host"))
        port = value("tcp_port")
        if not peer_id or not host or port is None:
            raise InvalidPeerError("Peer cần có peer_id, ip/host và tcp_port")
        try:
            UUID(peer_id)
            port = int(port)
        except (ValueError, TypeError) as exc:
            raise InvalidPeerError("peer_id hoặc TCP port không hợp lệ") from exc
        if not 1 <= port <= 65535:
            raise InvalidPeerError("TCP port phải nằm trong khoảng 1 đến 65535")
        return {
            "peer_id": peer_id,
            "name": value("name", peer_id),
            "ip": str(host),
            "tcp_port": port,
        }

    def connect(self, peer: Any) -> Session:
        """Kết nối tới peer; nhận dict hoặc Peer từ UDP discovery."""
        if not self._started:
            raise ServiceNotStartedError()
        info = self._peer_info(peer)
        if info["peer_id"] == str(self.peer_id):
            raise SelfConnectionError()
        try:
            session = self.sessions.connect(info["ip"], info["tcp_port"], info["peer_id"])
        except (OSError, ConnectionError, ProtocolError) as exc:
            raise ConnectionFailedError() from exc
        self._emit("peer_connected", peer=info)
        return session

    def send_message(self, peer_id: str, text: str) -> dict[str, Any]:
        """Gửi tin nhắn tới một peer đã kết nối."""
        return self.chat.send(str(peer_id), text)

    def history(self, peer_id: str) -> list[Record]:
        """Lấy lịch sử chat đã lưu trong SQLite."""
        return self.chat.history(str(peer_id))

    def leave(self, peer_id: str) -> None:
        """Đóng phiên chat với một peer, không đóng toàn bộ ứng dụng."""
        self.sessions.leave(str(peer_id))

    def _on_chat(self, session: Session, header: Header, body: bytes) -> None:
        self.chat.receive(session, header, body)

    def _on_close(self, session: Session) -> None:
        """Thông báo giao diện khi peer ngắt kết nối."""
        self._emit("peer_disconnected", peer_id=session.peer_id,
                   peer_name=session.remote.name)

    def close(self) -> None:
        """Dừng service và đóng các socket TCP đang mở."""
        if self._stop.is_set():
            return
        self._stop.set()
        try:
            self.sessions.close()
        finally:
            self.chat.close()
            self.store.close()
            self._emit("service_closed")

    def __enter__(self) -> LanService:
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()
